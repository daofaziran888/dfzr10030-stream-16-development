"""背压积压段续送测试：容量、优先级、断线重连、回执核验与逐段回查。"""
from app.services.backlog_segment import BacklogSegmentService
from app.store import store


def fresh_service() -> BacklogSegmentService:
    return BacklogSegmentService()


class TestCapacity:
    def test_default_limit_from_config(self):
        svc = fresh_service()
        assert svc.queue_limit() == 200
        assert svc.capacity_snapshot()["剩余容量"] == 200

    def test_bind_to_backpressure_policy(self):
        store.find("backpressure", 2)["队列上限"] = 300
        svc = fresh_service()
        snapshot, message = svc.bind_capacity(policy_id=2)
        assert snapshot is not None
        assert snapshot["队列上限"] == 300
        assert snapshot["绑定策略"] == 2
        assert "300" in message

    def test_unparseable_policy_limit_is_rejected(self):
        svc = fresh_service()
        snapshot, message = svc.bind_capacity(policy_id=1)  # 队列上限仍是示例文本
        assert snapshot is None
        assert "不是可解析的正整数" in message

    def test_numeric_override(self):
        svc = fresh_service()
        snapshot, _ = svc.bind_capacity(queue_limit="150")
        assert snapshot["队列上限"] == 150
        assert snapshot["剩余容量"] == 150


class TestBacklogVsNewStream:
    def test_old_backlog_sort_before_new_stream(self):
        svc = fresh_service()
        plan = svc.preview()
        assert plan["下一源表"] == "orders"
        assert plan["预计调度"][0]["段编号"] == "SEG-0001"
        assert plan["剩余容量"] == 200
        assert plan["本轮后剩余"] == 50

    def test_round_dispatches_old_segment_only(self):
        svc = fresh_service()
        row = svc.schedule_round()
        dispatched = [item["段编号"] for item in row["调度段"]]
        paused = {item["段编号"]: item for item in row["暂停段"]}
        assert dispatched == ["SEG-0001"]
        assert paused["SEG-0003"]["原因类型"] == "续送与新流冲突"
        assert paused["SEG-0002"]["原因类型"] == "容量不足"
        assert row["本轮后占用"] == 150
        assert row["剩余容量"] == 50

    def test_new_stream_wins_when_backlog_absent(self):
        svc = fresh_service()
        store.rows("backlog_segment").remove(store.find("backlog_segment", 1))
        plan = svc.preview()
        assert plan["下一段"]["段编号"] == "SEG-0002"
        assert plan["下一段"]["来源类型"] == "积压续送"

    def test_pause_records_keep_positions(self):
        svc = fresh_service()
        row = svc.schedule_round()
        for item in row["暂停段"]:
            assert item["待补范围"]
            assert item["数据位点"]
            assert item["续送起点"] is not None
            assert item["暂停原因"]

    def test_round_is_queryable(self):
        svc = fresh_service()
        code = svc.schedule_round()["轮次编号"]
        assert any(item["轮次编号"] == code for item in svc.list_rounds())
        assert all(item["轮次编号"] == code for item in svc.list_pauses(round_code=code))


class TestReceipt:
    def test_receipt_settles_and_releases_capacity(self):
        svc = fresh_service()
        svc.schedule_round()
        assert svc.capacity_snapshot()["已占容量"] == 150
        result = svc.acknowledge(segment_code="SEG-0001", start_pos=1000, end_pos=1150)
        assert result["ok"] is True
        assert result["capacity_released"] == 150
        assert svc.capacity_snapshot()["已占容量"] == 0
        assert store.find("backlog_segment", 1)["状态"] == "已入库"

    def test_duplicate_receipt_is_idempotent(self):
        svc = fresh_service()
        svc.schedule_round()
        svc.acknowledge(segment_code="SEG-0001", start_pos=1000, end_pos=1150)
        again = svc.acknowledge(segment_code="SEG-0001", start_pos=1000, end_pos=1150)
        assert again["ok"] is True
        assert again["result"] == "重复回执"
        assert again["capacity_released"] == 0

    def test_duplicate_receipt_with_wrong_start_is_rejected(self):
        svc = fresh_service()
        svc.schedule_round()
        svc.acknowledge(segment_code="SEG-0001", start_pos=1000, end_pos=1150)
        again = svc.acknowledge(segment_code="SEG-0001", start_pos=999, end_pos=1150)
        assert again["ok"] is False
        assert "拒绝重复结算" in again["message"]

    def test_wrong_end_does_not_settle(self):
        svc = fresh_service()
        svc.schedule_round()
        result = svc.acknowledge(segment_code="SEG-0001", start_pos=1000, end_pos=1160)
        assert result["ok"] is False
        segment = store.find("backlog_segment", 1)
        assert segment["已结算"] is False
        assert segment["入库回执"] == "回执不符"
        assert svc.capacity_snapshot()["已占容量"] == 150

    def test_one_sided_receipt_is_rejected(self):
        svc = fresh_service()
        svc.schedule_round()
        result = svc.acknowledge(segment_code="SEG-0001", start_pos=1000)
        assert result["ok"] is False
        assert "同时给出起点与末点" in result["message"]

    def test_receipt_accepts_string_positions(self):
        svc = fresh_service()
        svc.schedule_round()
        result = svc.acknowledge(segment_code="SEG-0001", start_pos="1000", end_pos="1150")
        assert result["ok"] is True

    def test_late_receipt_after_disconnect_still_settles(self):
        svc = fresh_service()
        svc.schedule_round()
        svc.disconnect("orders")
        assert store.find("backlog_segment", 1)["状态"] == "待续送"
        result = svc.acknowledge(segment_code="SEG-0001", start_pos=1000, end_pos=1150)
        assert result["ok"] is True
        assert result["capacity_released"] == 150
        assert svc.capacity_snapshot()["已占容量"] == 0
        cursors = {cursor["源表"]: cursor for cursor in svc.list_cursors()}
        assert cursors["orders"]["已确认位点"] == 1150

    def test_receipt_does_not_release_capacity_of_other_segments(self):
        svc = fresh_service()
        svc.bind_capacity(queue_limit=300)
        svc.schedule_round()
        assert svc.capacity_snapshot()["已占容量"] == 300
        svc.acknowledge(segment_code="SEG-0001", start_pos=1000, end_pos=1150)
        assert svc.capacity_snapshot()["已占容量"] == 150

    def test_receipt_for_unknown_segment(self):
        svc = fresh_service()
        result = svc.acknowledge(segment_code="SEG-9999", start_pos=1, end_pos=2)
        assert result["ok"] is False
        assert "不存在" in result["message"]


class TestCursorAndReconnect:
    def test_cursor_keeps_uncommitted_range(self):
        svc = fresh_service()
        svc.schedule_round()
        cursors = {cursor["源表"]: cursor for cursor in svc.list_cursors()}
        assert cursors["orders"]["续送起点"] == 1000
        assert cursors["orders"]["待补范围"] == [[1000, 1150], [1150, 1300]]

    def test_after_receipt_cursor_advances(self):
        svc = fresh_service()
        svc.schedule_round()
        svc.acknowledge(segment_code="SEG-0001", start_pos=1000, end_pos=1150)
        cursors = {cursor["源表"]: cursor for cursor in svc.list_cursors()}
        assert cursors["orders"]["已确认位点"] == 1150
        assert cursors["orders"]["续送起点"] == 1150
        assert cursors["orders"]["待补范围"] == [[1150, 1300]]

    def test_disconnect_keeps_capacity_and_start(self):
        svc = fresh_service()
        svc.schedule_round()
        svc.disconnect("orders")
        assert svc.capacity_snapshot()["已占容量"] == 150
        plan = svc.preview()
        reasons = {item["源表"]: item["原因类型"] for item in plan["预计暂停"]}
        assert reasons["orders"] == "连接断开"
        cursors = {cursor["源表"]: cursor for cursor in svc.list_cursors()}
        assert cursors["orders"]["连接状态"] == "已断开"
        assert cursors["orders"]["续送起点"] == 1000

    def test_disconnect_rolls_sending_segment_back(self):
        svc = fresh_service()
        svc.schedule_round()
        assert store.find("backlog_segment", 1)["状态"] == "续送中"
        svc.disconnect("orders")
        segment = store.find("backlog_segment", 1)
        assert segment["状态"] == "待续送"
        assert segment["续送起点"] == 1000
        assert segment["入库回执"] == "未回执"

    def test_reconnect_reports_resume_point(self):
        svc = fresh_service()
        svc.schedule_round()
        svc.disconnect("orders")
        result = svc.reconnect("orders")
        plan = result["重连计划"][0]
        assert plan["连接状态"] == "在线"
        assert plan["续送起点"] == 1000
        assert [1000, 1150] in plan["待补范围"]

    def test_reconnect_can_schedule_round(self):
        svc = fresh_service()
        svc.disconnect("orders")
        result = svc.reconnect("orders", auto_schedule=True)
        assert result["自动调度"] is not None
        assert "orders" in result["自动调度"]["说明"]


class TestCdcHandoff:
    def test_dispatch_hands_interval_to_cdc(self):
        svc = fresh_service()
        svc.schedule_round()
        intervals = svc.cdc_intervals()
        assert len(intervals) == 1
        interval = intervals[0]
        assert interval["监控表"] == "orders"
        assert interval["起始位点"] == 1000
        assert interval["结束位点"] == 1150
        assert interval["读取方式"] == "沿原位点重读"

    def test_cdc_can_consume_interval(self):
        svc = fresh_service()
        svc.schedule_round()
        handoff = svc.cdc_intervals()[0]
        row, message = svc.consume_cdc_interval(handoff["id"])
        assert row is not None
        assert row["状态"] == "已读取"
        assert "沿原位点重读" in message

    def test_resume_uses_stored_start_after_partial_send(self):
        svc = fresh_service()
        segment = store.find("backlog_segment", 1)
        segment["续送起点"] = 1080  # 已续送到一半，断线后位点保留
        svc.schedule_round()
        interval = svc.cdc_intervals()[0]
        assert interval["起始位点"] == 1080
        assert interval["结束位点"] == 1150


class TestTraceAndActions:
    def test_trace_lists_positions_receipts_pauses(self):
        svc = fresh_service()
        svc.schedule_round()
        svc.acknowledge(segment_code="SEG-0001", start_pos=1000, end_pos=1150)
        detail = svc.trace(1)
        assert detail["数据位点"]["起始位点"] == 1000
        assert detail["待补范围"] == []
        assert len(detail["回执"]) == 1
        assert detail["调度轮次"]

    def test_trace_unknown_segment(self):
        assert fresh_service().trace(999) is None

    def test_action_dispatch_and_settle(self):
        svc = fresh_service()
        result = svc.run_action(1, "调度续送")
        assert result["ok"] is True
        result = svc.run_action(1, "回执核验", start_pos=1000, end_pos=1150)
        assert result["ok"] is True
        assert store.find("backlog_segment", 1)["状态"] == "已入库"

    def test_action_settle_requires_both_ends(self):
        svc = fresh_service()
        svc.run_action(1, "调度续送")
        result = svc.run_action(1, "回执核验", start_pos=1000)
        assert result["ok"] is False
        assert "不能只确认一端" in result["message"]

    def test_action_rollback_keeps_capacity(self):
        svc = fresh_service()
        svc.run_action(1, "调度续送")
        result = svc.run_action(1, "断线")
        assert result["ok"] is True
        assert svc.capacity_snapshot()["已占容量"] == 150
        assert store.find("backlog_segment", 1)["状态"] == "待续送"

    def test_unknown_action_rejected(self):
        result = fresh_service().run_action(1, "直接删除")
        assert result["ok"] is False
        assert "不属于背压续送可执行范围" in result["message"]

    def test_preview_falls_back_to_next_queued_segment(self):
        svc = fresh_service()
        svc.bind_capacity(queue_limit=100)
        plan = svc.preview()
        # 剩余容量不足以调度，但下一段仍要能指出队列里最靠前的待续送段。
        assert plan["预计调度"] == []
        assert plan["下一段可调度"] is False
        assert plan["下一源表"] == "orders"
        assert plan["下一段"]["段编号"] == "SEG-0001"

    def test_single_dispatch_respects_capacity(self):
        svc = fresh_service()
        svc.run_action(1, "调度续送")
        blocked = svc.run_action(3, "调度续送")
        assert blocked["ok"] is False
        assert "剩余容量 50 行" in blocked["message"]


class TestSegmentRegistration:
    def test_create_valid_segment(self):
        svc = fresh_service()
        entry, missing = svc.create_segment({
            "段编号": "SEG-0100", "源表": "orders", "源端编号": "SOUR-MYSQL-01",
            "起始位点": 1300, "结束位点": 1400, "来源类型": "积压续送",
        })
        assert entry is not None and missing == []
        assert entry["数据行数"] == 100
        assert entry["状态"] == "待续送"

    def test_reject_inverted_range(self):
        svc = fresh_service()
        entry, message = svc.create_segment({
            "段编号": "SEG-0101", "源表": "orders", "起始位点": 1400, "结束位点": 1300,
        })
        assert entry is None
        assert "必须大于" in message

    def test_reject_duplicate_code(self):
        svc = fresh_service()
        entry, message = svc.create_segment({
            "段编号": "SEG-0001", "源表": "orders", "起始位点": 1400, "结束位点": 1500,
        })
        assert entry is None
        assert "已存在" in message

    def test_missing_fields_reported(self):
        svc = fresh_service()
        entry, missing = svc.create_segment({"段编号": "SEG-0102"})
        assert entry is None
        assert set(missing) == {"源表", "起始位点", "结束位点"}


class TestSummary:
    def test_summary_counts(self):
        svc = fresh_service()
        svc.schedule_round()
        summary = svc.summary()
        assert summary["队列上限"] == 200
        assert summary["已占容量"] == 150
        assert summary["待续送段"] == 2
        assert summary["续送中段"] == 1
        assert summary["下一源表"] == "orders"
        orders = next(item for item in summary["源表"] if item["源表"] == "orders")
        assert orders["待补段数"] == 2
        assert orders["未确认行数"] == 300
