"""背压积压段的续送调度：把队列上限与各源表尚未确认的位点范围绑在一起。

调度器只做三件事：

* 为每个源表保存可恢复的未提交位点段（续送起点、待补范围、源表连接状态）；
* 在队列上限之内挑选待续送段，旧积压段优先于新流接纳，并把续送区间交给变更捕获；
* 收到入库回执时核验起点与末点，只有核验通过的区间才结算并释放容量。

容量按“已送出但还没有入库回执的数据行数”计算。断线不会释放容量，只有回执
核验通过才会释放；重复回执按幂等处理，不会二次结算。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.config import settings
from app.store import store

MODULE = "backlog_segment"
SEGMENT_TABLE = MODULE
CURSOR_TABLE = "_backlog_cursors"
ROUND_TABLE = "_backlog_rounds"
PAUSE_TABLE = "_backlog_pauses"
RECEIPT_TABLE = "_backlog_receipts"
HANDOFF_TABLE = "_cdc_resume_queue"
STATE_TABLE = "_backlog_state"

SOURCE_TYPE_BACKLOG = "积压续送"
SOURCE_TYPE_NEW = "新流接纳"

STATUS_PENDING = "待续送"
STATUS_SENDING = "续送中"
STATUS_SETTLED = "已入库"

RECEIPT_NONE = "未回执"
RECEIPT_OK = "已回执"
RECEIPT_MISMATCH = "回执不符"

LIST_FIELDS = ["段编号", "源表", "源端编号", "起始位点", "结束位点", "数据行数", "续送起点", "来源类型", "入库回执"]
REQUIRED_FIELDS = ["段编号", "源表", "起始位点", "结束位点"]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_position(value: Any) -> int | str | None:
    """位点统一成整数（数字字符串也算整数）；非数字位点保留去空白后的字符串。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else str(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return text


def same_position(left: Any, right: Any) -> bool:
    return normalize_position(left) == normalize_position(right)


def _position_key(value: Any) -> tuple[int, Any]:
    normalized = normalize_position(value)
    if isinstance(normalized, int):
        return (0, normalized)
    return (1, "" if normalized is None else str(normalized))


def _rows_of(segment: dict[str, Any]) -> int:
    """数据行数优先取登记值；缺失时按位点跨度推算（位点区间按左闭右开理解）。"""
    try:
        rows = int(segment.get("数据行数"))
        if rows >= 0:
            return rows
    except (TypeError, ValueError):
        pass
    start = normalize_position(segment.get("起始位点"))
    end = normalize_position(segment.get("结束位点"))
    if isinstance(start, int) and isinstance(end, int) and end > start:
        return end - start
    return 0


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _interval_text(segment: dict[str, Any]) -> str:
    return f"{segment.get('起始位点')} ~ {segment.get('结束位点')}"


ACTION_DISPATCH = "调度续送"
ACTION_SETTLE = "回执核验"
ACTION_ROLLBACK = "断线回退"

# 页面主按钮之外允许的别名，动作语义保持一致。
ACTION_ALIASES = {
    "续送": ACTION_DISPATCH,
    "启动续送": ACTION_DISPATCH,
    "触发续送": ACTION_DISPATCH,
    "继续续送": ACTION_DISPATCH,
    "确认入库": ACTION_SETTLE,
    "登记回执": ACTION_SETTLE,
    "确认回执": ACTION_SETTLE,
    "回执结算": ACTION_SETTLE,
    "回退待续": ACTION_ROLLBACK,
    "挂起续送": ACTION_ROLLBACK,
    "断线": ACTION_ROLLBACK,
}


class BacklogSegmentService:
    # ------------------------------------------------------------------ 基础存储
    def _segments(self) -> list[dict[str, Any]]:
        return store.rows(SEGMENT_TABLE)

    def _cursors(self) -> list[dict[str, Any]]:
        return store.rows(CURSOR_TABLE)

    def _rounds(self) -> list[dict[str, Any]]:
        return store.rows(ROUND_TABLE)

    def _pauses(self) -> list[dict[str, Any]]:
        return store.rows(PAUSE_TABLE)

    def _receipts(self) -> list[dict[str, Any]]:
        return store.rows(RECEIPT_TABLE)

    def _handoffs(self) -> list[dict[str, Any]]:
        return store.rows(HANDOFF_TABLE)

    def _state(self) -> dict[str, Any]:
        rows = store.rows(STATE_TABLE)
        if not rows:
            rows.append({
                "id": 1,
                "queue_limit": int(settings.backlog_queue_limit),
                "bound_policy_id": None,
                "updated_at": _now(),
            })
        return rows[0]

    def _find_segment(self, *, segment_id: int | None = None, segment_code: str | None = None) -> dict[str, Any] | None:
        for segment in self._segments():
            if segment_id is not None and int(segment.get("id", 0)) == int(segment_id):
                return segment
            if segment_code and str(segment.get("段编号", "")) == str(segment_code).strip():
                return segment
        return None

    # ------------------------------------------------------------------ 队列上限
    def _parse_limit(self, value: Any) -> int | None:
        normalized = normalize_position(value)
        if isinstance(normalized, int) and normalized > 0:
            return normalized
        return None

    def queue_limit(self) -> int:
        """优先读取绑定的背压策略；策略不可解析时退回调度器默认上限。"""
        state = self._state()
        policy_id = state.get("bound_policy_id")
        if policy_id is not None:
            policy = store.find("backpressure", int(policy_id))
            if policy is not None:
                limit = self._parse_limit(policy.get("队列上限"))
                if limit is not None:
                    return limit
        limit = self._parse_limit(state.get("queue_limit"))
        return limit or int(settings.backlog_queue_limit)

    def bind_capacity(self, *, queue_limit: Any = None, policy_id: int | None = None) -> tuple[dict[str, Any] | None, str]:
        """把队列上限绑定到某个背压策略，或直接给定数值上限。"""
        state = self._state()
        if policy_id is not None:
            policy = store.find("backpressure", int(policy_id))
            if policy is None:
                return None, f"背压策略 {policy_id} 不存在或已归档"
            limit = self._parse_limit(policy.get("队列上限"))
            if limit is None:
                return None, (
                    f"背压策略 {policy_id} 的队列上限「{policy.get('队列上限')}」不是可解析的正整数，"
                    "无法用于续送调度"
                )
            state["bound_policy_id"] = int(policy_id)
            state["queue_limit"] = limit
        elif queue_limit is not None:
            limit = self._parse_limit(queue_limit)
            if limit is None:
                return None, f"队列上限「{queue_limit}」不是可解析的正整数"
            state["bound_policy_id"] = None
            state["queue_limit"] = limit
        else:
            return None, "请提供队列上限数值或要绑定的背压策略编号"
        state["updated_at"] = _now()
        return self.capacity_snapshot(), f"续送队列上限已设为 {state['queue_limit']} 行"

    def capacity_snapshot(self) -> dict[str, Any]:
        limit = self.queue_limit()
        occupied = sum(
            int(segment.get("占用量") or 0)
            for segment in self._segments()
            if segment.get("容量占用")
        )
        state = self._state()
        return {
            "队列上限": limit,
            "已占容量": occupied,
            "剩余容量": max(0, limit - occupied),
            "绑定策略": state.get("bound_policy_id"),
        }

    # ------------------------------------------------------------------ 源表位点
    def _ensure_cursor(self, source_table: str, source_id: str | None = None) -> dict[str, Any]:
        for cursor in self._cursors():
            if cursor.get("源表") == source_table:
                if source_id and not cursor.get("源端编号"):
                    cursor["源端编号"] = source_id
                return cursor
        cursor = {
            "id": _next_id(self._cursors()),
            "源表": source_table,
            "源端编号": source_id or "",
            "连接状态": "在线",
            "已确认位点": None,
            "续送起点": None,
            "未提交起点": None,
            "未提交末点": None,
            "待补范围": [],
            "待补段数": 0,
            "断线次数": 0,
            "最近断线时间": None,
            "最近重连时间": None,
            "更新时间": _now(),
        }
        self._cursors().append(cursor)
        return cursor

    def sync_source_cursor(self, source_table: str, source_id: str | None = None) -> dict[str, Any]:
        """按该源表所有段的回执状态重算可恢复位点，段一旦结算就只剩更晚的待补范围。"""
        cursor = self._ensure_cursor(source_table, source_id)
        segments = sorted(
            [segment for segment in self._segments() if segment.get("源表") == source_table],
            key=lambda segment: _position_key(segment.get("起始位点")),
        )
        if source_id is None:
            source_id = next((str(segment.get("源端编号") or "") for segment in segments if segment.get("源端编号")), "")
            cursor["源端编号"] = source_id or cursor.get("源端编号", "")

        unacked = [segment for segment in segments if not segment.get("已结算")]
        if unacked:
            cursor["续送起点"] = unacked[0].get("起始位点")
            cursor["未提交起点"] = min((segment.get("起始位点") for segment in unacked), key=_position_key)
            cursor["未提交末点"] = max((segment.get("结束位点") for segment in unacked), key=_position_key)
            cursor["待补范围"] = [
                [segment.get("起始位点"), segment.get("结束位点")] for segment in unacked
            ]
            cursor["待补段数"] = len(unacked)
        else:
            cursor["续送起点"] = None
            cursor["未提交起点"] = None
            cursor["未提交末点"] = None
            cursor["待补范围"] = []
            cursor["待补段数"] = 0

        # 已确认位点只增不减，且只在区间连续时向前推进，避免跨过尚未回执的段。
        # 还没有任何回执时，最早待续送段的起点就是可定位的确认边界。
        confirmed: Any = normalize_position(cursor.get("已确认位点"))
        if confirmed is None and segments:
            confirmed = normalize_position(segments[0].get("起始位点"))
        for segment in segments:
            if not segment.get("已结算"):
                break
            start = normalize_position(segment.get("起始位点"))
            end = normalize_position(segment.get("结束位点"))
            if not isinstance(start, int) or not isinstance(end, int):
                break
            if isinstance(confirmed, int) and start > confirmed:
                break
            if confirmed is None or (isinstance(confirmed, int) and end > confirmed):
                confirmed = end
        cursor["已确认位点"] = confirmed
        cursor["更新时间"] = _now()
        return cursor

    def sync_all_cursors(self) -> list[dict[str, Any]]:
        sources = {str(segment.get("源表")) for segment in self._segments() if segment.get("源表")}
        sources.update(str(cursor.get("源表")) for cursor in self._cursors() if cursor.get("源表"))
        return [self.sync_source_cursor(source) for source in sorted(sources)]

    def list_cursors(self) -> list[dict[str, Any]]:
        self.sync_all_cursors()
        return [dict(cursor) for cursor in self._cursors()]

    def pending_ranges(self) -> list[dict[str, Any]]:
        """逐段回查的汇总：每个源表还有哪些区间没有拿到入库回执。"""
        self.sync_all_cursors()
        result: list[dict[str, Any]] = []
        for cursor in self._cursors():
            segments = sorted(
                [s for s in self._segments()
                 if s.get("源表") == cursor.get("源表") and not s.get("已结算")],
                key=lambda s: _position_key(s.get("起始位点")),
            )
            result.append({
                "源表": cursor.get("源表"),
                "源端编号": cursor.get("源端编号"),
                "连接状态": cursor.get("连接状态"),
                "续送起点": cursor.get("续送起点"),
                "已确认位点": cursor.get("已确认位点"),
                "待补段数": len(segments),
                "未确认行数": sum(_rows_of(segment) for segment in segments),
                "待补范围": [
                    {
                        "段编号": segment.get("段编号"),
                        "起始位点": segment.get("起始位点"),
                        "结束位点": segment.get("结束位点"),
                        "续送起点": segment.get("续送起点"),
                        "状态": segment.get("状态"),
                        "入库回执": segment.get("入库回执"),
                        "暂停原因": segment.get("暂停原因") or "",
                    }
                    for segment in segments
                ],
            })
        return result

    # ------------------------------------------------------------------ 段登记与查询
    def list_segments(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(self._segments())
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("段编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status or row.get("状态") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_segment(self, segment_id: int) -> dict[str, Any] | None:
        return self._find_segment(segment_id=segment_id)

    def create_segment(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str] | str]:
        missing = [field for field in REQUIRED_FIELDS if normalize_position(values.get(field)) is None]
        if missing:
            return None, missing
        segment_code = str(values.get("段编号") or "").strip()
        if self._find_segment(segment_code=segment_code) is not None:
            return None, f"段编号「{segment_code}」已存在"
        start = normalize_position(values.get("起始位点"))
        end = normalize_position(values.get("结束位点"))
        if isinstance(start, int) and isinstance(end, int) and end <= start:
            return None, f"结束位点 {end} 必须大于起始位点 {start}"
        source_table = str(values.get("源表") or "").strip()
        source_type = str(values.get("来源类型") or SOURCE_TYPE_NEW).strip()
        if source_type not in (SOURCE_TYPE_BACKLOG, SOURCE_TYPE_NEW):
            return None, f"来源类型「{source_type}」只能是「{SOURCE_TYPE_BACKLOG}」或「{SOURCE_TYPE_NEW}」"
        segment = {
            "id": _next_id(self._segments()),
            "段编号": segment_code,
            "源表": source_table,
            "源端编号": str(values.get("源端编号") or "").strip(),
            "起始位点": start,
            "结束位点": end,
            "数据行数": _rows_of({"起始位点": start, "结束位点": end, "数据行数": values.get("数据行数")}),
            "来源类型": source_type,
            "状态": STATUS_PENDING,
            "status": STATUS_PENDING,
            "续送起点": start,
            "已送达位点": None,
            "入库回执": RECEIPT_NONE,
            "回执起点": None,
            "回执末点": None,
            "回执编号": None,
            "已结算": False,
            "容量占用": False,
            "占用量": 0,
            "尝试次数": 0,
            "断线次数": 0,
            "最近调度轮次": None,
            "暂停原因": "",
            "创建时间": _now(),
            "更新时间": _now(),
            "pending": True,
            "abnormal": False,
        }
        self._segments().append(segment)
        self.sync_source_cursor(source_table, segment["源端编号"] or None)
        return segment, []

    # ------------------------------------------------------------------ 调度
    def _candidate_sort_key(self, segment: dict[str, Any]) -> tuple[int, tuple[int, Any], int]:
        # 旧积压段（续送）优先于新流接纳，同类型内按位点先后。
        priority = 0 if segment.get("来源类型") == SOURCE_TYPE_BACKLOG else 1
        return (priority, _position_key(segment.get("起始位点")), int(segment.get("id", 0)))

    def _candidates(self) -> list[dict[str, Any]]:
        return sorted(
            [
                segment for segment in self._segments()
                if not segment.get("已结算") and segment.get("状态") == STATUS_PENDING
            ],
            key=self._candidate_sort_key,
        )

    def _plan(self) -> dict[str, Any]:
        """只计算不落库：给页面/接口先看这一轮会先处理 orders 还是 payments。"""
        limit = self.queue_limit()
        occupied = int(self.capacity_snapshot()["已占容量"])
        occupied_before = occupied
        remaining = max(0, limit - occupied)
        cursor_status = {
            str(cursor.get("源表")): str(cursor.get("连接状态", "在线"))
            for cursor in self._cursors()
        }
        selected: list[dict[str, Any]] = []
        deferred: list[dict[str, Any]] = []
        for segment in self._candidates():
            size = _rows_of(segment)
            held = bool(segment.get("容量占用"))
            if cursor_status.get(str(segment.get("源表")), "在线") != "在线":
                deferred.append({
                    "segment": segment,
                    "reason_type": "连接断开",
                    "reason": f"源表 {segment.get('源表')} 连接已断开，等待重连后续送，续送起点 {segment.get('续送起点')}",
                    "needs_capacity": 0,
                })
                continue
            if held or size <= remaining:
                selected.append({"segment": segment, "reuse_held": held, "size": size})
                if not held:
                    remaining -= size
                    occupied += size
                continue
            if segment.get("来源类型") == SOURCE_TYPE_NEW:
                reason = (
                    f"旧段续送优先，剩余容量 {remaining} 行不足以接纳新流段 {size} 行，"
                    f"待补范围 {_interval_text(segment)} 顺延到下一轮"
                )
                reason_type = "续送与新流冲突"
            else:
                reason = (
                    f"剩余容量 {remaining} 行不足以承载积压段 {size} 行，"
                    f"待补范围 {_interval_text(segment)} 顺延到下一轮"
                )
                reason_type = "容量不足"
            deferred.append({
                "segment": segment,
                "reason_type": reason_type,
                "reason": reason,
                "needs_capacity": size,
            })
        next_segment = selected[0]["segment"] if selected else None
        if next_segment is None and deferred:
            # 没有可立即调度的段时，下一段就是队列里最靠前的待续送段。
            next_segment = deferred[0]["segment"]
        return {
            "queue_limit": limit,
            "occupied": occupied,
            "occupied_before": occupied_before,
            "remaining": remaining,
            "selected": selected,
            "deferred": deferred,
            "next": next_segment,
        }

    def preview(self) -> dict[str, Any]:
        plan = self._plan()
        next_segment = plan["next"]
        return {
            "队列上限": plan["queue_limit"],
            "已占容量": plan["occupied_before"],
            "本轮后已占": plan["occupied"],
            "剩余容量": max(0, plan["queue_limit"] - plan["occupied_before"]),
            "本轮后剩余": plan["remaining"],
            "下一段": dict(next_segment) if next_segment else None,
            "下一源表": next_segment.get("源表") if next_segment else None,
            "下一段可调度": bool(plan["selected"]),
            "预计调度": [
                {
                    "段编号": item["segment"].get("段编号"),
                    "源表": item["segment"].get("源表"),
                    "起始位点": item["segment"].get("起始位点"),
                    "结束位点": item["segment"].get("结束位点"),
                    "数据行数": item["size"],
                    "容量方式": "复用已占容量" if item["reuse_held"] else "新占容量",
                }
                for item in plan["selected"]
            ],
            "预计暂停": [
                {
                    "段编号": item["segment"].get("段编号"),
                    "源表": item["segment"].get("源表"),
                    "原因类型": item["reason_type"],
                    "原因": item["reason"],
                    "待补范围": [item["segment"].get("起始位点"), item["segment"].get("结束位点")],
                }
                for item in plan["deferred"]
            ],
        }

    def schedule_round(self, *, round_label: str | None = None) -> dict[str, Any]:
        plan = self._plan()
        rounds = self._rounds()
        round_id = _next_id(rounds)
        round_code = round_label or f"BKLG-{round_id:04d}"
        dispatched: list[dict[str, Any]] = []
        paused: list[dict[str, Any]] = []
        limit = plan["queue_limit"]
        occupied_before = int(self.capacity_snapshot()["已占容量"])

        for item in plan["selected"]:
            segment = item["segment"]
            segment["状态"] = STATUS_SENDING
            segment["status"] = STATUS_SENDING
            segment["尝试次数"] = int(segment.get("尝试次数") or 0) + 1
            segment["最近调度轮次"] = round_code
            segment["已送达位点"] = segment.get("续送起点") or segment.get("起始位点")
            segment["暂停原因"] = ""
            segment["pending"] = True
            segment["abnormal"] = False
            segment["更新时间"] = _now()
            if not segment.get("容量占用"):
                segment["容量占用"] = True
                segment["占用量"] = item["size"]
            handoff = self._handoff_to_cdc(segment, round_code)
            dispatched.append({
                "段编号": segment.get("段编号"),
                "源表": segment.get("源表"),
                "起始位点": segment.get("起始位点"),
                "结束位点": segment.get("结束位点"),
                "续送起点": segment.get("续送起点"),
                "数据行数": item["size"],
                "容量方式": "复用已占容量" if item["reuse_held"] else "新占容量",
                "容量占用": int(segment.get("占用量") or 0),
                "变更捕获读取区间": {
                    "源表": segment.get("源表"),
                    "起始位点": segment.get("续送起点") or segment.get("起始位点"),
                    "结束位点": segment.get("结束位点"),
                    "读取方式": "沿原位点重读",
                },
                "下发任务": handoff.get("任务编号"),
            })

        for item in plan["deferred"]:
            segment = item["segment"]
            segment["暂停原因"] = item["reason"]
            segment["pending"] = True
            segment["更新时间"] = _now()
            pause = self._record_pause(round_code, segment, item["reason_type"], item["reason"])
            paused.append(pause)

        occupied_after = int(self.capacity_snapshot()["已占容量"])
        summary = self._round_summary(dispatched, paused, limit)
        round_row = {
            "id": round_id,
            "轮次编号": round_code,
            "队列上限": limit,
            "本轮前占用": occupied_before,
            "本轮后占用": occupied_after,
            "剩余容量": max(0, limit - occupied_after),
            "调度段": dispatched,
            "暂停段": paused,
            "状态": "已调度" if dispatched else "无可调度段",
            "说明": summary,
            "创建时间": _now(),
        }
        rounds.append(round_row)
        self.sync_all_cursors()
        return dict(round_row)

    def _round_summary(self, dispatched: list[dict[str, Any]], paused: list[dict[str, Any]], limit: int) -> str:
        if dispatched:
            parts = [
                f"本轮处理 {item['源表']} 的 {item['段编号']}（{item['起始位点']} ~ {item['结束位点']}）"
                for item in dispatched
            ]
            text = "；".join(parts) + f"；队列上限 {limit} 行。"
        else:
            text = f"本轮没有可用容量或可调度段，队列上限 {limit} 行。"
        if paused:
            text += " 未进入本轮的段：" + "；".join(
                f"{item['段编号']}（{item['原因类型']}：{item['暂停原因']}）" for item in paused
            )
        return text

    def _record_pause(self, round_code: str, segment: dict[str, Any], reason_type: str, reason: str) -> dict[str, Any]:
        pause = {
            "id": _next_id(self._pauses()),
            "轮次编号": round_code,
            "段编号": segment.get("段编号"),
            "源表": segment.get("源表"),
            "原因类型": reason_type,
            "暂停原因": reason,
            "数据位点": _interval_text(segment),
            "待补范围": [segment.get("起始位点"), segment.get("结束位点")],
            "续送起点": segment.get("续送起点") or segment.get("起始位点"),
            "状态": "待下轮续送",
            "创建时间": _now(),
        }
        self._pauses().append(pause)
        return pause

    def _handoff_to_cdc(self, segment: dict[str, Any], round_code: str) -> dict[str, Any]:
        for handoff in self._handoffs():
            if handoff.get("段编号") == segment.get("段编号") and handoff.get("状态") == "待读取":
                handoff["状态"] = "已被重发"
                handoff["更新时间"] = _now()
        handoff = {
            "id": _next_id(self._handoffs()),
            "任务编号": f"CDC-RESUME-{len(self._handoffs()) + 1:04d}",
            "源数据库": segment.get("源端编号") or "default",
            "监控表": segment.get("源表"),
            "段编号": segment.get("段编号"),
            "起始位点": segment.get("续送起点") or segment.get("起始位点"),
            "结束位点": segment.get("结束位点"),
            "读取方式": "沿原位点重读",
            "状态": "待读取",
            "下发轮次": round_code,
            "重试次数": int(segment.get("尝试次数") or 0),
            "创建时间": _now(),
            "更新时间": _now(),
        }
        self._handoffs().append(handoff)
        return handoff

    # ------------------------------------------------------------------ 入库回执
    def acknowledge(
        self,
        *,
        segment_id: int | None = None,
        segment_code: str | None = None,
        start_pos: Any = None,
        end_pos: Any = None,
        receipt_id: str | None = None,
    ) -> dict[str, Any]:
        """核验回执起点/末点后结算；重复回执保持幂等，容量只释放一次。"""
        segment = self._find_segment(segment_id=segment_id, segment_code=segment_code)
        if segment is None:
            return self._finish_receipt(
                segment=None, start_pos=start_pos, end_pos=end_pos, receipt_id=receipt_id,
                ok=False, result="段不存在",
                message=f"积压段 {segment_code or segment_id} 不存在，回执未处理",
                released=0,
            )
        if start_pos is None or end_pos is None:
            return self._finish_receipt(
                segment=segment, start_pos=start_pos, end_pos=end_pos, receipt_id=receipt_id,
                ok=False, result="回执不完整",
                message="入库回执必须同时给出起点与末点，不能只确认一端",
                released=0,
            )
        start = normalize_position(start_pos)
        end = normalize_position(end_pos)
        if isinstance(start, int) and isinstance(end, int) and end <= start:
            return self._finish_receipt(
                segment=segment, start_pos=start_pos, end_pos=end_pos, receipt_id=receipt_id,
                ok=False, result="回执不完整",
                message=f"回执末点 {end} 必须大于起点 {start}",
                released=0,
            )

        if segment.get("已结算"):
            recorded_start = segment.get("回执起点", segment.get("起始位点"))
            recorded_end = segment.get("回执末点", segment.get("结束位点"))
            if same_position(start, recorded_start) and same_position(end, recorded_end):
                return self._finish_receipt(
                    segment=segment, start_pos=start_pos, end_pos=end_pos, receipt_id=receipt_id,
                    ok=True, result="重复回执",
                    message=(
                        f"区间 {_interval_text(segment)} 已入库，起点与末点核验一致，"
                        "按幂等处理，未重复释放容量"
                    ),
                    released=0,
                )
            return self._finish_receipt(
                segment=segment, start_pos=start_pos, end_pos=end_pos, receipt_id=receipt_id,
                ok=False, result="重复回执核验失败",
                message=(
                    f"区间 {_interval_text(segment)} 已入库，但回执起点/末点 "
                    f"{start} ~ {end} 与已入账位点不一致，拒绝重复结算"
                ),
                released=0,
            )

        if not (same_position(start, segment.get("起始位点")) and same_position(end, segment.get("结束位点"))):
            segment["入库回执"] = RECEIPT_MISMATCH
            segment["abnormal"] = True
            segment["更新时间"] = _now()
            return self._finish_receipt(
                segment=segment, start_pos=start_pos, end_pos=end_pos, receipt_id=receipt_id,
                ok=False, result="起点末点不符",
                message=(
                    f"回执起点/末点 {start} ~ {end} 与续送区间 "
                    f"{_interval_text(segment)} 不一致，区间未结算，容量不释放"
                ),
                released=0,
            )

        released = int(segment.get("占用量") or 0) if segment.get("容量占用") else 0
        segment["状态"] = STATUS_SETTLED
        segment["status"] = STATUS_SETTLED
        segment["已结算"] = True
        segment["入库回执"] = RECEIPT_OK
        segment["回执起点"] = start
        segment["回执末点"] = end
        segment["回执编号"] = receipt_id or f"RCPT-{segment.get('段编号')}"
        segment["已送达位点"] = segment.get("结束位点")
        segment["容量占用"] = False
        segment["占用量"] = 0
        segment["暂停原因"] = ""
        segment["pending"] = False
        segment["abnormal"] = False
        segment["更新时间"] = _now()
        for handoff in self._handoffs():
            if handoff.get("段编号") == segment.get("段编号") and handoff.get("状态") in ("待读取", "已被重发"):
                handoff["状态"] = "已回执"
                handoff["更新时间"] = _now()
        self.sync_source_cursor(str(segment.get("源表")), str(segment.get("源端编号") or "") or None)
        capacity_note = f"释放容量 {released} 行" if released else "该段此前未占用容量，无容量释放"
        return self._finish_receipt(
            segment=segment, start_pos=start_pos, end_pos=end_pos, receipt_id=receipt_id,
            ok=True, result="已结算",
            message=(
                f"回执起点 {start}、末点 {end} 核验通过，区间已入库；{capacity_note}"
            ),
            released=released,
        )

    def _finish_receipt(
        self,
        *,
        segment: dict[str, Any] | None,
        start_pos: Any,
        end_pos: Any,
        receipt_id: str | None,
        ok: bool,
        result: str,
        message: str,
        released: int,
    ) -> dict[str, Any]:
        receipt_no = receipt_id or f"RCPT-{_next_id(self._receipts()):04d}"
        receipt = {
            "id": _next_id(self._receipts()),
            "回执编号": receipt_no,
            "段编号": segment.get("段编号") if segment else None,
            "源表": segment.get("源表") if segment else None,
            "回执起点": normalize_position(start_pos),
            "回执末点": normalize_position(end_pos),
            "结果": result,
            "是否结算": ok and result == "已结算",
            "释放容量": released,
            "说明": message,
            "创建时间": _now(),
        }
        self._receipts().append(receipt)
        return {
            "ok": ok,
            "result": result,
            "message": message,
            "entry": dict(segment) if segment else None,
            "receipt": receipt,
            "capacity_released": released,
        }

    # ------------------------------------------------------------------ 断线 / 重连
    def _sources_of_interest(self, source_table: str | None) -> list[str]:
        if source_table:
            return [source_table]
        sources = {str(segment.get("源表")) for segment in self._segments() if segment.get("源表")}
        sources.update(str(cursor.get("源表")) for cursor in self._cursors() if cursor.get("源表"))
        return sorted(sources)

    def disconnect(self, source_table: str | None = None, *, reason: str = "连接中断，等待重连后续送") -> dict[str, Any]:
        """断线只改变可续送状态，不释放容量；容量仍由未回执区间占用。"""
        changed: list[dict[str, Any]] = []
        for source in self._sources_of_interest(source_table):
            cursor = self._ensure_cursor(source)
            cursor["连接状态"] = "已断开"
            cursor["断线次数"] = int(cursor.get("断线次数") or 0) + 1
            cursor["最近断线时间"] = _now()
            cursor["更新时间"] = _now()
            for segment in self._segments():
                if segment.get("源表") != source or segment.get("已结算"):
                    continue
                if segment.get("状态") == STATUS_SENDING:
                    segment["状态"] = STATUS_PENDING
                    segment["status"] = STATUS_PENDING
                    segment["断线次数"] = int(segment.get("断线次数") or 0) + 1
                    segment["入库回执"] = RECEIPT_NONE
                    segment["暂停原因"] = reason
                    segment["pending"] = True
                    segment["更新时间"] = _now()
                    pause = self._record_pause(
                        "断线", segment, "连接中断",
                        f"{reason}，回执未到达，容量保持占用；重连后从续送起点 "
                        f"{segment.get('续送起点')} 沿原位置重读",
                    )
                else:
                    segment["暂停原因"] = (
                        f"源表连接已断开，本段未送出；重连后从续送起点 "
                        f"{segment.get('续送起点')} 继续续送"
                    )
                    segment["更新时间"] = _now()
                    pause = self._record_pause(
                        "断线", segment, "连接断开", segment["暂停原因"],
                    )
                changed.append(pause)
        self.sync_all_cursors()
        return {
            "ok": True,
            "message": f"已标记 {len(self._sources_of_interest(source_table))} 个源表断线，未回执区间容量保持占用",
            "源表": self._sources_of_interest(source_table),
            "回退段": changed,
        }

    def reconnect(self, source_table: str | None = None, *, auto_schedule: bool = False) -> dict[str, Any]:
        sources = self._sources_of_interest(source_table)
        plans: list[dict[str, Any]] = []
        for source in sources:
            cursor = self._ensure_cursor(source)
            cursor["连接状态"] = "在线"
            cursor["最近重连时间"] = _now()
            cursor["更新时间"] = _now()
            self.sync_source_cursor(source)
            unacked = sorted(
                [segment for segment in self._segments()
                 if segment.get("源表") == source and not segment.get("已结算")],
                key=lambda segment: _position_key(segment.get("起始位点")),
            )
            plans.append({
                "源表": source,
                "连接状态": "在线",
                "续送起点": cursor.get("续送起点"),
                "待补范围": [[segment.get("起始位点"), segment.get("结束位点")] for segment in unacked],
                "段": [
                    {
                        "段编号": segment.get("段编号"),
                        "续送起点": segment.get("续送起点"),
                        "起始位点": segment.get("起始位点"),
                        "结束位点": segment.get("结束位点"),
                        "容量占用": bool(segment.get("容量占用")),
                    }
                    for segment in unacked
                ],
            })
        result = {
            "ok": True,
            "message": "源表已重连，可按续送起点继续续送",
            "重连计划": plans,
            "自动调度": None,
        }
        if auto_schedule:
            result["自动调度"] = self.schedule_round(round_label=None)
        return result

    # ------------------------------------------------------------------ 段动作
    def run_action(
        self,
        entry_id: int,
        action: str,
        *,
        start_pos: Any = None,
        end_pos: Any = None,
        receipt_id: str | None = None,
    ) -> dict[str, Any]:
        """段级动作入口：调度续送、回执核验、断线回退。"""
        segment = self._find_segment(segment_id=entry_id)
        if segment is None:
            return {"ok": False, "message": f"积压段 {entry_id} 不存在", "entry": None, "receipt": None}
        normalized = ACTION_ALIASES.get(action.strip(), action.strip())
        if normalized == ACTION_DISPATCH:
            return self._action_dispatch(segment)
        if normalized == ACTION_SETTLE:
            if start_pos is None or end_pos is None:
                return {
                    "ok": False,
                    "entry": dict(segment),
                    "receipt": None,
                    "message": (
                        f"回执核验需要同时提供起点与末点，当前续送区间为 {_interval_text(segment)}，"
                        "不能只确认一端"
                    ),
                }
            return self.acknowledge(
                segment_id=entry_id, start_pos=start_pos, end_pos=end_pos, receipt_id=receipt_id)
        if normalized == ACTION_ROLLBACK:
            return self._action_rollback(segment)
        return {
            "ok": False,
            "entry": dict(segment),
            "receipt": None,
            "message": f"动作「{action}」不属于背压续送可执行范围",
        }

    def _action_dispatch(self, segment: dict[str, Any]) -> dict[str, Any]:
        """单段调度走同一条容量与优先级规则：旧积压优先，超出剩余容量就写暂停记录。"""
        if segment.get("已结算"):
            return {
                "ok": False, "entry": dict(segment), "receipt": None,
                "message": f"段 {segment.get('段编号')} 已入库，无需再次续送",
            }
        cursor = self._ensure_cursor(str(segment.get("源表")), str(segment.get("源端编号") or "") or None)
        if cursor.get("连接状态") != "在线":
            reason = (
                f"源表 {segment.get('源表')} 连接已断开，等待重连后续送，"
                f"续送起点 {segment.get('续送起点') or segment.get('起始位点')}"
            )
            self._record_pause("单段调度", segment, "连接断开", reason)
            segment["暂停原因"] = reason
            return {"ok": False, "entry": dict(segment), "receipt": None, "message": reason}
        size = _rows_of(segment)
        capacity = self.capacity_snapshot()
        remaining = int(capacity["剩余容量"])
        if not segment.get("容量占用") and size > remaining:
            reason = (
                f"剩余容量 {remaining} 行不足以承载段 {segment.get('段编号')} 的 {size} 行，"
                f"待补范围 {_interval_text(segment)} 顺延到下一轮"
            )
            self._record_pause("单段调度", segment, "容量不足", reason)
            segment["暂停原因"] = reason
            return {"ok": False, "entry": dict(segment), "receipt": None, "message": reason}
        segment["状态"] = STATUS_SENDING
        segment["status"] = STATUS_SENDING
        segment["尝试次数"] = int(segment.get("尝试次数") or 0) + 1
        segment["已送达位点"] = segment.get("续送起点") or segment.get("起始位点")
        segment["暂停原因"] = ""
        segment["pending"] = True
        segment["更新时间"] = _now()
        if not segment.get("容量占用"):
            segment["容量占用"] = True
            segment["占用量"] = size
        handoff = self._handoff_to_cdc(segment, "单段调度")
        self.sync_source_cursor(str(segment.get("源表")), str(segment.get("源端编号") or "") or None)
        return {
            "ok": True,
            "entry": dict(segment),
            "receipt": None,
            "message": (
                f"段 {segment.get('段编号')} 已交给变更捕获，沿原位点 "
                f"{handoff.get('起始位点')} ~ {handoff.get('结束位点')} 重读"
            ),
        }

    def _action_rollback(self, segment: dict[str, Any]) -> dict[str, Any]:
        if segment.get("已结算"):
            return {
                "ok": False, "entry": dict(segment), "receipt": None,
                "message": f"段 {segment.get('段编号')} 已入库，不能再回退为待续送",
            }
        segment["状态"] = STATUS_PENDING
        segment["status"] = STATUS_PENDING
        segment["入库回执"] = RECEIPT_NONE
        segment["断线次数"] = int(segment.get("断线次数") or 0) + 1
        reason = (
            f"续送被中断，回执未到达，容量保持占用；续送起点 "
            f"{segment.get('续送起点') or segment.get('起始位点')} 已保留"
        )
        segment["暂停原因"] = reason
        segment["pending"] = True
        segment["更新时间"] = _now()
        self._record_pause("单段调度", segment, "连接中断", reason)
        self.sync_source_cursor(str(segment.get("源表")), str(segment.get("源端编号") or "") or None)
        return {"ok": True, "entry": dict(segment), "receipt": None, "message": reason}

    # ------------------------------------------------------------------ 变更捕获 / 回查
    def cdc_intervals(self, *, status: str | None = None) -> list[dict[str, Any]]:
        rows = [dict(row) for row in self._handoffs()]
        if status:
            rows = [row for row in rows if row.get("状态") == status]
        return sorted(rows, key=lambda row: int(row.get("id", 0)), reverse=True)

    def consume_cdc_interval(self, handoff_id: int) -> tuple[dict[str, Any] | None, str]:
        for row in self._handoffs():
            if int(row.get("id", 0)) == int(handoff_id):
                if row.get("状态") == "已回执":
                    return row, "该续送区间已入库，无需重复读取"
                row["状态"] = "已读取"
                row["更新时间"] = _now()
                return row, f"变更捕获已领取 {row.get('监控表')} 的续送区间，沿原位点重读"
        return None, f"续送区间 {handoff_id} 不存在"

    def summary(self) -> dict[str, Any]:
        self.sync_all_cursors()
        capacity = self.capacity_snapshot()
        segments = self._segments()
        preview = self.preview()
        return {
            "业务模块": "背压积压续送",
            "队列上限": capacity["队列上限"],
            "已占容量": capacity["已占容量"],
            "剩余容量": capacity["剩余容量"],
            "绑定策略": capacity["绑定策略"],
            "待续送段": sum(1 for segment in segments if segment.get("状态") == STATUS_PENDING),
            "续送中段": sum(1 for segment in segments if segment.get("状态") == STATUS_SENDING),
            "已入库段": sum(1 for segment in segments if segment.get("状态") == STATUS_SETTLED),
            "暂停记录": len(self._pauses()),
            "下一段": preview["下一段"],
            "下一源表": preview["下一源表"],
            "源表": self.pending_ranges(),
        }

    def list_rounds(self) -> list[dict[str, Any]]:
        return sorted((dict(row) for row in self._rounds()), key=lambda row: int(row.get("id", 0)), reverse=True)

    def list_pauses(self, *, round_code: str | None = None, segment_code: str | None = None) -> list[dict[str, Any]]:
        rows = [dict(row) for row in self._pauses()]
        if round_code:
            rows = [row for row in rows if row.get("轮次编号") == round_code]
        if segment_code:
            rows = [row for row in rows if row.get("段编号") == segment_code]
        return sorted(rows, key=lambda row: int(row.get("id", 0)), reverse=True)

    def list_receipts(self, *, segment_code: str | None = None) -> list[dict[str, Any]]:
        rows = [dict(row) for row in self._receipts()]
        if segment_code:
            rows = [row for row in rows if row.get("段编号") == segment_code]
        return sorted(rows, key=lambda row: int(row.get("id", 0)), reverse=True)

    def trace(self, segment_id: int) -> dict[str, Any] | None:
        """逐段回查：位点、待补范围、回执、暂停记录与调度轮次都能对上。"""
        segment = self._find_segment(segment_id=segment_id)
        if segment is None:
            return None
        cursor = self.sync_source_cursor(str(segment.get("源表")), str(segment.get("源端编号") or "") or None)
        code = segment.get("段编号")
        rounds = [
            {
                "轮次编号": row.get("轮次编号"),
                "状态": row.get("状态"),
                "说明": row.get("说明"),
                "创建时间": row.get("创建时间"),
            }
            for row in self._rounds()
            if any(item.get("段编号") == code for item in (row.get("调度段") or []))
            or any(item.get("段编号") == code for item in (row.get("暂停段") or []))
        ]
        return {
            "段": dict(segment),
            "数据位点": {
                "起始位点": segment.get("起始位点"),
                "结束位点": segment.get("结束位点"),
                "续送起点": segment.get("续送起点"),
                "已送达位点": segment.get("已送达位点"),
            },
            "待补范围": [segment.get("起始位点"), segment.get("结束位点")] if not segment.get("已结算") else [],
            "源表位点": dict(cursor),
            "回执": self.list_receipts(segment_code=code),
            "暂停记录": self.list_pauses(segment_code=code),
            "调度轮次": rounds,
            "变更捕获下发": [
                dict(row) for row in self._handoffs() if row.get("段编号") == code
            ],
        }


service = BacklogSegmentService()


# 模块级快捷入口：便于管道脚本或测试直接调用，不经过路由层。
def schedule_round(**kwargs: Any) -> dict[str, Any]:
    return service.schedule_round(**kwargs)


def acknowledge(**kwargs: Any) -> dict[str, Any]:
    return service.acknowledge(**kwargs)


def preview() -> dict[str, Any]:
    return service.preview()


def pending_ranges() -> list[dict[str, Any]]:
    return service.pending_ranges()


def cdc_intervals(**kwargs: Any) -> list[dict[str, Any]]:
    return service.cdc_intervals(**kwargs)


def disconnect(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return service.disconnect(*args, **kwargs)


def reconnect(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return service.reconnect(*args, **kwargs)
