"""背压积压段续送接口：段登记、队列容量绑定、调度、回执、断线与逐段回查。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.schemas import ActionResult, PageResult
from app.services.backlog_segment import BacklogSegmentService

router = APIRouter(prefix="/api/backlog_segment", tags=["背压续送"])

service = BacklogSegmentService()

LIST_FIELDS = ["段编号", "源表", "源端编号", "起始位点", "结束位点", "数据行数", "续送起点", "来源类型", "入库回执"]
STATUSES = ["待续送", "续送中", "已入库"]


class SegmentPayload(BaseModel):
    """登记积压段：位点可以写整数，也可以写来源库自己的字符串位点。"""

    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class CapacityPayload(BaseModel):
    queue_limit: int | str | None = None
    policy_id: int | None = None


class SegmentActionPayload(BaseModel):
    """段动作：动作名之外可带回来执位点，方便页面一趟提交。"""

    action: str
    remark: str | None = None
    start_pos: int | str | None = None
    end_pos: int | str | None = None
    receipt_id: str | None = None


class RoundPayload(BaseModel):
    round_label: str | None = None


class ReceiptPayload(BaseModel):
    segment_id: int | None = None
    segment_code: str | None = None
    start_pos: int | str | None = None
    end_pos: int | str | None = None
    receipt_id: str | None = None


class ReconnectPayload(BaseModel):
    source_table: str | None = None
    auto_schedule: bool = False


class DisconnectPayload(BaseModel):
    source_table: str | None = None
    reason: str = "连接中断，等待重连后续送"


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_segments(
    request: Request,
    keyword: str | None = Query(default=None, description="按段编号检索"),
    status: str | None = Query(default=None, description="待续送、续送中、已入库"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按字段、段编号与状态过滤积压段清单。"""
    items, total = service.list_segments(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_segments(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_segments(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="backlog_segment.csv"'})


@router.get("/summary", response_model=dict)
def summary() -> dict:
    """容量、待续送段与各源表待补范围的一次性汇总。"""
    return service.summary()


@router.get("/capacity", response_model=dict)
def capacity() -> dict:
    """队列上限、已占容量与剩余容量。"""
    return service.capacity_snapshot()


@router.put("/capacity", response_model=ActionResult)
def bind_capacity(payload: CapacityPayload) -> ActionResult:
    """把队列上限绑定到背压策略，或直接设置数值上限。"""
    snapshot, message = service.bind_capacity(queue_limit=payload.queue_limit, policy_id=payload.policy_id)
    if snapshot is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=snapshot)


@router.get("/cursors", response_model=list[dict])
def list_cursors() -> list[dict]:
    """每个源表可恢复的未提交位点段。"""
    return service.list_cursors()


@router.get("/pending-ranges", response_model=list[dict])
def pending_ranges() -> list[dict]:
    """各源表尚未拿到入库回执的范围，按段列出。"""
    return service.pending_ranges()


@router.get("/plan", response_model=dict)
def preview_round() -> dict:
    """预览下一轮先处理 orders 还是 payments；只计算不落库。"""
    return service.preview()


@router.post("/rounds", response_model=dict)
def run_round(payload: RoundPayload | None = None) -> dict:
    """执行一轮续送调度：旧积压段优先，容量不足或断线的段写入暂停记录。"""
    return service.schedule_round(round_label=(payload.round_label if payload else None))


@router.get("/rounds", response_model=list[dict])
def list_rounds() -> list[dict]:
    return service.list_rounds()


@router.get("/pauses", response_model=list[dict])
def list_pauses(
    round_code: str | None = Query(default=None),
    segment_code: str | None = Query(default=None),
) -> list[dict]:
    """未进入某一轮的段必须能说明原因，并带上数据位点与待补范围。"""
    return service.list_pauses(round_code=round_code, segment_code=segment_code)


@router.get("/receipts", response_model=list[dict])
def list_receipts(segment_code: str | None = Query(default=None)) -> list[dict]:
    return service.list_receipts(segment_code=segment_code)


@router.post("/receipts", response_model=ActionResult)
def acknowledge(payload: ReceiptPayload) -> ActionResult:
    """入库回执：同时核验起点与末点，通过才结算并释放容量；重复回执按幂等处理。"""
    result = service.acknowledge(
        segment_id=payload.segment_id,
        segment_code=payload.segment_code,
        start_pos=payload.start_pos,
        end_pos=payload.end_pos,
        receipt_id=payload.receipt_id,
    )
    entry = result.get("entry")
    return ActionResult(ok=bool(result.get("ok")), message=str(result.get("message")), entry=entry)


@router.post("/disconnect", response_model=dict)
def disconnect(payload: DisconnectPayload | None = None) -> dict:
    """标记源表断线：回执不再到达，但未回执区间保持容量占用。"""
    return service.disconnect(
        payload.source_table if payload else None,
        reason=(payload.reason if payload else "连接中断，等待重连后续送"),
    )


@router.post("/reconnect", response_model=dict)
def reconnect(payload: ReconnectPayload | None = None) -> dict:
    """源表重连：给出续送起点与待补范围，可选立刻调度一轮。"""
    return service.reconnect(
        payload.source_table if payload else None,
        auto_schedule=bool(payload.auto_schedule) if payload else False,
    )


@router.get("/cdc-intervals", response_model=list[dict])
def cdc_intervals(status: str | None = Query(default=None)) -> list[dict]:
    """交给变更捕获的源表数据续送区间，变更捕获沿原位置重读。"""
    return service.cdc_intervals(status=status)


@router.post("/cdc-intervals/{handoff_id}/consume", response_model=ActionResult)
def consume_interval(handoff_id: int) -> ActionResult:
    row, message = service.consume_cdc_interval(handoff_id)
    if row is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=row)


@router.get("/{entry_id}", response_model=dict)
def get_segment(entry_id: int) -> dict:
    """读取单段明细；不存在时给出可读的错误说明。"""
    entry = service.get_segment(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"积压段 {entry_id} 不存在")
    return entry


@router.get("/{entry_id}/trace", response_model=dict)
def trace_segment(entry_id: int) -> dict:
    """逐段回查：数据位点、待补范围、回执、暂停记录与调度轮次。"""
    detail = service.trace(entry_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"积压段 {entry_id} 不存在")
    return detail


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: SegmentActionPayload) -> ActionResult:
    """单段动作：调度续送、回执核验、断线回退；核验失败会说明起点/末点差异。"""
    result = service.run_action(
        entry_id,
        payload.action.strip(),
        start_pos=payload.start_pos,
        end_pos=payload.end_pos,
        receipt_id=payload.receipt_id,
    )
    entry = result.get("entry")
    return ActionResult(ok=bool(result.get("ok")), message=str(result.get("message")), entry=entry)


@router.post("", response_model=ActionResult)
def create_segment(payload: SegmentPayload) -> ActionResult:
    """登记一个积压段，缺字段或位点区间不合法时说明原因。"""
    entry, missing = service.create_segment(payload.values)
    if isinstance(missing, str):
        return ActionResult(ok=False, message=missing)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="积压段已登记", entry=entry)
