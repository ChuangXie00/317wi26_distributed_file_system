from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, File, Header, Query, Request, UploadFile
from fastapi.responses import JSONResponse

from core.action_executor import ActionExecutor
from core.aggregator import StateAggregator
from core.event_engine import ALLOWED_EVENT_FILTERS, EventEngine
from core.file_service import DemoFileError, DemoFileService
from core.metrics_engine import MetricsEngine
from core.schemas import DemoApiError, parse_action_request
from infra.meta_entry_client import MetaEntryClient


# 统一 API 路由前缀，保持与 8890 契约一致。
router = APIRouter(prefix="/api/demo", tags=["demo"])
# 状态聚合器：负责拉取 entry/membership/leader/replication 并组装统一快照。
state_aggregator = StateAggregator(MetaEntryClient())
# 动作执行器：负责 action 参数校验、归一化、执行与错误码映射。
action_executor = ActionExecutor()
# 事件引擎：负责时间线缓存、增量查询和标准事件生产。
event_engine = EventEngine()
# 指标引擎：负责 /metrics 口径聚合与无样本处理。
metrics_engine = MetricsEngine(event_engine)
file_service = DemoFileService()


def _request_id(x_request_id: str | None) -> str:
    # 优先透传客户端 request_id，缺失时由后端生成，便于链路追踪。
    if x_request_id:
        return x_request_id
    return f"req_{uuid4().hex[:12]}"


def _timestamp() -> str:
    # 所有响应统一使用 UTC ISO8601 时间戳。
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ok_response(request_id: str, data: dict[str, Any], status_code: int = 200) -> JSONResponse:
    # 成功包络：与设计文档约定的 ok/request_id/timestamp/data 结构对齐。
    return JSONResponse(
        status_code=status_code,
        content={
            "ok": True,
            "request_id": request_id,
            "timestamp": _timestamp(),
            "data": data,
        },
    )


def _error_response(
    request_id: str,
    status_code: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    # 失败包络：统一输出标准错误码，便于前端和日志按 code 处理。
    return JSONResponse(
        status_code=status_code,
        content={
            "ok": False,
            "request_id": request_id,
            "timestamp": _timestamp(),
            "error": {
                "code": code,
                "message": message,
                "details": details or {},
            },
        },
    )


@router.get("/health")
def get_health(x_request_id: str | None = Header(default=None, alias="X-Request-Id")):
    # 健康检查仅用于服务可用性探针，不依赖上游 debug 接口。
    request_id = _request_id(x_request_id)
    return _ok_response(
        request_id=request_id,
        data={
            "service": "demo-backend",
            "status": "up",
        },
    )


@router.get("/state")
def get_state(x_request_id: str | None = Header(default=None, alias="X-Request-Id")):
    # 状态接口：聚合多源数据并在必要时降级返回。
    request_id = _request_id(x_request_id)
    result = state_aggregator.aggregate()
    if not result.ok:
        # 全上游不可用时按契约返回 503 + DEMO-UPS-002。
        return _error_response(
            request_id=request_id,
            status_code=503,
            code=result.error_code or "DEMO-UPS-002",
            message=result.error_message or "all upstream debug sources unavailable",
            details=result.error_details,
        )

    # 每次成功 state 拉取后，把快照送入事件引擎做差分检测。
    snapshot = result.data or {}
    event_engine.ingest_snapshot(snapshot)
    return _ok_response(request_id=request_id, data=snapshot)


@router.post("/action")
async def post_action(request: Request, x_request_id: str | None = Header(default=None, alias="X-Request-Id")):
    # 动作接口：执行高风险控制动作，必须返回明确回执。
    request_id = _request_id(x_request_id)
    try:
        # 请求体必须为 JSON 对象，格式错误直接返回参数错误码。
        payload = await request.json()
    except Exception:
        return _error_response(
            request_id=request_id,
            status_code=400,
            code="DEMO-VAL-003",
            message="invalid request body",
            details={},
        )

    try:
        # 先做参数校验，便于在“执行前”准确写入 action.accepted 事件。
        action_request = parse_action_request(payload)
        accepted_correlation_id = action_request.client_request_id or request_id
        event_engine.emit_action_accepted(
            input_action=action_request.action,
            normalized_action=action_request.normalized_action,
            target=action_request.target,
            correlation_id=accepted_correlation_id,
            client_request_id=action_request.client_request_id,
        )

        # 执行动作并记录 action.succeeded 事件。
        result = action_executor.execute_request(action_request)
        result_data = result.to_dict()
        state_aggregator.apply_optimistic_action(
            normalized_action=action_request.normalized_action,
            target=action_request.target,
        )
        event_engine.emit_action_succeeded(result_data)
        return _ok_response(request_id=request_id, data=result_data)
    except DemoApiError as exc:
        # 校验通过后才有 action_request；否则说明是纯参数错误，不应写失败事件。
        action_request = _safe_parse_action_request(payload)
        if action_request is not None:
            event_engine.emit_action_failed(
                input_action=action_request.action,
                normalized_action=action_request.normalized_action,
                target=action_request.target,
                error_code=exc.code,
                error_message=exc.message,
                details=exc.details,
                correlation_id=action_request.client_request_id or request_id,
            )
        return _error_response(
            request_id=request_id,
            status_code=exc.http_status,
            code=exc.code,
            message=exc.message,
            details=exc.details,
        )
    except Exception as exc:
        # 兜底异常：统一降为控制执行失败，并记录 action.failed 事件。
        action_request = _safe_parse_action_request(payload)
        if action_request is not None:
            event_engine.emit_action_failed(
                input_action=action_request.action,
                normalized_action=action_request.normalized_action,
                target=action_request.target,
                error_code="DEMO-CTL-003",
                error_message="compose command failed",
                details={"error": str(exc)},
                correlation_id=action_request.client_request_id or request_id,
            )
        return _error_response(
            request_id=request_id,
            status_code=500,
            code="DEMO-CTL-003",
            message="compose command failed",
            details={"error": str(exc)},
        )


@router.get("/events")
def get_events(request: Request, x_request_id: str | None = Header(default=None, alias="X-Request-Id")):
    # 事件流接口：支持 since_seq 增量读取与 type 多值筛选。
    request_id = _request_id(x_request_id)
    params = request.query_params

    try:
        since_seq = _parse_int_query(params.get("since_seq"), default=0, field="since_seq", min_value=0)
        limit = _parse_int_query(params.get("limit"), default=50, field="limit", min_value=1, max_value=200)
        type_filters = _parse_type_filters(params.getlist("type"))
    except ValueError as exc:
        return _error_response(
            request_id=request_id,
            status_code=400,
            code="DEMO-VAL-003",
            message=str(exc),
            details={},
        )

    try:
        data = event_engine.query_events(since_seq=since_seq, limit=limit, filters=type_filters)
    except Exception as exc:
        return _error_response(
            request_id=request_id,
            status_code=500,
            code="DEMO-EVT-001",
            message="event engine error",
            details={"error": str(exc)},
        )
    return _ok_response(request_id=request_id, data=data)


@router.get("/metrics")
def get_metrics(x_request_id: str | None = Header(default=None, alias="X-Request-Id")):
    # 指标接口：输出恢复耗时和关键 KPI；无样本时按契约返回 503。
    request_id = _request_id(x_request_id)
    try:
        result = metrics_engine.collect()
    except Exception as exc:
        return _error_response(
            request_id=request_id,
            status_code=500,
            code="DEMO-EVT-001",
            message="metrics engine error",
            details={"error": str(exc)},
        )

    if not result.has_sample:
        return _error_response(
            request_id=request_id,
            status_code=503,
            code="DEMO-MET-001",
            message="metrics sample not ready",
            details=result.data,
        )
    return _ok_response(request_id=request_id, data=result.data)


@router.post("/file/upload")
async def post_file_upload(
    file: UploadFile = File(...),
    file_name: str | None = None,
    x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
):
    # 文件上传：由 demo-backend 代理执行 chunk 注册/上传/commit。
    request_id = _request_id(x_request_id)
    try:
        raw = await file.read()
        chosen_name = str(file_name or file.filename or "").strip()
        result = file_service.upload_file(file_name=chosen_name, content=raw or b"")
        return _ok_response(request_id=request_id, data=result.to_dict())
    except DemoFileError as exc:
        return _error_response(
            request_id=request_id,
            status_code=exc.http_status,
            code=exc.code,
            message=exc.message,
            details=exc.details or {},
        )
    except Exception as exc:
        return _error_response(
            request_id=request_id,
            status_code=500,
            code="DEMO-FILE-999",
            message="unexpected file upload error",
            details={"error": str(exc)},
        )


@router.get("/file/download")
def get_file_download(
    file_name: str = Query(..., min_length=1),
    x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
):
    # 文件下载：按元数据拼装全部 chunk 后，返回 base64 内容给前端还原下载。
    request_id = _request_id(x_request_id)
    try:
        result = file_service.download_file(file_name=file_name)
        # 统一返回结构，前端仍通过 DemoApiError 体系处理错误。
        return _ok_response(request_id=request_id, data=result.to_dict())
    except DemoFileError as exc:
        return _error_response(
            request_id=request_id,
            status_code=exc.http_status,
            code=exc.code,
            message=exc.message,
            details=exc.details or {},
        )
    except Exception as exc:
        return _error_response(
            request_id=request_id,
            status_code=500,
            code="DEMO-FILE-999",
            message="unexpected file download error",
            details={"error": str(exc)},
        )


@router.get("/file/replicas")
def get_file_replicas(
    file_name: str = Query(..., min_length=1),
    x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
):
    # 副本矩阵：返回文件每个 chunk 的 replica 分布，供 File Panel 展示。
    request_id = _request_id(x_request_id)
    try:
        result = file_service.get_replica_matrix(file_name=file_name)
        return _ok_response(request_id=request_id, data=result.to_dict())
    except DemoFileError as exc:
        return _error_response(
            request_id=request_id,
            status_code=exc.http_status,
            code=exc.code,
            message=exc.message,
            details=exc.details or {},
        )
    except Exception as exc:
        return _error_response(
            request_id=request_id,
            status_code=500,
            code="DEMO-FILE-999",
            message="unexpected file replica query error",
            details={"error": str(exc)},
        )


@router.get("/file/list")
def get_file_list(
    limit: int = Query(default=200, ge=1, le=1000),
    x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
):
    request_id = _request_id(x_request_id)
    try:
        result = file_service.list_files(limit=limit)
        return _ok_response(request_id=request_id, data=result.to_dict())
    except DemoFileError as exc:
        return _error_response(
            request_id=request_id,
            status_code=exc.http_status,
            code=exc.code,
            message=exc.message,
            details=exc.details or {},
        )
    except Exception as exc:
        return _error_response(
            request_id=request_id,
            status_code=500,
            code="DEMO-FILE-999",
            message="unexpected file list error",
            details={"error": str(exc)},
        )


@router.delete("/file/{file_name}")
def delete_file(
    file_name: str,
    x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
):
    request_id = _request_id(x_request_id)
    try:
        result = file_service.delete_file(file_name=file_name)
        return _ok_response(request_id=request_id, data=result.to_dict())
    except DemoFileError as exc:
        return _error_response(
            request_id=request_id,
            status_code=exc.http_status,
            code=exc.code,
            message=exc.message,
            details=exc.details or {},
        )
    except Exception as exc:
        return _error_response(
            request_id=request_id,
            status_code=500,
            code="DEMO-FILE-999",
            message="unexpected file delete error",
            details={"error": str(exc)},
        )


def _parse_int_query(
    raw: str | None,
    *,
    default: int,
    field: str,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    # Query 整数参数解析：统一做缺省、类型和范围校验。
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"invalid query parameter: {field}") from exc

    if min_value is not None and value < min_value:
        raise ValueError(f"invalid query parameter: {field}")
    if max_value is not None and value > max_value:
        raise ValueError(f"invalid query parameter: {field}")
    return value


def _parse_type_filters(raw_filters: list[str]) -> list[str]:
    # 支持 type=leader&type=action 或 type=leader,action 两种传参方式。
    values: list[str] = []
    for raw in raw_filters:
        parts = [part.strip().lower() for part in raw.split(",")]
        for part in parts:
            if part:
                values.append(part)

    if not values:
        return []
    invalid = [value for value in values if value not in ALLOWED_EVENT_FILTERS]
    if invalid:
        raise ValueError("invalid query parameter: type")
    return list(dict.fromkeys(values))


def _safe_parse_action_request(payload: Any):
    # 尝试把 payload 解析为 ActionRequest；失败时返回 None，供异常路径容错使用。
    try:
        return parse_action_request(payload)
    except Exception:
        return None
