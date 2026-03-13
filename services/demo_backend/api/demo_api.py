from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from core.action_executor import ActionExecutor
from core.aggregator import StateAggregator
from core.schemas import DemoApiError
from infra.meta_entry_client import MetaEntryClient


router = APIRouter(prefix="/api/demo", tags=["demo"])
state_aggregator = StateAggregator(MetaEntryClient())
action_executor = ActionExecutor()


def _request_id(x_request_id: str | None) -> str:
    if x_request_id:
        return x_request_id
    return f"req_{uuid4().hex[:12]}"


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ok_response(request_id: str, data: dict[str, Any], status_code: int = 200) -> JSONResponse:
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
    request_id = _request_id(x_request_id)
    result = state_aggregator.aggregate()
    if not result.ok:
        return _error_response(
            request_id=request_id,
            status_code=503,
            code=result.error_code or "DEMO-UPS-002",
            message=result.error_message or "all upstream debug sources unavailable",
            details=result.error_details,
        )
    return _ok_response(request_id=request_id, data=result.data or {})


@router.post("/action")
async def post_action(request: Request, x_request_id: str | None = Header(default=None, alias="X-Request-Id")):
    request_id = _request_id(x_request_id)
    try:
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
        result = action_executor.execute(payload)
        return _ok_response(request_id=request_id, data=result.to_dict())
    except DemoApiError as exc:
        return _error_response(
            request_id=request_id,
            status_code=exc.http_status,
            code=exc.code,
            message=exc.message,
            details=exc.details,
        )
    except Exception as exc:
        return _error_response(
            request_id=request_id,
            status_code=500,
            code="DEMO-CTL-003",
            message="compose command failed",
            details={"error": str(exc)},
        )
