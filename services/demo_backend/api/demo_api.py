from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, Header


router = APIRouter(prefix="/api/demo", tags=["demo"])


def _request_id(x_request_id: str | None) -> str:
    if x_request_id:
        return x_request_id
    return f"req_{uuid4().hex[:12]}"


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@router.get("/health")
def get_health(x_request_id: str | None = Header(default=None, alias="X-Request-Id")):
    return {
        "ok": True,
        "request_id": _request_id(x_request_id),
        "timestamp": _timestamp(),
        "data": {
            "service": "demo-backend",
            "status": "up",
        },
    }
