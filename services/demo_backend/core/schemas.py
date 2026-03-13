from dataclasses import dataclass
from typing import Any


ALLOWED_ACTIONS = {"stop", "start", "reload", "pause", "resume", "restart"}
ACTION_NORMALIZATION = {
    "stop": "stop",
    "start": "start",
    "reload": "reload",
    "pause": "stop",
    "resume": "start",
    "restart": "reload",
}
ALLOWED_TARGETS = {
    "meta-01",
    "meta-02",
    "meta-03",
    "storage-01",
    "storage-02",
    "storage-03",
}


class DemoApiError(Exception):
    def __init__(
        self,
        *,
        http_status: int,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.code = code
        self.message = message
        self.details = details or {}


@dataclass
class ActionRequest:
    action: str
    target: str
    reason: str
    client_request_id: str
    dry_run: bool

    @property
    def normalized_action(self) -> str:
        return ACTION_NORMALIZATION[self.action]


def parse_action_request(payload: Any) -> ActionRequest:
    if not isinstance(payload, dict):
        raise DemoApiError(
            http_status=400,
            code="DEMO-VAL-003",
            message="invalid request body",
            details={},
        )

    action = _required_string(payload, "action")
    target = _required_string(payload, "target")

    if action not in ALLOWED_ACTIONS:
        raise DemoApiError(
            http_status=400,
            code="DEMO-VAL-001",
            message="invalid action",
            details={"action": action},
        )

    if target not in ALLOWED_TARGETS:
        raise DemoApiError(
            http_status=422,
            code="DEMO-VAL-002",
            message="target not allowed",
            details={"target": target},
        )

    reason = payload.get("reason", "")
    if reason is None:
        reason = ""
    if not isinstance(reason, str):
        raise DemoApiError(
            http_status=400,
            code="DEMO-VAL-003",
            message="invalid reason",
            details={"field": "reason"},
        )
    reason = reason.strip()
    if len(reason) > 128:
        raise DemoApiError(
            http_status=400,
            code="DEMO-VAL-003",
            message="reason too long",
            details={"field": "reason", "max_length": 128},
        )

    client_request_id = payload.get("client_request_id", "")
    if client_request_id is None:
        client_request_id = ""
    if not isinstance(client_request_id, str):
        raise DemoApiError(
            http_status=400,
            code="DEMO-VAL-003",
            message="invalid client_request_id",
            details={"field": "client_request_id"},
        )

    dry_run = payload.get("dry_run", False)
    if not isinstance(dry_run, bool):
        raise DemoApiError(
            http_status=400,
            code="DEMO-VAL-003",
            message="invalid dry_run",
            details={"field": "dry_run"},
        )

    return ActionRequest(
        action=action,
        target=target,
        reason=reason,
        client_request_id=client_request_id.strip(),
        dry_run=dry_run,
    )


def _required_string(payload: dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str):
        raise DemoApiError(
            http_status=400,
            code="DEMO-VAL-003",
            message=f"missing required field: {field}",
            details={"field": field},
        )
    value = value.strip().lower()
    if not value:
        raise DemoApiError(
            http_status=400,
            code="DEMO-VAL-003",
            message=f"missing required field: {field}",
            details={"field": field},
        )
    return value
