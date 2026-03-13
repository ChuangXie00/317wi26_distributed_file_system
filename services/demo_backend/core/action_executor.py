import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from core.schemas import ActionRequest, DemoApiError, parse_action_request
from infra.compose_runner import ComposeRunner


@dataclass
class ActionExecutionResult:
    action_id: str
    input_action: str
    normalized_action: str
    target: str
    status: str
    started_at: str
    finished_at: str
    duration_ms: int
    command: str
    stdout_tail: str
    stderr_tail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "input_action": self.input_action,
            "normalized_action": self.normalized_action,
            "target": self.target,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "command": self.command,
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
        }


class ActionExecutor:
    def __init__(self, compose_runner: ComposeRunner | None = None) -> None:
        self._compose_runner = compose_runner or ComposeRunner()
        self._target_locks: dict[str, threading.Lock] = {}
        self._lock_guard = threading.Lock()

    def execute(self, payload: Any) -> ActionExecutionResult:
        request = parse_action_request(payload)
        lock = self._get_target_lock(request.target)
        if not lock.acquire(blocking=False):
            raise DemoApiError(
                http_status=409,
                code="DEMO-CTL-001",
                message="target already has in-flight action",
                details={"target": request.target},
            )

        try:
            return self._execute_locked(request)
        finally:
            lock.release()

    def _execute_locked(self, request: ActionRequest) -> ActionExecutionResult:
        action_id = _new_action_id()
        run_result = self._compose_runner.run(
            normalized_action=request.normalized_action,
            target=request.target,
            dry_run=request.dry_run,
        )

        if run_result.timeout:
            raise DemoApiError(
                http_status=504,
                code="DEMO-CTL-002",
                message="compose command timeout",
                details={"target": request.target, "timeout_sec": self._compose_runner.timeout_sec},
            )

        if run_result.exit_code not in (0, None):
            raise DemoApiError(
                http_status=500,
                code="DEMO-CTL-003",
                message="compose command failed",
                details={"exit_code": run_result.exit_code, "target": request.target},
            )

        return ActionExecutionResult(
            action_id=action_id,
            input_action=request.action,
            normalized_action=request.normalized_action,
            target=request.target,
            status="succeeded",
            started_at=run_result.started_at,
            finished_at=run_result.finished_at,
            duration_ms=run_result.duration_ms,
            command=run_result.command,
            stdout_tail=run_result.stdout_tail,
            stderr_tail=run_result.stderr_tail,
        )

    def _get_target_lock(self, target: str) -> threading.Lock:
        with self._lock_guard:
            if target not in self._target_locks:
                self._target_locks[target] = threading.Lock()
            return self._target_locks[target]


def _new_action_id() -> str:
    now = datetime.now(timezone.utc)
    return f"act_{now.strftime('%Y%m%d')}_{uuid4().hex[:8]}"
