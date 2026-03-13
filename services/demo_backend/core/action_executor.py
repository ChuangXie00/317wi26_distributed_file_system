import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from core.schemas import ActionRequest, DemoApiError, parse_action_request
from infra.compose_runner import ComposeRunner


@dataclass
class ActionExecutionResult:
    # 动作执行回执：与 /api/demo/action 的 data 字段一一对应。
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
        # 序列化为响应 JSON，避免 API 层重复拼装字段。
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
        # Compose 执行器，默认调用真实 docker compose。
        self._compose_runner = compose_runner or ComposeRunner()
        # 每个 target 一个互斥锁，保证同节点动作串行。
        self._target_locks: dict[str, threading.Lock] = {}
        # 保护 _target_locks 字典本身的并发访问。
        self._lock_guard = threading.Lock()

    def execute(self, payload: Any) -> ActionExecutionResult:
        # 入口流程：请求校验 -> 节点加锁 -> 实际执行 -> 释放锁。
        request = parse_action_request(payload)
        return self.execute_request(request)

    def execute_request(self, request: ActionRequest) -> ActionExecutionResult:
        # 已完成参数校验时可直接调用此方法，避免重复校验。
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
        # 在已持有节点锁的前提下执行 compose 命令。
        action_id = _new_action_id()
        run_result = self._compose_runner.run(
            normalized_action=request.normalized_action,
            target=request.target,
            dry_run=request.dry_run,
        )

        # 超时映射为 504 / DEMO-CTL-002。
        if run_result.timeout:
            raise DemoApiError(
                http_status=504,
                code="DEMO-CTL-002",
                message="compose command timeout",
                details={"target": request.target, "timeout_sec": self._compose_runner.timeout_sec},
            )

        # 非 0 返回码映射为 500 / DEMO-CTL-003。
        if run_result.exit_code not in (0, None):
            raise DemoApiError(
                http_status=500,
                code="DEMO-CTL-003",
                message="compose command failed",
                details={"exit_code": run_result.exit_code, "target": request.target},
            )

        # 成功回执包含动作标识、归一化动作、耗时与命令输出尾部。
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
        # 懒创建目标锁，避免初始化阶段创建无用锁对象。
        with self._lock_guard:
            if target not in self._target_locks:
                self._target_locks[target] = threading.Lock()
            return self._target_locks[target]


def _new_action_id() -> str:
    # 生成动作关联 ID，供时间线与日志关联使用。
    now = datetime.now(timezone.utc)
    return f"act_{now.strftime('%Y%m%d')}_{uuid4().hex[:8]}"
