from dataclasses import dataclass
from typing import Any


# 对外允许的动作枚举（兼容 04 与 8883 两套文案）。
ALLOWED_ACTIONS = {"stop", "start", "reload", "pause", "resume", "restart"}
# 动作归一化映射：最终只落到 stop/start/reload 三类控制语义。
ACTION_NORMALIZATION = {
    "stop": "stop",
    "start": "start",
    "reload": "reload",
    "pause": "stop",
    "resume": "start",
    "restart": "reload",
}
# 节点白名单：仅允许演示环境中明确声明的目标节点。
ALLOWED_TARGETS = {
    "meta-01",
    "meta-02",
    "meta-03",
    "storage-01",
    "storage-02",
    "storage-03",
}


class DemoApiError(Exception):
    # 统一业务异常对象：把 HTTP 状态码与错误码绑在一起向 API 层传递。
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
    # 规范化后的动作请求模型，供执行器直接消费。
    action: str
    target: str
    reason: str
    client_request_id: str
    dry_run: bool

    @property
    def normalized_action(self) -> str:
        # 根据动作归一化表获取最终执行动作。
        return ACTION_NORMALIZATION[self.action]


def parse_action_request(payload: Any) -> ActionRequest:
    # 入口校验：将原始 JSON 转为强约束的 ActionRequest。
    if not isinstance(payload, dict):
        raise DemoApiError(
            http_status=400,
            code="DEMO-VAL-003",
            message="invalid request body",
            details={},
        )

    action = _required_string(payload, "action")
    target = _required_string(payload, "target")

    # 动作必须在契约枚举内。
    if action not in ALLOWED_ACTIONS:
        raise DemoApiError(
            http_status=400,
            code="DEMO-VAL-001",
            message="invalid action",
            details={"action": action},
        )

    # 目标节点必须命中白名单，防止误操作任意容器。
    if target not in ALLOWED_TARGETS:
        raise DemoApiError(
            http_status=422,
            code="DEMO-VAL-002",
            message="target not allowed",
            details={"target": target},
        )

    # reason 用于审计与演示说明，限制长度避免日志污染。
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

    # client_request_id 可选，用于前端侧幂等辅助和关联追踪。
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

    # dry_run 仅允许布尔值，避免隐式类型转换。
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
    # 必填字符串字段校验：存在、类型正确、去空白后非空。
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
