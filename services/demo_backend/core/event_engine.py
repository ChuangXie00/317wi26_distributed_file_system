import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


# /events 支持的筛选类型（与 8890 契约保持一致）。
ALLOWED_EVENT_FILTERS = {"node", "leader", "entry", "action", "recovery", "error"}


@dataclass
class DemoEvent:
    # 标准事件模型：字段与 /api/demo/events 响应结构一致。
    seq: int
    ts: str
    type: str
    severity: str
    entity_type: str
    entity_id: str
    title: str
    message: str
    correlation_id: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "ts": self.ts,
            "type": self.type,
            "severity": self.severity,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "title": self.title,
            "message": self.message,
            "correlation_id": self.correlation_id,
            "payload": self.payload,
        }


class EventEngine:
    def __init__(self, capacity: int = 2000) -> None:
        # 事件总线线程锁，保护事件序号与缓存的一致性。
        self._lock = threading.Lock()
        # 环形缓冲区：超过容量自动 FIFO 淘汰最旧事件。
        self._events: deque[DemoEvent] = deque(maxlen=capacity)
        # 下一个事件序号（单调递增）。
        self._next_seq = 1

        # 这些字段用于 state 快照差分，生成 leader/entry/node 变化事件。
        self._last_leader = ""
        self._last_entry_active_leader = ""
        self._last_node_status: dict[str, str] = {}

        # 指标相关状态：用于 /metrics 输出与恢复耗时口径计算。
        self._leader_switch_count = 0
        self._last_failover_started_at: str | None = None
        self._last_failover_recovered_at: str | None = None
        self._last_failover_recovery_seconds: float | None = None

        # 恢复流程状态机：用于识别 recovery.started -> recovery.completed。
        self._recovery_in_progress = False
        self._recovery_start_dt: datetime | None = None
        self._recovery_anchor_leader = ""
        self._recovery_stable_cycles = 0
        self._recovery_incomplete_emitted = False

    def ingest_snapshot(self, snapshot: dict[str, Any]) -> None:
        # 基于最新状态快照做差分，产出 leader/entry/node 标准事件，并推进恢复状态机。
        with self._lock:
            leader_view = _as_dict(snapshot.get("leader_view"))
            entry = _as_dict(snapshot.get("entry"))
            membership_view = _as_dict(snapshot.get("membership_view"))
            membership = _as_dict(membership_view.get("membership"))

            previous_leader = self._last_leader
            current_leader = _as_str(leader_view.get("leader"))
            if previous_leader and current_leader and current_leader != previous_leader:
                self._leader_switch_count += 1
                self._append_event_locked(
                    event_type="leader.switched",
                    severity="info",
                    entity_type="meta",
                    entity_id=current_leader,
                    title="Leader switched",
                    message=f"leader changed {previous_leader} -> {current_leader}",
                    correlation_id="",
                    payload={
                        "from_leader": previous_leader,
                        "to_leader": current_leader,
                    },
                )
            if current_leader:
                self._last_leader = current_leader

            current_entry_leader = _as_str(entry.get("active_leader_id"))
            if (
                self._last_entry_active_leader
                and current_entry_leader
                and current_entry_leader != self._last_entry_active_leader
            ):
                self._append_event_locked(
                    event_type="entry.switched",
                    severity="info",
                    entity_type="entry",
                    entity_id="entry",
                    title="Entry switched",
                    message=f"entry active leader changed {self._last_entry_active_leader} -> {current_entry_leader}",
                    correlation_id="",
                    payload={
                        "from_active_leader_id": self._last_entry_active_leader,
                        "to_active_leader_id": current_entry_leader,
                    },
                )
            if current_entry_leader:
                self._last_entry_active_leader = current_entry_leader

            # 节点状态变化事件：仅在同一节点状态与上次不同且都非空时发出。
            next_node_status: dict[str, str] = {}
            for node_id, node_info in membership.items():
                info = _as_dict(node_info)
                status = _as_str(info.get("status")).lower()
                if not status:
                    continue
                next_node_status[node_id] = status
                prev_status = self._last_node_status.get(node_id, "")
                if prev_status and status != prev_status:
                    severity = "warn" if status in {"suspected", "dead"} else "info"
                    self._append_event_locked(
                        event_type="node.status_changed",
                        severity=severity,
                        entity_type=_node_entity_type(node_id),
                        entity_id=node_id,
                        title="Node status changed",
                        message=f"node {node_id} status {prev_status} -> {status}",
                        correlation_id="",
                        payload={
                            "from_status": prev_status,
                            "to_status": status,
                        },
                    )

                # 观测型恢复起点：当前 leader 从 alive -> suspected/dead 时触发。
                if (
                    node_id == previous_leader
                    and prev_status == "alive"
                    and status in {"suspected", "dead"}
                ):
                    self._start_recovery_locked(
                        anchor_leader=previous_leader,
                        reason="leader_alive_to_unhealthy",
                        correlation_id="",
                        payload={"node_id": node_id, "from_status": prev_status, "to_status": status},
                    )
            self._last_node_status = next_node_status

            self._evaluate_recovery_completion_locked(entry=entry, leader_view=leader_view)

    def emit_action_accepted(
        self,
        *,
        input_action: str,
        normalized_action: str,
        target: str,
        correlation_id: str,
        client_request_id: str,
    ) -> None:
        # 动作接收事件：用于记录“已受理”时刻与请求参数。
        with self._lock:
            self._append_event_locked(
                event_type="action.accepted",
                severity="info",
                entity_type=_node_entity_type(target),
                entity_id=target,
                title="Action accepted",
                message=f"action accepted {input_action}->{normalized_action} on {target}",
                correlation_id=correlation_id,
                payload={
                    "input_action": input_action,
                    "normalized_action": normalized_action,
                    "target": target,
                    "client_request_id": client_request_id,
                },
            )

            # 操作型恢复起点：收到 stop 且目标为当前 leader（meta 节点）时触发。
            if (
                normalized_action == "stop"
                and target.startswith("meta-")
                and self._last_leader
                and target == self._last_leader
            ):
                self._start_recovery_locked(
                    anchor_leader=target,
                    reason="action_stop_current_leader",
                    correlation_id=correlation_id,
                    payload={"input_action": input_action, "normalized_action": normalized_action, "target": target},
                )

    def emit_action_succeeded(self, result: dict[str, Any]) -> None:
        # 动作成功事件：关联 action_id，供时间线与结果卡片联动。
        with self._lock:
            target = _as_str(result.get("target"))
            action_id = _as_str(result.get("action_id"))
            self._append_event_locked(
                event_type="action.succeeded",
                severity="info",
                entity_type=_node_entity_type(target),
                entity_id=target,
                title="Action succeeded",
                message=f"action {result.get('normalized_action', '')} succeeded on {target}",
                correlation_id=action_id,
                payload={
                    "action_id": action_id,
                    "input_action": _as_str(result.get("input_action")),
                    "normalized_action": _as_str(result.get("normalized_action")),
                    "target": target,
                    "duration_ms": _as_int(result.get("duration_ms")),
                },
            )

    def emit_action_failed(
        self,
        *,
        input_action: str,
        normalized_action: str,
        target: str,
        error_code: str,
        error_message: str,
        details: dict[str, Any],
        correlation_id: str,
    ) -> None:
        # 动作失败事件：用于 UI 告警和问题定位。
        with self._lock:
            self._append_event_locked(
                event_type="action.failed",
                severity="error",
                entity_type=_node_entity_type(target),
                entity_id=target,
                title="Action failed",
                message=f"action {input_action}->{normalized_action} failed on {target}: {error_code}",
                correlation_id=correlation_id,
                payload={
                    "input_action": input_action,
                    "normalized_action": normalized_action,
                    "target": target,
                    "error_code": error_code,
                    "error_message": error_message,
                    "details": details,
                },
            )

    def query_events(
        self,
        *,
        since_seq: int = 0,
        limit: int = 50,
        filters: list[str] | None = None,
    ) -> dict[str, Any]:
        # 增量查询：按 since_seq 向后取，并支持按事件大类筛选。
        with self._lock:
            matched = []
            for event in self._events:
                if event.seq <= since_seq:
                    continue
                if not _match_filter(event.type, filters):
                    continue
                matched.append(event)

            has_more = len(matched) > limit
            page = matched[:limit]
            if page:
                next_seq = page[-1].seq + 1
            else:
                next_seq = max(self._next_seq, since_seq + 1)

            return {
                "events": [event.to_dict() for event in page],
                "next_seq": next_seq,
                "has_more": has_more,
            }

    def get_metrics_snapshot(self, sampling_window_seconds: int) -> dict[str, Any]:
        # 读取 metrics 快照：由 /metrics 接口直接透传。
        with self._lock:
            return {
                "failover_recovery_seconds": self._last_failover_recovery_seconds,
                "last_failover_started_at": self._last_failover_started_at,
                "last_failover_recovered_at": self._last_failover_recovered_at,
                "leader_switch_count": self._leader_switch_count,
                "event_backlog": len(self._events),
                "sampling_window_seconds": sampling_window_seconds,
            }

    def has_recovery_sample(self) -> bool:
        # 是否已产生有效恢复样本。
        with self._lock:
            return self._last_failover_recovery_seconds is not None

    def emit_recovery_incomplete_if_needed(self) -> None:
        # 无有效样本时写入 recovery.incomplete，避免每次 /metrics 都重复刷屏。
        with self._lock:
            if self._last_failover_recovery_seconds is not None:
                return
            if self._recovery_in_progress:
                return
            if self._recovery_incomplete_emitted:
                return
            self._append_event_locked(
                event_type="recovery.incomplete",
                severity="warn",
                entity_type="recovery",
                entity_id="cluster",
                title="Recovery sample incomplete",
                message="no valid failover recovery sample yet",
                correlation_id="",
                payload={},
            )
            self._recovery_incomplete_emitted = True

    def _start_recovery_locked(
        self,
        *,
        anchor_leader: str,
        reason: str,
        correlation_id: str,
        payload: dict[str, Any],
    ) -> None:
        # 仅在未开始时启动恢复计时，防止重复触发覆盖 t_start。
        if self._recovery_in_progress:
            return
        if not anchor_leader:
            return

        self._recovery_in_progress = True
        self._recovery_start_dt = _utc_now_dt()
        self._recovery_anchor_leader = anchor_leader
        self._recovery_stable_cycles = 0
        self._last_failover_started_at = _dt_to_iso(self._recovery_start_dt)
        self._recovery_incomplete_emitted = False

        data = dict(payload)
        data.update({"reason": reason, "anchor_leader": anchor_leader})
        self._append_event_locked(
            event_type="recovery.started",
            severity="info",
            entity_type="recovery",
            entity_id=anchor_leader,
            title="Recovery started",
            message=f"recovery timer started for leader {anchor_leader}",
            correlation_id=correlation_id,
            payload=data,
        )

    def _evaluate_recovery_completion_locked(self, *, entry: dict[str, Any], leader_view: dict[str, Any]) -> None:
        # 满足连续 2 个采样周期条件后，判定恢复完成并计算耗时。
        if not self._recovery_in_progress:
            return
        if self._recovery_start_dt is None:
            return

        active_leader = _as_str(entry.get("active_leader_id"))
        observed_leader = _as_str(leader_view.get("leader"))
        single_observed_leader = bool(observed_leader)
        single_writable_leader = bool(leader_view.get("writable_leader"))
        switched_to_new_leader = bool(active_leader) and active_leader != self._recovery_anchor_leader

        if switched_to_new_leader and single_observed_leader and single_writable_leader:
            self._recovery_stable_cycles += 1
        else:
            self._recovery_stable_cycles = 0

        if self._recovery_stable_cycles < 2:
            return

        completed_at = _utc_now_dt()
        duration_seconds = (completed_at - self._recovery_start_dt).total_seconds()
        self._last_failover_recovered_at = _dt_to_iso(completed_at)
        self._last_failover_recovery_seconds = round(duration_seconds, 3)

        self._append_event_locked(
            event_type="recovery.completed",
            severity="info",
            entity_type="recovery",
            entity_id=active_leader or observed_leader,
            title="Recovery completed",
            message=f"recovery completed in {self._last_failover_recovery_seconds}s",
            correlation_id="",
            payload={
                "anchor_leader": self._recovery_anchor_leader,
                "active_leader_id": active_leader,
                "observed_leader": observed_leader,
                "failover_recovery_seconds": self._last_failover_recovery_seconds,
            },
        )

        self._recovery_in_progress = False
        self._recovery_start_dt = None
        self._recovery_stable_cycles = 0

    def _append_event_locked(
        self,
        *,
        event_type: str,
        severity: str,
        entity_type: str,
        entity_id: str,
        title: str,
        message: str,
        correlation_id: str,
        payload: dict[str, Any],
    ) -> None:
        # 仅可在持锁状态调用：保证 seq 连续且事件写入原子化。
        event = DemoEvent(
            seq=self._next_seq,
            ts=_dt_to_iso(_utc_now_dt()),
            type=event_type,
            severity=severity,
            entity_type=entity_type,
            entity_id=entity_id,
            title=title,
            message=message,
            correlation_id=correlation_id,
            payload=payload,
        )
        self._events.append(event)
        self._next_seq += 1


def _match_filter(event_type: str, filters: list[str] | None) -> bool:
    # 过滤规则：筛选值 node/leader/... 对应匹配 type 前缀。
    if not filters:
        return True
    prefix = event_type.split(".", 1)[0]
    return prefix in filters


def _node_entity_type(node_id: str) -> str:
    if node_id.startswith("meta-"):
        return "meta"
    if node_id.startswith("storage-"):
        return "storage"
    return "node"


def _utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
