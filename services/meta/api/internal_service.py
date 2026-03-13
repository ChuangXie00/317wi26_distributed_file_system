import json
from datetime import datetime, timezone
from typing import Dict
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest, urlopen

from fastapi import HTTPException

from core.config import (
    HEARTBEAT_WRITE_MIN_INTERVAL_SEC,
    META_INTERNAL_TIMEOUT_SEC,
    META_NODE_ID,
    STORAGE_NODES,
    build_meta_base_url,
)
from core.election import handle_incoming_coordinator, handle_incoming_election, handle_incoming_vote_request
from core.replication import apply_replicated_state, build_state_snapshot, record_leader_heartbeat, trigger_takeover_async
from core.runtime import get_lamport_clock, get_runtime_snapshot, is_writable_leader
from core.state import State, get_membership_snapshot, mark_storage_heartbeat, mutate_state, refresh_storage_membership


# 写接口统一门禁：仅 writable leader 允许处理写请求。
def ensure_leader_write_api() -> None:
    if not is_writable_leader():
        runtime = get_runtime_snapshot()
        raise HTTPException(
            status_code=409,
            detail=_format_non_writable_leader_detail(runtime),
        )


def _format_non_writable_leader_detail(runtime: Dict[str, object]) -> str:
    return (
        "node is not writable leader: "
        f"role={runtime.get('role')}, "
        f"leader={runtime.get('current_leader_id')}, "
        f"epoch={runtime.get('leader_epoch')}"
    )


# 内部 JSON POST 工具：用于 follower 转发 storage heartbeat 到观测 leader。
def _post_json(
    url: str,
    payload: Dict[str, object],
    *,
    timeout_sec: float,
    headers: Dict[str, str] | None = None,
) -> Dict[str, object]:
    request_headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if headers:
        request_headers.update(headers)
    request = UrlRequest(
        url,
        method="POST",
        data=json.dumps(payload).encode("utf-8"),
        headers=request_headers,
    )
    with urlopen(request, timeout=max(0.1, float(timeout_sec))) as response:
        if response.status != 200:
            raise RuntimeError(f"internal post failed: status={response.status}, url={url}")
        raw = response.read()
        if not raw:
            return {}
        decoded = json.loads(raw.decode("utf-8"))
        if not isinstance(decoded, dict):
            return {}
        return decoded


# entry 切换窗口内，follower 收到 storage heartbeat 时优先转发给观测 leader。
def _forward_storage_heartbeat_to_leader(node_id: str, leader_id: str) -> Dict[str, str]:
    forward_url = f"{build_meta_base_url(leader_id)}/internal/storage_heartbeat"
    payload = {"node_id": node_id}
    response = _post_json(
        forward_url,
        payload,
        timeout_sec=META_INTERNAL_TIMEOUT_SEC,
        headers={"X-Meta-Forwarded-By": META_NODE_ID},
    )
    if str(response.get("status", "")).strip().lower() != "alive":
        raise RuntimeError(f"unexpected forward response from {leader_id}: {response}")
    return {
        "status": "alive",
        "node_id": node_id,
        "observed_at": str(response.get("observed_at", "")),
    }


# 本地兜底写入：在 leader 尚未稳定时也保持 storage 心跳时间推进，避免 UI alive/dead 抖动。
def _record_storage_heartbeat_local(node_id: str) -> Dict[str, str]:
    holder: Dict[str, str] = {"observed_at": ""}

    def _mutator(state: State) -> bool:
        # 先刷新超时状态，再写入本次心跳，确保时间语义稳定。
        changed = refresh_storage_membership(state)
        if mark_storage_heartbeat(state, node_id, min_interval_sec=HEARTBEAT_WRITE_MIN_INTERVAL_SEC):
            changed = True
        snapshot = get_membership_snapshot(state)
        holder["observed_at"] = str(snapshot.get(node_id, {}).get("last_heartbeat_at", ""))
        return changed

    mutate_state(_mutator)
    return {"status": "alive", "node_id": node_id, "observed_at": holder["observed_at"]}


# 处理 leader heartbeat 请求。
def process_internal_heartbeat(leader_id: str, leader_epoch: int, lamport: int) -> Dict[str, object]:
    result = record_leader_heartbeat(
        leader_id=leader_id,
        leader_epoch=leader_epoch,
        lamport=lamport,
    )
    return {
        "status": "alive",
        "follower_id": META_NODE_ID,
        "observed_at": str(result["observed_at"]),
        "role": str(result["role"]),
        "current_leader_id": str(result["leader_id"]),
        "leader_epoch": int(result["leader_epoch"]),
        "lamport": get_lamport_clock(),
    }


# 处理 replicate_state 请求。
def process_internal_replicate_state(snapshot: Dict[str, object]) -> Dict[str, object]:
    result = apply_replicated_state(snapshot)
    return {
        "status": str(result["status"]),
        "follower_id": META_NODE_ID,
        "applied_at": str(result["applied_at"]),
        "detail": str(result.get("detail", "")),
        "lamport": int(result.get("lamport", get_lamport_clock())),
    }


# 输出当前节点观测到的 leader 视图，供 entry 进行多节点收敛判断。
def process_internal_current_leader() -> Dict[str, object]:
    runtime = get_runtime_snapshot()
    return {
        "status": "ok",
        "node_id": str(runtime.get("node_id", META_NODE_ID)),
        "role": str(runtime.get("role", "follower")),
        "current_leader_id": str(runtime.get("current_leader_id", "")),
        "leader_epoch": int(runtime.get("leader_epoch", 0)),
        "current_term": int(runtime.get("current_term", 0)),
        "voted_for": str(runtime.get("voted_for", "")),
        "writable_leader": bool(is_writable_leader()),
        "lamport": int(runtime.get("lamport_clock", get_lamport_clock())),
        "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


# 处理 state_snapshot 请求（仅 writable leader 可导出）。
def process_internal_state_snapshot() -> Dict[str, object]:
    ensure_leader_write_api()
    return build_state_snapshot(reason="manual_snapshot")


# 处理 election 请求，并在必要时异步触发本地预抢占。
def process_internal_election(candidate_id: str, candidate_epoch: int, lamport: int, reason: str) -> Dict[str, object]:
    result = handle_incoming_election(
        candidate_id=candidate_id,
        candidate_epoch=candidate_epoch,
        lamport=lamport,
        reason=reason,
    )
    if bool(result.get("should_start_local_election", False)):
        trigger_takeover_async(reason=f"bully_preempt_from_{candidate_id}")
    return result


# 处理 quorum vote 请求。
def process_internal_vote(
    candidate_id: str,
    candidate_term: int,
    candidate_epoch: int,
    lamport: int,
    reason: str,
) -> Dict[str, object]:
    return handle_incoming_vote_request(
        candidate_id=candidate_id,
        candidate_term=candidate_term,
        candidate_epoch=candidate_epoch,
        lamport=lamport,
        reason=reason,
    )


# 处理 coordinator 请求。
def process_internal_coordinator(leader_id: str, leader_epoch: int, lamport: int, reason: str) -> Dict[str, object]:
    return handle_incoming_coordinator(
        leader_id=leader_id,
        leader_epoch=leader_epoch,
        lamport=lamport,
        reason=reason,
    )


# 处理 storage heartbeat 请求。
def process_storage_heartbeat(node_id: str, forwarded_by: str = "") -> Dict[str, str]:
    normalized_node_id = str(node_id or "").strip()
    if not normalized_node_id:
        raise HTTPException(status_code=400, detail="node_id is required")
    if normalized_node_id not in STORAGE_NODES:
        raise HTTPException(status_code=400, detail=f"unknown storage node: {normalized_node_id}")

    # 非 writable leader 时：先转发；若转发失败/无 leader 视图，则本地兜底写入。
    if not is_writable_leader():
        runtime = get_runtime_snapshot()
        normalized_forwarded_by = str(forwarded_by or "").strip().lower()
        observed_leader_id = str(runtime.get("current_leader_id", "")).strip().lower()
        if not normalized_forwarded_by and observed_leader_id and observed_leader_id != META_NODE_ID:
            try:
                return _forward_storage_heartbeat_to_leader(normalized_node_id, observed_leader_id)
            except (HTTPError, URLError, OSError, RuntimeError, TimeoutError, json.JSONDecodeError):
                return _record_storage_heartbeat_local(normalized_node_id)
        return _record_storage_heartbeat_local(normalized_node_id)

    return _record_storage_heartbeat_local(normalized_node_id)
