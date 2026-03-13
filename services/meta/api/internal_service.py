from typing import Dict

from fastapi import HTTPException

from core.config import HEARTBEAT_WRITE_MIN_INTERVAL_SEC, META_NODE_ID, STORAGE_NODES
from core.election import handle_incoming_coordinator, handle_incoming_election, handle_incoming_vote_request
from core.replication import (
    apply_replicated_state,
    build_state_snapshot,
    record_leader_heartbeat,
    trigger_takeover_async,
)
from core.runtime import get_lamport_clock, get_runtime_snapshot, is_writable_leader
from core.state import (
    State,
    get_membership_snapshot,
    mark_storage_heartbeat,
    mutate_state,
    refresh_storage_membership,
)


# 写接口统一门禁，保证只有可写 leader 能处理写请求。
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


# 处理 leader 心跳请求，返回接口层需要的结构化响应字段。
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


# 处理 replicate_state 请求，输出接口层响应字段。
def process_internal_replicate_state(snapshot: Dict[str, object]) -> Dict[str, object]:
    result = apply_replicated_state(snapshot)
    return {
        "status": str(result["status"]),
        "follower_id": META_NODE_ID,
        "applied_at": str(result["applied_at"]),
        "detail": str(result.get("detail", "")),
        "lamport": int(result.get("lamport", get_lamport_clock())),
    }


# 处理 state_snapshot 请求（仅可写 leader 可导出）。
def process_internal_state_snapshot() -> Dict[str, object]:
    ensure_leader_write_api()
    return build_state_snapshot(reason="manual_snapshot")


# 处理 election 请求，并在需要时异步触发本地预抢占。
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
def process_storage_heartbeat(node_id: str) -> Dict[str, str]:
    ensure_leader_write_api()
    normalized_node_id = node_id.strip()
    if not normalized_node_id:
        raise HTTPException(status_code=400, detail="node_id is required")
    if normalized_node_id not in STORAGE_NODES:
        raise HTTPException(status_code=400, detail=f"unknown storage node: {normalized_node_id}")

    # 通过 holder 暂存本次 heartbeat 的观测时间，供响应输出。
    holder: Dict[str, str] = {"observed_at": ""}

    def _mutator(state: State) -> bool:
        changed = refresh_storage_membership(state)
        if mark_storage_heartbeat(state, normalized_node_id, min_interval_sec=HEARTBEAT_WRITE_MIN_INTERVAL_SEC):
            changed = True

        snapshot = get_membership_snapshot(state)
        holder["observed_at"] = str(snapshot.get(normalized_node_id, {}).get("last_heartbeat_at", ""))
        return changed

    mutate_state(_mutator)
    return {"status": "alive", "node_id": normalized_node_id, "observed_at": holder["observed_at"]}
