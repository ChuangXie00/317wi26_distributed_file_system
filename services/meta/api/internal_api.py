from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from core.config import HEARTBEAT_WRITE_MIN_INTERVAL_SEC, META_NODE_ID, ROLE, STORAGE_NODES
from core.replication import apply_replicated_state, build_state_snapshot, record_leader_heartbeat
from core.state import (
    State,
    get_membership_snapshot,
    mark_storage_heartbeat,
    mutate_state,
    refresh_storage_membership,
)

from .vo import (
    LeaderHeartbeatReq,
    LeaderHeartbeatResp,
    ReplicateStateReq,
    ReplicateStateResp,
    StorageHeartbeatReq,
    StorageHeartbeatResp,
)

router = APIRouter()


def _ensure_leader_write_api() -> None:
    # 0.1p04 follower 是 warm-standby，只读不处理写路径
    if ROLE != "leader":
        raise HTTPException(status_code=409, detail="meta follower is read-only in 0.1p04")


@router.post("/internal/heartbeat", response_model=LeaderHeartbeatResp)
def internal_heartbeat(req: LeaderHeartbeatReq) -> LeaderHeartbeatResp:
    if ROLE != "follower":
        raise HTTPException(status_code=409, detail="only follower accepts leader heartbeat")
    observed_at = record_leader_heartbeat(req.leader_id)
    return LeaderHeartbeatResp(status="alive", follower_id=META_NODE_ID, observed_at=observed_at)


@router.post("/internal/replicate_state", response_model=ReplicateStateResp)
def internal_replicate_state(req: ReplicateStateReq) -> ReplicateStateResp:
    if ROLE != "follower":
        raise HTTPException(status_code=409, detail="only follower accepts replicated state")
    result = apply_replicated_state(req.dict())
    return ReplicateStateResp(status="synced", follower_id=META_NODE_ID, applied_at=str(result["applied_at"]))


@router.get("/internal/state_snapshot")
def internal_state_snapshot() -> dict:
    if ROLE != "leader":
        raise HTTPException(status_code=409, detail="only leader serves state snapshot")
    return build_state_snapshot(reason="manual_snapshot")


@router.post("/internal/storage_heartbeat", response_model=StorageHeartbeatResp)
def storage_heartbeat(req: StorageHeartbeatReq) -> StorageHeartbeatResp:
    _ensure_leader_write_api()

    node_id = req.node_id.strip()
    if not node_id:
        raise HTTPException(status_code=400, detail="node_id is required")
    if node_id not in STORAGE_NODES:
        raise HTTPException(status_code=400, detail=f"unknown storage node: {node_id}")

    holder: Dict[str, str] = {"observed_at": ""}

    def _mutator(state: State) -> bool:
        changed = refresh_storage_membership(state)
        if mark_storage_heartbeat(state, node_id, min_interval_sec=HEARTBEAT_WRITE_MIN_INTERVAL_SEC):
            changed = True

        snapshot = get_membership_snapshot(state)
        holder["observed_at"] = str(snapshot.get(node_id, {}).get("last_heartbeat_at", ""))
        return changed

    mutate_state(_mutator)
    return StorageHeartbeatResp(status="alive", node_id=node_id, observed_at=holder["observed_at"])

