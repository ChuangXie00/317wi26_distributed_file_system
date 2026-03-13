from fastapi import APIRouter, Header

from core.config import META_NODE_ID

from .vo import (
    CoordinatorReq,
    CoordinatorResp,
    CurrentLeaderResp,
    ElectionReq,
    ElectionResp,
    LeaderHeartbeatReq,
    LeaderHeartbeatResp,
    ReplicateStateReq,
    ReplicateStateResp,
    StorageHeartbeatReq,
    StorageHeartbeatResp,
    VoteReq,
    VoteResp,
)
from .internal_service import (
    process_internal_coordinator,
    process_internal_current_leader,
    process_internal_election,
    process_internal_heartbeat,
    process_internal_replicate_state,
    process_internal_state_snapshot,
    process_internal_vote,
    process_storage_heartbeat,
)

router = APIRouter()


@router.post("/internal/heartbeat", response_model=LeaderHeartbeatResp)
def internal_heartbeat(req: LeaderHeartbeatReq) -> LeaderHeartbeatResp:
    result = process_internal_heartbeat(
        leader_id=req.leader_id,
        leader_epoch=req.leader_epoch,
        lamport=req.lamport,
    )
    return LeaderHeartbeatResp(**result)


@router.post("/internal/replicate_state", response_model=ReplicateStateResp)
def internal_replicate_state(req: ReplicateStateReq) -> ReplicateStateResp:
    result = process_internal_replicate_state(req.dict())
    return ReplicateStateResp(**result)


@router.get("/internal/state_snapshot")
def internal_state_snapshot() -> dict:
    return process_internal_state_snapshot()


@router.get("/internal/current_leader", response_model=CurrentLeaderResp)
def internal_current_leader() -> CurrentLeaderResp:
    # entry 通过该接口轮询集群各节点，收敛“当前 leader”后再更新转发目标。
    result = process_internal_current_leader()
    return CurrentLeaderResp(**result)


@router.post("/internal/election", response_model=ElectionResp)
def internal_election(req: ElectionReq) -> ElectionResp:
    result = process_internal_election(
        candidate_id=req.candidate_id,
        candidate_epoch=req.candidate_epoch,
        lamport=req.lamport,
        reason=req.reason,
    )
    return ElectionResp(**result)


@router.post("/internal/vote", response_model=VoteResp)
def internal_vote(req: VoteReq) -> VoteResp:
    result = process_internal_vote(
        candidate_id=req.candidate_id,
        candidate_term=req.candidate_term,
        candidate_epoch=req.candidate_epoch,
        lamport=req.lamport,
        reason=req.reason,
    )
    return VoteResp(**result)


@router.post("/internal/coordinator", response_model=CoordinatorResp)
def internal_coordinator(req: CoordinatorReq) -> CoordinatorResp:
    result = process_internal_coordinator(
        leader_id=req.leader_id,
        leader_epoch=req.leader_epoch,
        lamport=req.lamport,
        reason=req.reason,
    )
    return CoordinatorResp(**result)


@router.post("/internal/storage_heartbeat", response_model=StorageHeartbeatResp)
def storage_heartbeat(
    req: StorageHeartbeatReq,
    x_meta_forwarded_by: str | None = Header(default=None, alias="X-Meta-Forwarded-By"),
) -> StorageHeartbeatResp:
    # follower 转发链路会带 X-Meta-Forwarded-By，避免重复转发形成环路。
    result = process_storage_heartbeat(req.node_id, forwarded_by=str(x_meta_forwarded_by or ""))
    return StorageHeartbeatResp(**result)
