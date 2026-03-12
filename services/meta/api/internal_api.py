from typing import Dict

from fastapi import APIRouter, HTTPException

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

from .vo import (
    CoordinatorReq,
    CoordinatorResp,
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

router = APIRouter()


# 写接口统一门禁，保证只有可写 leader 能处理写请求。
def _ensure_leader_write_api() -> None:
    if not is_writable_leader():
        runtime = get_runtime_snapshot()
        raise HTTPException(
            status_code=409,
            detail=(
                "node is not writable leader: "
                f"role={runtime.get('role')}, "
                f"leader={runtime.get('current_leader_id')}, "
                f"epoch={runtime.get('leader_epoch')}"
            ),
        )


@router.post("/internal/heartbeat", response_model=LeaderHeartbeatResp)
def internal_heartbeat(req: LeaderHeartbeatReq) -> LeaderHeartbeatResp:
    # 任何节点都可接收 leader 心跳；若 epoch 更高会触发本地降级（fencing）。
    result = record_leader_heartbeat(
        leader_id=req.leader_id,
        leader_epoch=req.leader_epoch,
        lamport=req.lamport,
    )
    return LeaderHeartbeatResp(
        status="alive",
        follower_id=META_NODE_ID,
        observed_at=str(result["observed_at"]),
        role=str(result["role"]),
        current_leader_id=str(result["leader_id"]),
        leader_epoch=int(result["leader_epoch"]),
        lamport=get_lamport_clock(),
    )


@router.post("/internal/replicate_state", response_model=ReplicateStateResp)
def internal_replicate_state(req: ReplicateStateReq) -> ReplicateStateResp:
    # 复制接口支持在降级过程中接收新 leader 状态；旧 Lamport 消息会被忽略。
    result = apply_replicated_state(req.dict())
    return ReplicateStateResp(
        status=str(result["status"]),
        follower_id=META_NODE_ID,
        applied_at=str(result["applied_at"]),
        detail=str(result.get("detail", "")),
        lamport=int(result.get("lamport", get_lamport_clock())),
    )


@router.get("/internal/state_snapshot")
def internal_state_snapshot() -> dict:
    # 只允许可写 leader 导出快照，避免旧 leader 对外提供过期状态。
    _ensure_leader_write_api()
    return build_state_snapshot(reason="manual_snapshot")


@router.post("/internal/election", response_model=ElectionResp)
def internal_election(req: ElectionReq) -> ElectionResp:
    # 处理 election 请求，并按当前选主策略决定是否回 OK 与是否本地发起选举。
    result = handle_incoming_election(
        candidate_id=req.candidate_id,
        candidate_epoch=req.candidate_epoch,
        lamport=req.lamport,
        reason=req.reason,
    )
    if bool(result.get("should_start_local_election", False)):
        # 异步发起本地 election，避免阻塞当前内部请求。
        trigger_takeover_async(reason=f"bully_preempt_from_{req.candidate_id}")
    return ElectionResp(**result)


@router.post("/internal/vote", response_model=VoteResp)
def internal_vote(req: VoteReq) -> VoteResp:
    # 中文：处理 quorum 投票请求；当前提交先打通请求/响应协议，具体多数票选举在后续提交完善。
    result = handle_incoming_vote_request(
        candidate_id=req.candidate_id,
        candidate_term=req.candidate_term,
        candidate_epoch=req.candidate_epoch,
        lamport=req.lamport,
        reason=req.reason,
    )
    return VoteResp(**result)


@router.post("/internal/coordinator", response_model=CoordinatorResp)
def internal_coordinator(req: CoordinatorReq) -> CoordinatorResp:
    # 收到 coordinator 后更新 leader 视图；更高 epoch 会强制降级。
    result = handle_incoming_coordinator(
        leader_id=req.leader_id,
        leader_epoch=req.leader_epoch,
        lamport=req.lamport,
        reason=req.reason,
    )
    return CoordinatorResp(**result)


@router.post("/internal/storage_heartbeat", response_model=StorageHeartbeatResp)
def storage_heartbeat(req: StorageHeartbeatReq) -> StorageHeartbeatResp:
    _ensure_leader_write_api()

    node_id = req.node_id.strip()
    if not node_id:
        raise HTTPException(status_code=400, detail="node_id is required")
    if node_id not in STORAGE_NODES:
        raise HTTPException(status_code=400, detail=f"unknown storage node: {node_id}")

    # 通过 holder 暂存本次 heartbeat 的观测时间，供响应输出。
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
