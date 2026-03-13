import copy
import threading
import time
from typing import Any, Dict

from ..config import LEADER_ELECTION_MODE, META_CLUSTER_NODES, META_LEADER_HEARTBEAT_TIMEOUT_SEC, META_NODE_ID
from ..election import trigger_election
from ..runtime import get_rejoin_election_holdoff, get_runtime_snapshot, is_writable_leader
from .heartbeat_sync import push_state_to_followers
from .state_store import now_iso, read_runtime, record_error, update_runtime


# 选主互斥锁，避免并发触发多轮 election。
_ELECTION_LOCK = threading.Lock()


# quorum 模式下按节点顺序施加确定性超时偏移，打破同周期 timeout 导致的重复平票/抖动。
def _quorum_timeout_offset_sec() -> float:
    if LEADER_ELECTION_MODE != "quorum":
        return 0.0

    normalized_nodes = sorted({str(node_id).strip().lower() for node_id in META_CLUSTER_NODES if str(node_id).strip()})
    if not normalized_nodes:
        return 0.0

    try:
        node_index = normalized_nodes.index(META_NODE_ID)
    except ValueError:
        node_index = 0

    # 按 3.5s 级差错开，确保在 3s 轮询粒度下进入不同选举窗口。
    return float(node_index) * 3.5


# 纯函数评估当前是否应该触发 timeout takeover，便于后续单测覆盖调度策略。
def should_takeover_by_timeout(
    *,
    role: str,
    elapsed_sec: float,
    effective_timeout_sec: float,
    holdoff_active: bool,
    since_last_takeover_sec: float,
) -> Dict[str, Any]:
    if role == "leader":
        return {"should_takeover": False, "reason": "local_is_leader"}
    if elapsed_sec <= effective_timeout_sec:
        return {"should_takeover": False, "reason": "heartbeat_not_timeout"}
    if holdoff_active:
        return {"should_takeover": False, "reason": "rejoin_holdoff_active"}
    if since_last_takeover_sec >= 0 and since_last_takeover_sec < 1.5:
        return {"should_takeover": False, "reason": "takeover_cooldown"}
    return {
        "should_takeover": True,
        "reason": "leader_timeout",
        "detail": f"leader_timeout_{round(elapsed_sec, 3)}s_threshold_{round(effective_timeout_sec, 3)}s",
    }


# 执行一次 takeover 流程（超时触发或内部预抢占触发）。
def trigger_takeover(reason: str) -> Dict[str, Any]:
    # 重入冷却期内拒绝本地主动选举，避免恢复节点立即抢主。
    rejoin_holdoff = get_rejoin_election_holdoff()
    if bool(rejoin_holdoff.get("active", False)):
        skipped_result = {
            "status": "skipped",
            "reason": "rejoin_election_holdoff",
            "takeover_reason": str(reason),
            "holdoff_remaining_sec": float(rejoin_holdoff.get("remaining_sec", 0.0)),
            "holdoff_until": str(rejoin_holdoff.get("until", "")),
            "holdoff_source_reason": str(rejoin_holdoff.get("source_reason", "")),
        }
        update_runtime(
            last_takeover_at=now_iso(),
            last_takeover_reason=reason,
            last_takeover_result="skipped_rejoin_holdoff",
            last_takeover_ts=time.time(),
            last_takeover_detail=copy.deepcopy(skipped_result),
        )
        return skipped_result

    acquired = _ELECTION_LOCK.acquire(blocking=False)
    if not acquired:
        return {"status": "skipped", "reason": "election already in progress"}

    update_runtime(election_in_progress=True)
    try:
        if is_writable_leader():
            return {"status": "skipped", "reason": "node is already writable leader"}

        result = trigger_election(reason=reason)
        update_runtime(
            last_takeover_at=now_iso(),
            last_takeover_reason=reason,
            last_takeover_result=str(result.get("status", "unknown")),
            last_takeover_ts=time.time(),
            last_takeover_detail=copy.deepcopy(result),
        )

        # 当选后立即推送一次状态，尽快收敛 follower 视图。
        if is_writable_leader():
            push_state_to_followers(reason="takeover_elected")
        return result
    except Exception as exc:  # pragma: no cover - 防御性兜底
        record_error(f"trigger takeover failed: {exc}")
        return {"status": "error", "reason": reason, "error": str(exc)}
    finally:
        update_runtime(election_in_progress=False)
        _ELECTION_LOCK.release()


# 异步触发 takeover，避免在 API 线程内阻塞。
def trigger_takeover_async(reason: str) -> None:
    def _runner() -> None:
        trigger_takeover(reason=reason)

    thread = threading.Thread(target=_runner, name="meta-takeover", daemon=True)
    thread.start()


# follower/candidate 周期检查 leader 心跳超时，超时后触发 election。
def _maybe_takeover_by_timeout() -> None:
    runtime_snapshot = get_runtime_snapshot()
    role = str(runtime_snapshot.get("role", "follower"))

    runtime_data = read_runtime()
    last_ts = float(runtime_data.get("last_leader_heartbeat_ts", 0.0) or 0.0)
    started_ts = float(runtime_data.get("started_ts", time.time()))
    last_takeover_ts = float(runtime_data.get("last_takeover_ts", 0.0) or 0.0)

    reference_ts = last_ts if last_ts > 0 else started_ts
    elapsed = max(0.0, time.time() - reference_ts)
    # quorum 模式使用“基础超时 + 节点偏移”作为生效阈值，避免多节点同拍触发选举。
    effective_timeout_sec = float(META_LEADER_HEARTBEAT_TIMEOUT_SEC + _quorum_timeout_offset_sec())

    # 重入冷却期内不执行 timeout takeover，优先等待 leader 心跳恢复。
    rejoin_holdoff = get_rejoin_election_holdoff()
    holdoff_active = bool(rejoin_holdoff.get("active", False))
    since_last_takeover_sec = -1.0 if last_takeover_ts <= 0 else max(0.0, time.time() - last_takeover_ts)

    decision = should_takeover_by_timeout(
        role=role,
        elapsed_sec=elapsed,
        effective_timeout_sec=effective_timeout_sec,
        holdoff_active=holdoff_active,
        since_last_takeover_sec=since_last_takeover_sec,
    )
    if not bool(decision.get("should_takeover", False)):
        return

    trigger_takeover(reason=str(decision.get("detail", "leader_timeout")))
