import copy
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .config import LEADER_ELECTION_MODE, META_BOOTSTRAP_ROLE, META_NODE_ID, META_REJOIN_ELECTION_HOLDOFF_SEC


# 运行时状态锁；保护 role/leader/epoch/lamport 的并发读写一致性。
_RUNTIME_LOCK = threading.RLock()


# 返回当前 UTC 时间的 ISO 字符串，统一调试字段格式。
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# 中文：将 Unix 时间戳转成 UTC ISO 字符串，便于 debug 观察冷却结束时间点。
def _iso_from_ts(ts: float) -> str:
    normalized_ts = float(ts)
    if normalized_ts <= 0:
        return ""
    return datetime.fromtimestamp(normalized_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


# 规范化角色值，避免非法字符串污染运行态。
def _normalize_role(raw_role: str) -> str:
    role = str(raw_role or "").strip().lower()
    if role in {"leader", "follower", "candidate"}:
        return role
    return "follower"


# 中文：规范化节点 ID，用于投票记录与 leader 标识字段统一格式。
def _normalize_node_id(raw_node_id: str) -> str:
    return str(raw_node_id or "").strip().lower()


# 初始化角色；仅用于进程启动 bootstrap，后续以运行时变更为准。
_INITIAL_ROLE = _normalize_role(META_BOOTSTRAP_ROLE)
# 初始化 leader 与 epoch，leader 默认以 epoch=1 启动。
_INITIAL_LEADER_ID = META_NODE_ID if _INITIAL_ROLE == "leader" else ""
_INITIAL_EPOCH = 1 if _INITIAL_ROLE == "leader" else 0
# 中文：在 quorum 尚未接入前，term 默认跟随 epoch 初始化，保持语义兼容。
_INITIAL_TERM = _INITIAL_EPOCH


# 统一保存 Phase5 关键运行时字段，供 fencing/debug/election 共用。
_RUNTIME_STATE: Dict[str, Any] = {
    "node_id": META_NODE_ID,
    "election_mode": LEADER_ELECTION_MODE,
    "role": _INITIAL_ROLE,
    "current_leader_id": _INITIAL_LEADER_ID,
    "leader_epoch": _INITIAL_EPOCH,
    # 中文：quorum 任期号；当前阶段先与 leader_epoch 对齐，后续由 quorum 投票链路驱动。
    "current_term": _INITIAL_TERM,
    # 中文：当前任期内本节点已投票对象（空字符串表示尚未投票）。
    "voted_for": "",
    "lamport_clock": 0,
    "last_lamport_event": "",
    "last_lamport_at": "",
    "last_applied_lamport": 0,
    "last_applied_lamport_at": "",
    "last_applied_lamport_reason": "",
    "last_role_change_at": _now_iso(),
    "last_role_change_reason": "bootstrap",
    "last_epoch_change_at": _now_iso(),
    "last_epoch_change_reason": "bootstrap",
    # 中文：term 变更审计信息，便于追踪任期推进来源。
    "last_term_change_at": _now_iso(),
    "last_term_change_reason": "bootstrap",
    # 中文：投票变更审计信息，便于排查 quorum 投票行为。
    "last_vote_change_at": "",
    "last_vote_change_reason": "",
    # 记录最近一次“我让位给更高优先级节点”的观测信息，便于排查三节点选举抖动。
    "last_election_deferred_at": "",
    "last_election_deferred_reason": "",
    "last_election_deferred_epoch": 0,
    "last_election_deferred_to": [],
    # 中文：重入 follower 后的选举冷却窗口，避免恢复节点立刻抢主。
    "last_rejoin_as_follower_at": "",
    "last_rejoin_as_follower_reason": "",
    "rejoin_election_holdoff_until_ts": 0.0,
    "rejoin_election_holdoff_until": "",
}


# 输出运行态快照，供 API/debug 使用。
def get_runtime_snapshot() -> Dict[str, Any]:
    with _RUNTIME_LOCK:
        return copy.deepcopy(_RUNTIME_STATE)


# 中文：读取重入冷却状态；active=true 时本节点不应主动发起本地选举。
def get_rejoin_election_holdoff(now_ts: Optional[float] = None) -> Dict[str, Any]:
    check_ts = time.time() if now_ts is None else float(now_ts)
    with _RUNTIME_LOCK:
        holdoff_until_ts = float(_RUNTIME_STATE.get("rejoin_election_holdoff_until_ts", 0.0) or 0.0)
        holdoff_until = str(_RUNTIME_STATE.get("rejoin_election_holdoff_until", ""))
        source_reason = str(_RUNTIME_STATE.get("last_rejoin_as_follower_reason", ""))

    remaining_sec = max(0.0, holdoff_until_ts - check_ts)
    return {
        "active": remaining_sec > 0,
        "remaining_sec": round(remaining_sec, 3),
        "until_ts": holdoff_until_ts,
        "until": holdoff_until,
        "source_reason": source_reason,
    }


# 获取当前角色（leader/follower/candidate）。
def get_node_role() -> str:
    with _RUNTIME_LOCK:
        return str(_RUNTIME_STATE["role"])


# 获取当前已知 leader 节点 ID。
def get_current_leader_id() -> str:
    with _RUNTIME_LOCK:
        return str(_RUNTIME_STATE.get("current_leader_id", ""))


# 获取当前 leader epoch（fencing 主键）。
def get_leader_epoch() -> int:
    with _RUNTIME_LOCK:
        return int(_RUNTIME_STATE.get("leader_epoch", 0))


# 中文：读取当前任期（term），供 quorum 选主链路使用。
def get_current_term() -> int:
    with _RUNTIME_LOCK:
        return int(_RUNTIME_STATE.get("current_term", 0))


# 中文：读取当前任期内的投票对象；空字符串表示尚未投票。
def get_voted_for() -> str:
    with _RUNTIME_LOCK:
        return str(_RUNTIME_STATE.get("voted_for", ""))


# 判断当前节点是否可处理写请求（仅“我就是 leader”才放行）。
def is_writable_leader() -> bool:
    with _RUNTIME_LOCK:
        return (
            str(_RUNTIME_STATE.get("role", "")) == "leader"
            and str(_RUNTIME_STATE.get("current_leader_id", "")) == META_NODE_ID
        )


# 在锁内更新角色并记录审计信息。
def _set_role_unlocked(new_role: str, reason: str) -> bool:
    role = _normalize_role(new_role)
    if _RUNTIME_STATE["role"] == role:
        return False
    _RUNTIME_STATE["role"] = role
    _RUNTIME_STATE["last_role_change_at"] = _now_iso()
    _RUNTIME_STATE["last_role_change_reason"] = str(reason)
    return True


# 在锁内更新 epoch 并记录变更原因。
def _set_epoch_unlocked(epoch: int, reason: str) -> bool:
    normalized = max(0, int(epoch))
    if int(_RUNTIME_STATE["leader_epoch"]) == normalized:
        return False
    _RUNTIME_STATE["leader_epoch"] = normalized
    _RUNTIME_STATE["last_epoch_change_at"] = _now_iso()
    _RUNTIME_STATE["last_epoch_change_reason"] = str(reason)
    # 中文：在 Bully/旧路径下保持 term 与 epoch 同步，避免双时间轴漂移。
    _set_term_unlocked(normalized, reason=f"sync_with_epoch:{reason}")
    return True


# 中文：在锁内更新 term；仅允许非递减推进，防止旧任期回退覆盖。
def _set_term_unlocked(term: int, reason: str, *, allow_same: bool = True) -> bool:
    normalized = max(0, int(term))
    current_term = int(_RUNTIME_STATE.get("current_term", 0))
    if normalized < current_term:
        return False
    if normalized == current_term and not allow_same:
        return False
    if normalized == current_term and allow_same:
        return True
    _RUNTIME_STATE["current_term"] = normalized
    _RUNTIME_STATE["last_term_change_at"] = _now_iso()
    _RUNTIME_STATE["last_term_change_reason"] = str(reason)
    return True


# 中文：在锁内记录本任期投票对象；用于 quorum 的“一任期一票”约束。
def _set_voted_for_unlocked(voted_for: str, reason: str) -> bool:
    normalized_voted_for = _normalize_node_id(voted_for)
    if str(_RUNTIME_STATE.get("voted_for", "")) == normalized_voted_for:
        return False
    _RUNTIME_STATE["voted_for"] = normalized_voted_for
    _RUNTIME_STATE["last_vote_change_at"] = _now_iso()
    _RUNTIME_STATE["last_vote_change_reason"] = str(reason)
    return True


# 中文：本地观测到更高 term 时统一执行降级逻辑，避免旧 leader/candidate 继续参与写入或选举。
def _step_down_on_higher_term_unlocked(term: int, reason: str, *, sync_epoch: bool = True, reset_voted_for: bool = True) -> bool:
    normalized_term = max(0, int(term))
    current_term = int(_RUNTIME_STATE.get("current_term", 0))
    if normalized_term <= current_term:
        return False

    _set_term_unlocked(normalized_term, reason=f"higher_term:{reason}", allow_same=False)
    if sync_epoch and normalized_term > int(_RUNTIME_STATE.get("leader_epoch", 0)):
        _set_epoch_unlocked(normalized_term, reason=f"higher_term_sync_epoch:{reason}")

    # 中文：进入新 term 后清空旧 leader 视图，等待后续 heartbeat/coordinator 收敛新的 leader。
    _RUNTIME_STATE["current_leader_id"] = ""
    _set_role_unlocked("follower", reason=f"higher_term_step_down:{reason}")
    if reset_voted_for:
        _set_voted_for_unlocked("", reason=f"higher_term_reset_vote:{reason}")
    return True


# 推进 Lamport 逻辑时钟；收到远端时钟时先做 max，再 +1。
def tick_lamport(event: str, incoming_lamport: Optional[int] = None) -> int:
    with _RUNTIME_LOCK:
        current = int(_RUNTIME_STATE.get("lamport_clock", 0))
        if incoming_lamport is not None:
            current = max(current, max(0, int(incoming_lamport)))
        current += 1
        _RUNTIME_STATE["lamport_clock"] = current
        _RUNTIME_STATE["last_lamport_event"] = str(event)
        _RUNTIME_STATE["last_lamport_at"] = _now_iso()
        return current


# 读取当前 Lamport 值。
def get_lamport_clock() -> int:
    with _RUNTIME_LOCK:
        return int(_RUNTIME_STATE.get("lamport_clock", 0))


# 读取 follower 最近应用的 Lamport（用于拒绝旧复制消息）。
def get_last_applied_lamport() -> int:
    with _RUNTIME_LOCK:
        return int(_RUNTIME_STATE.get("last_applied_lamport", 0))


# 记录最新已应用 Lamport，保证单调递增。
def mark_last_applied_lamport(lamport: int, reason: str) -> bool:
    normalized = max(0, int(lamport))
    with _RUNTIME_LOCK:
        current = int(_RUNTIME_STATE.get("last_applied_lamport", 0))
        if normalized <= current:
            return False
        _RUNTIME_STATE["last_applied_lamport"] = normalized
        _RUNTIME_STATE["last_applied_lamport_at"] = _now_iso()
        _RUNTIME_STATE["last_applied_lamport_reason"] = str(reason)
        return True


# 中文：观测到更高任期时推进 term，并按需同步 epoch（用于后续 quorum 入站处理）。
def observe_term(term: int, reason: str, *, sync_epoch: bool = True, reset_voted_for: bool = True) -> Dict[str, Any]:
    normalized_term = max(0, int(term))
    with _RUNTIME_LOCK:
        before_term = int(_RUNTIME_STATE.get("current_term", 0))
        changed = False
        ignored = False
        stepped_down = False

        if normalized_term < before_term:
            ignored = True
        else:
            if _step_down_on_higher_term_unlocked(
                term=normalized_term,
                reason=f"observe_term:{reason}",
                sync_epoch=sync_epoch,
                reset_voted_for=reset_voted_for,
            ):
                changed = True
                stepped_down = True

        return {
            "changed": changed,
            "ignored": ignored,
            "stepped_down": stepped_down,
            "role": str(_RUNTIME_STATE.get("role", "follower")),
            "leader_id": str(_RUNTIME_STATE.get("current_leader_id", "")),
            "term": int(_RUNTIME_STATE.get("current_term", 0)),
            "leader_epoch": int(_RUNTIME_STATE.get("leader_epoch", 0)),
            "voted_for": str(_RUNTIME_STATE.get("voted_for", "")),
        }


# 中文：记录当前任期的投票对象；允许传空字符串清票。
def mark_voted_for(voted_for: str, reason: str, term: Optional[int] = None) -> Dict[str, Any]:
    normalized_voted_for = _normalize_node_id(voted_for)
    with _RUNTIME_LOCK:
        requested_term = None if term is None else max(0, int(term))
        current_term = int(_RUNTIME_STATE.get("current_term", 0))
        stale_term = False
        term_changed = False
        stepped_down = False

        if requested_term is not None:
            if requested_term < current_term:
                stale_term = True
            elif requested_term > current_term:
                if _step_down_on_higher_term_unlocked(
                    term=requested_term,
                    reason=f"vote_term:{reason}",
                    sync_epoch=True,
                    reset_voted_for=True,
                ):
                    term_changed = True
                    stepped_down = True

        vote_changed = False
        if not stale_term:
            vote_changed = _set_voted_for_unlocked(normalized_voted_for, reason=f"mark_voted_for:{reason}")

        return {
            "stale_term": stale_term,
            "term_changed": term_changed,
            "stepped_down": stepped_down,
            "vote_changed": vote_changed,
            "role": str(_RUNTIME_STATE.get("role", "follower")),
            "leader_id": str(_RUNTIME_STATE.get("current_leader_id", "")),
            "term": int(_RUNTIME_STATE.get("current_term", 0)),
            "leader_epoch": int(_RUNTIME_STATE.get("leader_epoch", 0)),
            "voted_for": str(_RUNTIME_STATE.get("voted_for", "")),
        }


# 进入 candidate 轮次，epoch 自增并清空当前 leader。
def begin_election_round(reason: str) -> Dict[str, Any]:
    with _RUNTIME_LOCK:
        next_epoch = int(_RUNTIME_STATE.get("leader_epoch", 0)) + 1
        _set_epoch_unlocked(next_epoch, reason=f"begin_election:{reason}")
        _RUNTIME_STATE["current_leader_id"] = ""
        _set_role_unlocked("candidate", reason=f"begin_election:{reason}")
        lamport = tick_lamport(event="begin_election")
        return {
            "epoch": next_epoch,
            "lamport": lamport,
            "role": str(_RUNTIME_STATE["role"]),
        }


# 标记本轮选举被更高优先级节点接管，本节点退回 follower 等待 coordinator 收敛。
def mark_election_deferred(epoch: int, reason: str, defer_to_nodes: Optional[List[str]] = None) -> Dict[str, Any]:
    normalized_epoch = max(0, int(epoch))
    normalized_defer_to = [str(node_id).strip() for node_id in (defer_to_nodes or []) if str(node_id).strip()]

    with _RUNTIME_LOCK:
        # 让位并不降低 epoch；仅在必要时抬升到本轮候选 epoch。
        if normalized_epoch > int(_RUNTIME_STATE.get("leader_epoch", 0)):
            _set_epoch_unlocked(normalized_epoch, reason=f"election_deferred:{reason}")

        _set_role_unlocked("follower", reason=f"election_deferred:{reason}")
        _RUNTIME_STATE["last_election_deferred_at"] = _now_iso()
        _RUNTIME_STATE["last_election_deferred_reason"] = str(reason)
        _RUNTIME_STATE["last_election_deferred_epoch"] = int(_RUNTIME_STATE.get("leader_epoch", 0))
        _RUNTIME_STATE["last_election_deferred_to"] = normalized_defer_to
        lamport = tick_lamport(event="election_deferred")
        return {
            "role": str(_RUNTIME_STATE.get("role", "follower")),
            "leader_id": str(_RUNTIME_STATE.get("current_leader_id", "")),
            "leader_epoch": int(_RUNTIME_STATE.get("leader_epoch", 0)),
            "lamport": lamport,
            "defer_to_nodes": normalized_defer_to,
        }


# 本节点当选 leader 后更新运行态。
def promote_self_to_leader(epoch: int, reason: str) -> Dict[str, Any]:
    with _RUNTIME_LOCK:
        normalized_epoch = max(int(_RUNTIME_STATE.get("leader_epoch", 0)), max(0, int(epoch)))
        _set_epoch_unlocked(normalized_epoch, reason=f"promote_leader:{reason}")
        _RUNTIME_STATE["current_leader_id"] = META_NODE_ID
        # 中文：当选 leader 后清空重入冷却标记，避免 debug 误导。
        _RUNTIME_STATE["rejoin_election_holdoff_until_ts"] = 0.0
        _RUNTIME_STATE["rejoin_election_holdoff_until"] = ""
        _set_role_unlocked("leader", reason=f"promote_leader:{reason}")
        lamport = tick_lamport(event="promote_leader")
        return {
            "node_id": META_NODE_ID,
            "role": "leader",
            "leader_id": META_NODE_ID,
            "leader_epoch": normalized_epoch,
            "lamport": lamport,
        }


# 收到更高 epoch 的 election 消息时先降级并刷新 epoch，避免旧主继续写入。
def observe_candidate_epoch(candidate_epoch: int, reason: str) -> Dict[str, Any]:
    with _RUNTIME_LOCK:
        incoming = max(0, int(candidate_epoch))
        current = int(_RUNTIME_STATE.get("leader_epoch", 0))
        changed = False
        if incoming > current:
            changed = True
            _set_epoch_unlocked(incoming, reason=f"candidate_epoch:{reason}")
            _RUNTIME_STATE["current_leader_id"] = ""
            _set_role_unlocked("follower", reason=f"candidate_epoch:{reason}")
        return {
            "changed": changed,
            "leader_epoch": int(_RUNTIME_STATE.get("leader_epoch", 0)),
            "role": str(_RUNTIME_STATE.get("role", "follower")),
        }


# 观测 leader 信息（heartbeat/coordinator），并执行 fencing 降级规则。
def observe_leader(leader_id: str, leader_epoch: int, reason: str) -> Dict[str, Any]:
    normalized_leader = str(leader_id or "").strip()
    incoming_epoch = max(0, int(leader_epoch))

    with _RUNTIME_LOCK:
        current_epoch = int(_RUNTIME_STATE.get("leader_epoch", 0))
        current_leader = str(_RUNTIME_STATE.get("current_leader_id", ""))
        changed = False
        ignored = False

        if incoming_epoch < current_epoch:
            ignored = True
        else:
            # 同 epoch 冲突时采用确定性规则（字典序较大 leader 胜出）避免双主。
            if incoming_epoch == current_epoch and current_leader and normalized_leader and current_leader != normalized_leader:
                normalized_leader = max(current_leader, normalized_leader)

            if incoming_epoch > current_epoch:
                if _set_epoch_unlocked(incoming_epoch, reason=f"observe_leader:{reason}"):
                    changed = True

            if normalized_leader and normalized_leader != current_leader:
                _RUNTIME_STATE["current_leader_id"] = normalized_leader
                changed = True

            desired_role = "leader" if _RUNTIME_STATE.get("current_leader_id") == META_NODE_ID else "follower"
            if _set_role_unlocked(desired_role, reason=f"observe_leader:{reason}"):
                changed = True

        return {
            "changed": changed,
            "ignored": ignored,
            "role": str(_RUNTIME_STATE.get("role", "follower")),
            "leader_id": str(_RUNTIME_STATE.get("current_leader_id", "")),
            "leader_epoch": int(_RUNTIME_STATE.get("leader_epoch", 0)),
        }


# 启动重入时强制回归 follower，避免恢复节点按旧 bootstrap 角色立即抢主。
def force_rejoin_as_follower(reason: str, observed_leader_id: str = "", observed_leader_epoch: Optional[int] = None) -> Dict[str, Any]:
    normalized_leader_id = str(observed_leader_id or "").strip().lower()
    normalized_epoch = None if observed_leader_epoch is None else max(0, int(observed_leader_epoch))

    with _RUNTIME_LOCK:
        # 若探测到更高/更新任期，优先抬升 epoch，防止旧任期继续生效。
        if normalized_epoch is not None and normalized_epoch > int(_RUNTIME_STATE.get("leader_epoch", 0)):
            _set_epoch_unlocked(normalized_epoch, reason=f"rejoin_as_follower:{reason}")

        # 若已探测到 leader，写入 leader 视图；否则清空“我是 leader”的自认定。
        if normalized_leader_id:
            _RUNTIME_STATE["current_leader_id"] = normalized_leader_id
        elif str(_RUNTIME_STATE.get("current_leader_id", "")) == META_NODE_ID:
            _RUNTIME_STATE["current_leader_id"] = ""

        _set_role_unlocked("follower", reason=f"rejoin_as_follower:{reason}")
        # 中文：重入后开启选举冷却，优先等待现任 leader 的心跳/协调收敛。
        holdoff_sec = max(0.0, float(META_REJOIN_ELECTION_HOLDOFF_SEC))
        holdoff_until_ts = time.time() + holdoff_sec if holdoff_sec > 0 else 0.0
        _RUNTIME_STATE["last_rejoin_as_follower_at"] = _now_iso()
        _RUNTIME_STATE["last_rejoin_as_follower_reason"] = str(reason)
        _RUNTIME_STATE["rejoin_election_holdoff_until_ts"] = holdoff_until_ts
        _RUNTIME_STATE["rejoin_election_holdoff_until"] = _iso_from_ts(holdoff_until_ts)
        lamport = tick_lamport(event="rejoin_as_follower")
        return {
            "role": str(_RUNTIME_STATE.get("role", "follower")),
            "leader_id": str(_RUNTIME_STATE.get("current_leader_id", "")),
            "leader_epoch": int(_RUNTIME_STATE.get("leader_epoch", 0)),
            "lamport": lamport,
            "reason": str(reason),
            "rejoin_holdoff_sec": holdoff_sec,
            "rejoin_holdoff_until": str(_RUNTIME_STATE.get("rejoin_election_holdoff_until", "")),
        }
