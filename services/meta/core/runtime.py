import copy
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .config import LEADER_ELECTION_MODE, META_BOOTSTRAP_ROLE, META_NODE_ID


# 运行时状态锁；保护 role/leader/epoch/lamport 的并发读写一致性。
_RUNTIME_LOCK = threading.RLock()


# 返回当前 UTC 时间的 ISO 字符串，统一调试字段格式。
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# 规范化角色值，避免非法字符串污染运行态。
def _normalize_role(raw_role: str) -> str:
    role = str(raw_role or "").strip().lower()
    if role in {"leader", "follower", "candidate"}:
        return role
    return "follower"


# 初始化角色；仅用于进程启动 bootstrap，后续以运行时变更为准。
_INITIAL_ROLE = _normalize_role(META_BOOTSTRAP_ROLE)
# 初始化 leader 与 epoch，leader 默认以 epoch=1 启动。
_INITIAL_LEADER_ID = META_NODE_ID if _INITIAL_ROLE == "leader" else ""
_INITIAL_EPOCH = 1 if _INITIAL_ROLE == "leader" else 0


# 统一保存 Phase5 关键运行时字段，供 fencing/debug/election 共用。
_RUNTIME_STATE: Dict[str, Any] = {
    "node_id": META_NODE_ID,
    "election_mode": LEADER_ELECTION_MODE,
    "role": _INITIAL_ROLE,
    "current_leader_id": _INITIAL_LEADER_ID,
    "leader_epoch": _INITIAL_EPOCH,
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
    # 记录最近一次“我让位给更高优先级节点”的观测信息，便于排查三节点选举抖动。
    "last_election_deferred_at": "",
    "last_election_deferred_reason": "",
    "last_election_deferred_epoch": 0,
    "last_election_deferred_to": [],
}


# 输出运行态快照，供 API/debug 使用。
def get_runtime_snapshot() -> Dict[str, Any]:
    with _RUNTIME_LOCK:
        return copy.deepcopy(_RUNTIME_STATE)


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
        lamport = tick_lamport(event="rejoin_as_follower")
        return {
            "role": str(_RUNTIME_STATE.get("role", "follower")),
            "leader_id": str(_RUNTIME_STATE.get("current_leader_id", "")),
            "leader_epoch": int(_RUNTIME_STATE.get("leader_epoch", 0)),
            "lamport": lamport,
            "reason": str(reason),
        }
