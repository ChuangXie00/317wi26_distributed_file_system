import time
from typing import Any, Dict, List, Optional

from ..config import META_NODE_ID, META_REJOIN_ELECTION_HOLDOFF_SEC
from .state_machine import (
    _RUNTIME_LOCK,
    _RUNTIME_STATE,
    _iso_from_ts,
    _now_iso,
    _set_epoch_unlocked,
    _set_role_unlocked,
    tick_lamport,
)


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
        # 当选 leader 后清空重入冷却标记，避免 debug 误导。
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
        # 重入后开启选举冷却，优先等待现任 leader 的心跳/协调收敛。
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
