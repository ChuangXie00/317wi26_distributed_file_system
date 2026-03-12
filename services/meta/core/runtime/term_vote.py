from typing import Any, Dict, Optional

from .state_machine import (
    _RUNTIME_LOCK,
    _RUNTIME_STATE,
    _normalize_node_id,
    _set_epoch_unlocked,
    _set_role_unlocked,
    _set_term_unlocked,
    _set_voted_for_unlocked,
)


# 中文：本地观测到更高 term 时统一执行降级逻辑，避免旧 leader/candidate 继续参与写入或选举。
def step_down_to_follower(term: int, reason: str, *, sync_epoch: bool = True, reset_voted_for: bool = True) -> bool:
    with _RUNTIME_LOCK:
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


# 中文：显式推进 term 并按需降级，供 quorum 入站/对账链路复用。
def advance_term(term: int, reason: str, *, sync_epoch: bool = True, reset_voted_for: bool = True) -> Dict[str, Any]:
    normalized_term = max(0, int(term))
    with _RUNTIME_LOCK:
        before_term = int(_RUNTIME_STATE.get("current_term", 0))
        changed = False
        ignored = False
        stepped_down = False

        if normalized_term < before_term:
            ignored = True
        else:
            if step_down_to_follower(
                term=normalized_term,
                reason=reason,
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


# 中文：显式记录任期投票对象，统一处理 stale-term 与投票落账。
def record_vote(voted_for: str, reason: str, term: Optional[int] = None) -> Dict[str, Any]:
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
                if step_down_to_follower(
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


# 中文：观测到更高任期时推进 term，并按需同步 epoch（用于后续 quorum 入站处理）。
def observe_term(term: int, reason: str, *, sync_epoch: bool = True, reset_voted_for: bool = True) -> Dict[str, Any]:
    return advance_term(
        term=term,
        reason=f"observe_term:{reason}",
        sync_epoch=sync_epoch,
        reset_voted_for=reset_voted_for,
    )


# 中文：记录当前任期的投票对象；允许传空字符串清票。
def mark_voted_for(voted_for: str, reason: str, term: Optional[int] = None) -> Dict[str, Any]:
    return record_vote(
        voted_for=voted_for,
        reason=reason,
        term=term,
    )


# 中文：保留旧内部函数名，避免历史调用点在本次重构中断裂。
_step_down_on_higher_term_unlocked = step_down_to_follower
