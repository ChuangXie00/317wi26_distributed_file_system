from typing import Any, Dict


# 纯函数判定当前 candidate 对本节点是否具备投票资格，不直接执行运行时写入。
def decide_incoming_vote(
    *,
    self_node_id: str,
    candidate_id: str,
    candidate_term: int,
    current_term: int,
    current_voted_for: str,
    candidate_known: bool,
) -> Dict[str, Any]:
    stale = bool(
        not candidate_known
        or candidate_id == self_node_id
        or candidate_term < current_term
    )
    if stale:
        return {
            "stale": True,
            "precheck_granted": False,
            "detail": "stale_or_unknown_candidate_or_term",
            "should_record_vote": False,
            "vote_reason": "",
            "vote_for": "",
        }

    if current_voted_for and current_voted_for != candidate_id:
        # 同任期冲突时，允许“已给自己投票”的低优先级节点让票给更高优先级 candidate。
        can_preempt_self_vote = bool(
            candidate_term == current_term
            and current_voted_for == self_node_id
            and candidate_id > self_node_id
        )
        if can_preempt_self_vote:
            return {
                "stale": False,
                "precheck_granted": True,
                "detail": "vote_granted_preempt_self_vote",
                "should_record_vote": True,
                "vote_reason": "incoming_vote_request_preempt_self",
                "vote_for": candidate_id,
            }
        return {
            "stale": False,
            "precheck_granted": False,
            "detail": f"already_voted_for:{current_voted_for}",
            "should_record_vote": False,
            "vote_reason": "",
            "vote_for": "",
        }

    return {
        "stale": False,
        "precheck_granted": True,
        "detail": "vote_granted",
        "should_record_vote": True,
        "vote_reason": "incoming_vote_request",
        "vote_for": candidate_id,
    }


# 纯函数判定本轮 quorum 收敛结果，供策略层决定后续状态动作。
def decide_quorum_round_outcome(
    *,
    candidate_term: int,
    max_observed_term: int,
    granted_votes: int,
    quorum: int,
) -> str:
    if max_observed_term > candidate_term:
        return "defer_higher_term"
    if granted_votes >= quorum:
        return "elected"
    return "defer_not_reached"
