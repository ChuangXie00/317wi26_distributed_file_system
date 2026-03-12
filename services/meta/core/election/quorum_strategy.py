import json
from typing import Any, Dict, List
from urllib.error import HTTPError, URLError

from ..config import META_NODE_ID, build_meta_base_url, get_meta_peer_nodes
from ..runtime import (
    begin_election_round,
    get_current_term,
    get_leader_epoch,
    get_node_role,
    get_voted_for,
    mark_election_deferred,
    mark_voted_for,
    observe_candidate_epoch,
    observe_term,
    promote_self_to_leader,
    tick_lamport,
)
from .common import known_meta_nodes, normalize_node_id, quorum_required
from .coordinator import broadcast_coordinator
from .transport import post_json
from .vote_rules import decide_incoming_vote, decide_quorum_round_outcome


# Quorum 策略实现；通过 vote 请求统计多数票并在达 quorum 时晋升 leader。
class QuorumElectionStrategy:
    # 中文：发起 quorum 选举：候选节点先自投票，再向 peers 请求投票，达到多数后当选 leader。
    def trigger_election(self, reason: str) -> Dict[str, Any]:
        round_info = begin_election_round(reason=reason)
        candidate_epoch = int(round_info["epoch"])
        candidate_term = max(candidate_epoch, get_current_term())
        local_vote = mark_voted_for(
            voted_for=META_NODE_ID,
            reason=f"quorum_self_vote:{reason}",
            term=candidate_term,
        )

        peer_nodes = get_meta_peer_nodes()
        total_nodes = len(peer_nodes) + 1
        quorum = quorum_required(total_nodes)

        granted_nodes: List[str] = [META_NODE_ID]
        rejected_nodes: List[Dict[str, str]] = []
        failed_nodes: List[Dict[str, str]] = []
        max_observed_term = int(local_vote.get("term", candidate_term))

        for peer_node_id in peer_nodes:
            req_lamport = tick_lamport(event="send_vote_request")
            payload = {
                "candidate_id": META_NODE_ID,
                "candidate_term": candidate_term,
                "candidate_epoch": candidate_epoch,
                "lamport": req_lamport,
                "reason": reason,
            }
            peer_url = f"{build_meta_base_url(peer_node_id)}/internal/vote"
            try:
                resp = post_json(peer_url, payload)
                tick_lamport(event="recv_vote_response", incoming_lamport=int(resp.get("lamport", 0)))

                responder_term = max(0, int(resp.get("responder_term", 0)))
                max_observed_term = max(max_observed_term, responder_term)
                if bool(resp.get("granted", False)):
                    granted_nodes.append(peer_node_id)
                else:
                    rejected_nodes.append(
                        {
                            "node_id": peer_node_id,
                            "detail": str(resp.get("detail", "")),
                            "supported": str(bool(resp.get("supported", False))).lower(),
                        }
                    )
            except (HTTPError, URLError, OSError, RuntimeError, json.JSONDecodeError) as exc:
                failed_nodes.append({"node_id": peer_node_id, "error": str(exc)})

        granted_votes = len(granted_nodes)
        round_outcome = decide_quorum_round_outcome(
            candidate_term=candidate_term,
            max_observed_term=max_observed_term,
            granted_votes=granted_votes,
            quorum=quorum,
        )
        if round_outcome == "defer_higher_term":
            # 中文：若观测到更高 term，即使当前已拿到票也必须让位，避免旧 term 误当选。
            observe_term(
                term=max_observed_term,
                reason=f"quorum_higher_term_observed:{reason}",
                sync_epoch=True,
                reset_voted_for=True,
            )
            defer_state = mark_election_deferred(
                epoch=max(candidate_epoch, max_observed_term),
                reason=f"quorum_higher_term_observed:{reason}",
                defer_to_nodes=granted_nodes,
            )
            return {
                "status": "deferred",
                "reason": reason,
                "candidate_term": candidate_term,
                "candidate_epoch": candidate_epoch,
                "quorum_required": quorum,
                "granted_votes": granted_votes,
                "granted_nodes": sorted(set(granted_nodes)),
                "rejected_nodes": rejected_nodes,
                "failed_nodes": failed_nodes,
                "max_observed_term": max_observed_term,
                "stale_term_guard": True,
                "defer_state": defer_state,
            }

        if round_outcome == "elected":
            # 中文：满足多数票后晋升 leader，并沿用既有 coordinator 广播链路收敛全局视图。
            promote_info = promote_self_to_leader(epoch=candidate_epoch, reason=f"quorum_win:{reason}")
            coordinator_lamport = tick_lamport(event="send_coordinator")
            broadcast_result = broadcast_coordinator(
                leader_id=META_NODE_ID,
                leader_epoch=int(promote_info["leader_epoch"]),
                lamport=coordinator_lamport,
                reason=f"quorum_win:{reason}",
            )
            return {
                "status": "elected",
                "reason": reason,
                "leader_id": META_NODE_ID,
                "leader_epoch": int(promote_info["leader_epoch"]),
                "candidate_term": candidate_term,
                "quorum_required": quorum,
                "granted_votes": granted_votes,
                "granted_nodes": sorted(set(granted_nodes)),
                "rejected_nodes": rejected_nodes,
                "failed_nodes": failed_nodes,
                "max_observed_term": max_observed_term,
                "broadcast": broadcast_result,
            }

        # 中文：未达到法定票数则结束本轮，回退 follower，等待下一次 timeout/触发重试。
        defer_state = mark_election_deferred(
            epoch=candidate_epoch,
            reason=f"quorum_not_reached:{reason}",
            defer_to_nodes=granted_nodes,
        )
        return {
            "status": "deferred",
            "reason": reason,
            "candidate_term": candidate_term,
            "candidate_epoch": candidate_epoch,
            "quorum_required": quorum,
            "granted_votes": granted_votes,
            "granted_nodes": sorted(set(granted_nodes)),
            "rejected_nodes": rejected_nodes,
            "failed_nodes": failed_nodes,
            "max_observed_term": max_observed_term,
            "defer_state": defer_state,
        }

    # 中文：当前提交仅保留接口占位，避免在接入前误用 quorum 入站处理。
    def handle_incoming_election(
        self,
        candidate_id: str,
        candidate_epoch: int,
        lamport: int,
        reason: str,
    ) -> Dict[str, Any]:
        tick_lamport(event="recv_legacy_election_in_quorum", incoming_lamport=int(lamport))
        # 中文：quorum 模式不处理 Bully election 请求，返回“可忽略”的稳定响应，避免 500 噪音。
        resp_lamport = tick_lamport(event="send_legacy_election_ack_in_quorum")
        return {
            "status": "ok",
            "ok": False,
            "stale": True,
            "should_start_local_election": False,
            "responder_id": META_NODE_ID,
            "responder_role": get_node_role(),
            "responder_epoch": get_leader_epoch(),
            "lamport": resp_lamport,
        }

    # 中文：处理 quorum 投票请求；对旧 term 拒绝授票，对新 term 先降级再按“一任期一票”授票。
    def handle_incoming_vote_request(
        self,
        candidate_id: str,
        candidate_term: int,
        candidate_epoch: int,
        lamport: int,
        reason: str,
    ) -> Dict[str, Any]:
        normalized_candidate_id = normalize_node_id(candidate_id)
        normalized_term = max(0, int(candidate_term))
        normalized_epoch = max(0, int(candidate_epoch))
        known_nodes = known_meta_nodes()

        tick_lamport(event="recv_vote_request", incoming_lamport=int(lamport))
        observe_term(
            term=normalized_term,
            reason=f"incoming_vote_request:{reason}",
            sync_epoch=True,
            reset_voted_for=True,
        )

        current_term = get_current_term()
        current_voted_for = normalize_node_id(get_voted_for())
        decision = decide_incoming_vote(
            self_node_id=META_NODE_ID,
            candidate_id=normalized_candidate_id,
            candidate_term=normalized_term,
            current_term=current_term,
            current_voted_for=current_voted_for,
            candidate_known=normalized_candidate_id in known_nodes,
        )
        stale = bool(decision.get("stale", False))
        granted = False
        detail = str(decision.get("detail", ""))

        if bool(decision.get("should_record_vote", False)):
            vote_result = mark_voted_for(
                voted_for=str(decision.get("vote_for", "")),
                reason=f"{str(decision.get('vote_reason', 'incoming_vote_request'))}:{reason}",
                term=normalized_term,
            )
            granted = not bool(vote_result.get("stale_term", False))
            if bool(vote_result.get("stale_term", False)):
                detail = "stale_term"
            elif bool(decision.get("precheck_granted", False)):
                detail = str(decision.get("detail", "vote_granted"))
        else:
            granted = bool(decision.get("precheck_granted", False))

        # 中文：若 candidate 透传了更高 epoch，保守抬升本地 epoch，防止旧视图写入路径延迟收敛。
        if normalized_epoch > get_leader_epoch():
            observe_candidate_epoch(
                candidate_epoch=normalized_epoch,
                reason=f"incoming_vote_request_epoch:{reason}",
            )

        resp_lamport = tick_lamport(event="send_vote_response")
        return {
            "status": "ok",
            "granted": granted,
            "stale": stale,
            "supported": True,
            "responder_id": META_NODE_ID,
            "responder_role": get_node_role(),
            "responder_term": get_current_term(),
            "responder_epoch": get_leader_epoch(),
            "voted_for": get_voted_for(),
            "lamport": resp_lamport,
            "detail": detail,
        }
