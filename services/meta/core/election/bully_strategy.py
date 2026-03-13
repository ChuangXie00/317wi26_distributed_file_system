import json
from typing import Any, Dict, List
from urllib.error import HTTPError, URLError

from ..config import META_NODE_ID, build_meta_base_url, get_meta_peer_nodes
from ..runtime import (
    align_epoch_floor,
    begin_election_round,
    get_current_leader_id,
    get_current_term,
    get_leader_epoch,
    get_node_role,
    get_rejoin_election_holdoff,
    get_voted_for,
    mark_election_deferred,
    observe_candidate_epoch,
    promote_self_to_leader,
    tick_lamport,
)
from .common import known_meta_nodes, normalize_node_id
from .coordinator import broadcast_coordinator
from .transport import post_json


# Bully 策略实现（按 node_id 字典序比较优先级）。
class BullyElectionStrategy:
    # 发起本地 election：仅向优先级更高的节点发送请求。
    def trigger_election(self, reason: str) -> Dict[str, Any]:
        round_info = begin_election_round(reason=reason)
        candidate_epoch = int(round_info["epoch"])

        higher_nodes = [node_id for node_id in get_meta_peer_nodes() if node_id > META_NODE_ID]
        ok_nodes: List[str] = []
        failed_nodes: List[Dict[str, str]] = []

        for peer_node_id in higher_nodes:
            req_lamport = tick_lamport(event="send_election_request")
            payload = {
                "candidate_id": META_NODE_ID,
                "candidate_epoch": candidate_epoch,
                "lamport": req_lamport,
                "reason": reason,
            }
            peer_url = f"{build_meta_base_url(peer_node_id)}/internal/election"
            try:
                resp = post_json(peer_url, payload)
                tick_lamport(event="recv_election_response", incoming_lamport=int(resp.get("lamport", 0)))
                if bool(resp.get("ok", False)):
                    ok_nodes.append(peer_node_id)
            except (HTTPError, URLError, OSError, RuntimeError, json.JSONDecodeError) as exc:
                failed_nodes.append({"node_id": peer_node_id, "error": str(exc)})

        if ok_nodes:
            # 有更高优先级节点存活，本节点让位为 follower。
            defer_state = mark_election_deferred(
                epoch=candidate_epoch,
                reason=f"higher_node_alive:{reason}",
                defer_to_nodes=ok_nodes,
            )
            return {
                "status": "deferred",
                "reason": reason,
                "candidate_epoch": candidate_epoch,
                "ok_from_higher_nodes": ok_nodes,
                "failed_nodes": failed_nodes,
                "defer_state": defer_state,
            }

        # 没有更高优先级节点响应，当前节点当选 leader 并广播 coordinator。
        promote_info = promote_self_to_leader(epoch=candidate_epoch, reason=f"bully_win:{reason}")
        coordinator_lamport = tick_lamport(event="send_coordinator")
        broadcast_result = broadcast_coordinator(
            leader_id=META_NODE_ID,
            leader_epoch=int(promote_info["leader_epoch"]),
            lamport=coordinator_lamport,
            reason=reason,
        )
        return {
            "status": "elected",
            "reason": reason,
            "leader_id": META_NODE_ID,
            "leader_epoch": int(promote_info["leader_epoch"]),
            "broadcast": broadcast_result,
        }

    # 处理入站 election 请求，返回是否 ack 以及是否建议本地发起预抢占。
    def handle_incoming_election(
        self,
        candidate_id: str,
        candidate_epoch: int,
        lamport: int,
        reason: str,
    ) -> Dict[str, Any]:
        normalized_candidate_id = normalize_node_id(candidate_id)
        normalized_epoch = max(0, int(candidate_epoch))
        before_epoch = get_leader_epoch()
        before_leader = normalize_node_id(get_current_leader_id())
        known_nodes = known_meta_nodes()
        has_higher_priority = META_NODE_ID > normalized_candidate_id

        tick_lamport(event="recv_election", incoming_lamport=int(lamport))

        # stale 条件：未知节点、自己给自己发、epoch 倒退、同 epoch 但已存在更高优先级 leader。
        stale = (
            normalized_candidate_id not in known_nodes
            or normalized_candidate_id == META_NODE_ID
            or normalized_epoch < before_epoch
            or (
                normalized_epoch == before_epoch
                and bool(before_leader)
                and before_leader != normalized_candidate_id
                and before_leader > normalized_candidate_id
            )
        )

        # 关键修复：高优先级节点收到更高 epoch 请求时抬升 epoch floor，
        # 避免自己继续发送旧 epoch heartbeat，导致低优先级节点反复忽略并重新发起 election。
        if has_higher_priority and not stale and normalized_epoch > before_epoch:
            align_epoch_floor(
                min_epoch=normalized_epoch,
                reason=f"incoming_election_floor:{reason}",
            )

        # 仅在对方优先级更高时执行降级 fencing。
        if not stale and not has_higher_priority:
            observe_candidate_epoch(
                candidate_epoch=normalized_epoch,
                reason=f"incoming_election:{reason}",
            )

        local_role = get_node_role()
        current_leader_id = normalize_node_id(get_current_leader_id())
        current_epoch = get_leader_epoch()
        is_self_leader = local_role == "leader" and get_current_leader_id() == META_NODE_ID

        has_known_higher_or_equal_leader = bool(
            current_leader_id
            and current_epoch >= normalized_epoch
            and current_leader_id >= normalized_candidate_id
        )

        rejoin_holdoff = get_rejoin_election_holdoff()
        rejoin_guard_active = bool(rejoin_holdoff.get("active", False))
        should_start_local_election = bool(
            has_higher_priority
            and not is_self_leader
            and not stale
            and not has_known_higher_or_equal_leader
            and not rejoin_guard_active
        )
        ok = bool(has_higher_priority and not stale)

        resp_lamport = tick_lamport(event="send_election_response")
        return {
            "status": "ok",
            "ok": ok,
            "stale": stale,
            "should_start_local_election": should_start_local_election,
            "responder_id": META_NODE_ID,
            "responder_role": get_node_role(),
            "responder_epoch": get_leader_epoch(),
            "lamport": resp_lamport,
        }

    # Bully 模式不参与 quorum vote，返回协议级“不支持”。
    def handle_incoming_vote_request(
        self,
        candidate_id: str,
        candidate_term: int,
        candidate_epoch: int,
        lamport: int,
        reason: str,
    ) -> Dict[str, Any]:
        tick_lamport(event="recv_vote_request_unsupported", incoming_lamport=int(lamport))
        resp_lamport = tick_lamport(event="send_vote_response_unsupported")
        return {
            "status": "ok",
            "granted": False,
            "stale": True,
            "supported": False,
            "responder_id": META_NODE_ID,
            "responder_role": get_node_role(),
            "responder_term": get_current_term(),
            "responder_epoch": get_leader_epoch(),
            "voted_for": get_voted_for(),
            "lamport": resp_lamport,
            "detail": "vote_protocol_disabled_in_bully_mode",
        }
