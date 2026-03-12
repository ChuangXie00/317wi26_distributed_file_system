import json
from typing import Any, Callable, Dict, List, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest, urlopen

from .config import (
    LEADER_ELECTION_MODE,
    META_INTERNAL_TIMEOUT_SEC,
    META_NODE_ID,
    build_meta_base_url,
    get_meta_peer_nodes,
)
from .runtime import (
    begin_election_round,
    get_current_leader_id,
    get_current_term,
    get_leader_epoch,
    get_node_role,
    get_rejoin_election_holdoff,
    get_voted_for,
    mark_election_deferred,
    mark_voted_for,
    observe_candidate_epoch,
    observe_leader,
    observe_term,
    promote_self_to_leader,
    tick_lamport,
)


# 统一 HTTP JSON POST 工具，供 election/coordinator 内部通信复用。
def _post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = UrlRequest(url, data=body, method="POST", headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=META_INTERNAL_TIMEOUT_SEC) as resp:
        if resp.status != 200:
            raise RuntimeError(f"internal POST failed: status={resp.status}, url={url}")
        raw = resp.read()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))


# 规范化节点 ID，避免大小写/空白导致优先级比较不一致。
def _normalize_node_id(raw_node_id: str) -> str:
    return str(raw_node_id or "").strip().lower()


# 返回当前可识别的 meta 节点集合（含自身），用于过滤未知节点的内部协议请求。
def _known_meta_nodes() -> List[str]:
    return sorted(set(get_meta_peer_nodes() + [META_NODE_ID]))


# 中文：计算当前集群所需法定票数（quorum），规则为 floor(N/2)+1。
def _quorum_required(total_nodes: int) -> int:
    normalized_total = max(1, int(total_nodes))
    return (normalized_total // 2) + 1


# 选主策略抽象接口；Phase 5.0 当前仅实现 Bully。
class ElectionStrategy(Protocol):
    # 中文：触发本节点发起一轮选主流程，返回本轮执行结果。
    def trigger_election(self, reason: str) -> Dict[str, Any]:
        ...

    # 中文：处理来自其他节点的 election 请求，返回协议响应字段。
    def handle_incoming_election(
        self,
        candidate_id: str,
        candidate_epoch: int,
        lamport: int,
        reason: str,
    ) -> Dict[str, Any]:
        ...

    # 中文：处理投票请求（quorum 协议），返回是否授票及响应方状态。
    def handle_incoming_vote_request(
        self,
        candidate_id: str,
        candidate_term: int,
        candidate_epoch: int,
        lamport: int,
        reason: str,
    ) -> Dict[str, Any]:
        ...


# Bully 策略实现（按 node_id 字典序比较优先级）。
class BullyElectionStrategy:
    # 中文：按 Bully 规则发起选举，只向更高优先级节点发起请求。
    def trigger_election(self, reason: str) -> Dict[str, Any]:
        round_info = begin_election_round(reason=reason)
        candidate_epoch = int(round_info["epoch"])

        # Bully 只向“优先级更高”的节点发 election 请求。
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
                resp = _post_json(peer_url, payload)
                tick_lamport(event="recv_election_response", incoming_lamport=int(resp.get("lamport", 0)))
                if bool(resp.get("ok", False)):
                    ok_nodes.append(peer_node_id)
            except (HTTPError, URLError, OSError, RuntimeError, json.JSONDecodeError) as exc:
                failed_nodes.append({"node_id": peer_node_id, "error": str(exc)})

        if ok_nodes:
            # 三节点场景下，一旦确认更高优先级节点存活，本节点立即让位为 follower，避免重复抢占抖动。
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

        # 没有更高优先级节点存活，则当前 candidate 直接晋升为 leader。
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

    # 中文：按 Bully 规则处理入站 election 请求，并决定是否建议本地预抢占选举。
    def handle_incoming_election(
        self,
        candidate_id: str,
        candidate_epoch: int,
        lamport: int,
        reason: str,
    ) -> Dict[str, Any]:
        normalized_candidate_id = _normalize_node_id(candidate_id)
        normalized_epoch = max(0, int(candidate_epoch))
        before_epoch = get_leader_epoch()
        before_leader = _normalize_node_id(get_current_leader_id())
        known_nodes = _known_meta_nodes()

        tick_lamport(event="recv_election", incoming_lamport=int(lamport))

        # 以下请求视为 stale：未知节点、候选者是自己、epoch 倒退、同 epoch 但已存在更高优先级 leader。
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

        if not stale:
            # 收到更高 epoch 的 candidate 时，先更新本地 epoch 并降级为 follower（fencing）。
            observe_candidate_epoch(candidate_epoch=normalized_epoch, reason=f"incoming_election:{reason}")

        local_role = get_node_role()
        current_leader_id = _normalize_node_id(get_current_leader_id())
        current_epoch = get_leader_epoch()
        has_higher_priority = META_NODE_ID > normalized_candidate_id
        is_self_leader = local_role == "leader" and get_current_leader_id() == META_NODE_ID
        # 三节点稳定化：若本地已知同/更高 epoch 的更高优先级 leader，则只回复 ok，不再重复触发本地选举。
        has_known_higher_or_equal_leader = bool(
            current_leader_id
            and current_epoch >= normalized_epoch
            and current_leader_id >= normalized_candidate_id
        )
        # 中文：重入冷却期内不主动抢主，优先等待现任 leader 继续收敛。
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

    # 中文：Bully 模式下不参与 quorum 投票，返回协议级“已处理但不支持”。
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
        quorum = _quorum_required(total_nodes)

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
                resp = _post_json(peer_url, payload)
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
        if max_observed_term > candidate_term:
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

        if granted_votes >= quorum:
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
        normalized_candidate_id = _normalize_node_id(candidate_id)
        normalized_term = max(0, int(candidate_term))
        normalized_epoch = max(0, int(candidate_epoch))
        known_nodes = _known_meta_nodes()

        tick_lamport(event="recv_vote_request", incoming_lamport=int(lamport))
        observe_term(
            term=normalized_term,
            reason=f"incoming_vote_request:{reason}",
            sync_epoch=True,
            reset_voted_for=True,
        )

        current_term = get_current_term()
        current_voted_for = _normalize_node_id(get_voted_for())
        stale = bool(
            normalized_candidate_id not in known_nodes
            or normalized_candidate_id == META_NODE_ID
            or normalized_term < current_term
        )
        granted = False
        detail = ""

        if stale:
            detail = "stale_or_unknown_candidate_or_term"
        elif current_voted_for and current_voted_for != normalized_candidate_id:
            detail = f"already_voted_for:{current_voted_for}"
        else:
            vote_result = mark_voted_for(
                voted_for=normalized_candidate_id,
                reason=f"incoming_vote_request:{reason}",
                term=normalized_term,
            )
            granted = not bool(vote_result.get("stale_term", False))
            detail = "vote_granted" if granted else "stale_term"

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


# 中文：策略工厂映射，统一管理不同选主模式对应的策略实现。
_ELECTION_STRATEGY_FACTORIES: Dict[str, Callable[[], ElectionStrategy]] = {
    "bully": BullyElectionStrategy,
    "quorum": QuorumElectionStrategy,
}


# 根据配置获取选主策略实例；具体模式能力在各策略内自行约束。
def get_election_strategy() -> ElectionStrategy:
    strategy_factory = _ELECTION_STRATEGY_FACTORIES.get(LEADER_ELECTION_MODE)
    if strategy_factory is None:
        raise RuntimeError(f"unsupported LEADER_ELECTION_MODE={LEADER_ELECTION_MODE!r}")
    return strategy_factory()


# 对外统一入口，触发一次选主流程。
def trigger_election(reason: str) -> Dict[str, Any]:
    strategy = get_election_strategy()
    return strategy.trigger_election(reason=reason)


# 广播 coordinator 消息，让其他节点更新 leader 视图并执行降级。
def broadcast_coordinator(leader_id: str, leader_epoch: int, lamport: int, reason: str) -> Dict[str, Any]:
    ok_nodes: List[str] = []
    failed_nodes: List[Dict[str, str]] = []

    for peer_node_id in get_meta_peer_nodes():
        payload = {
            "leader_id": str(leader_id).strip(),
            "leader_epoch": int(leader_epoch),
            "lamport": int(lamport),
            "reason": str(reason),
        }
        peer_url = f"{build_meta_base_url(peer_node_id)}/internal/coordinator"
        try:
            resp = _post_json(peer_url, payload)
            tick_lamport(event="recv_coordinator_ack", incoming_lamport=int(resp.get("lamport", 0)))
            ok_nodes.append(peer_node_id)
        except (HTTPError, URLError, OSError, RuntimeError, json.JSONDecodeError) as exc:
            failed_nodes.append({"node_id": peer_node_id, "error": str(exc)})

    return {"attempted": len(get_meta_peer_nodes()), "ok_nodes": ok_nodes, "failed_nodes": failed_nodes}


# 处理收到的 election 请求；返回 ok 与是否建议本节点启动本地 election。
def handle_incoming_election(candidate_id: str, candidate_epoch: int, lamport: int, reason: str) -> Dict[str, Any]:
    strategy = get_election_strategy()
    return strategy.handle_incoming_election(
        candidate_id=candidate_id,
        candidate_epoch=candidate_epoch,
        lamport=lamport,
        reason=reason,
    )


# 处理收到的 quorum vote 请求；用于 candidate 统计多数票结果。
def handle_incoming_vote_request(
    candidate_id: str,
    candidate_term: int,
    candidate_epoch: int,
    lamport: int,
    reason: str,
) -> Dict[str, Any]:
    strategy = get_election_strategy()
    return strategy.handle_incoming_vote_request(
        candidate_id=candidate_id,
        candidate_term=candidate_term,
        candidate_epoch=candidate_epoch,
        lamport=lamport,
        reason=reason,
    )


# 处理收到的 coordinator 消息；根据 epoch 执行 leader 视图更新与强制降级。
def handle_incoming_coordinator(leader_id: str, leader_epoch: int, lamport: int, reason: str) -> Dict[str, Any]:
    tick_lamport(event="recv_coordinator", incoming_lamport=int(lamport))
    normalized_leader_id = _normalize_node_id(leader_id)

    # 仅接受来自已知 meta 节点的 coordinator，防止未知节点污染 leader 视图。
    if normalized_leader_id not in _known_meta_nodes():
        ack_lamport = tick_lamport(event="send_coordinator_ack_unknown_leader")
        return {
            "status": "ack",
            "changed": False,
            "ignored": True,
            "node_id": META_NODE_ID,
            "role": get_node_role(),
            "leader_id": get_current_leader_id(),
            "leader_epoch": get_leader_epoch(),
            "lamport": ack_lamport,
        }

    observe_result = observe_leader(
        leader_id=normalized_leader_id,
        leader_epoch=max(0, int(leader_epoch)),
        reason=f"incoming_coordinator:{reason}",
    )
    ack_lamport = tick_lamport(event="send_coordinator_ack")
    return {
        "status": "ack",
        "changed": bool(observe_result["changed"]),
        "ignored": bool(observe_result["ignored"]),
        "node_id": META_NODE_ID,
        "role": get_node_role(),
        "leader_id": get_current_leader_id(),
        "leader_epoch": get_leader_epoch(),
        "lamport": ack_lamport,
    }
