import json
from typing import Any, Dict, List, Protocol
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
    get_leader_epoch,
    get_node_role,
    observe_candidate_epoch,
    observe_leader,
    promote_self_to_leader,
    tick_lamport,
)


# 中文：统一 HTTP JSON POST 工具，供 election/coordinator 内部通信复用。
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


# 中文：选主策略抽象接口；Phase 5.0 当前仅实现 Bully。
class ElectionStrategy(Protocol):
    def trigger_election(self, reason: str) -> Dict[str, Any]:
        ...


# 中文：Bully 策略实现（按 node_id 字典序比较优先级）。
class BullyElectionStrategy:
    def trigger_election(self, reason: str) -> Dict[str, Any]:
        round_info = begin_election_round(reason=reason)
        candidate_epoch = int(round_info["epoch"])

        # 中文：Bully 只向“优先级更高”的节点发 election 请求。
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
            return {
                "status": "deferred",
                "reason": reason,
                "candidate_epoch": candidate_epoch,
                "ok_from_higher_nodes": ok_nodes,
                "failed_nodes": failed_nodes,
            }

        # 中文：没有更高优先级节点存活，则当前 candidate 直接晋升为 leader。
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


# 中文：根据配置获取选主策略实例；当前仅支持 bully。
def get_election_strategy() -> ElectionStrategy:
    if LEADER_ELECTION_MODE == "bully":
        return BullyElectionStrategy()
    raise RuntimeError(f"unsupported LEADER_ELECTION_MODE={LEADER_ELECTION_MODE!r}")


# 中文：对外统一入口，触发一次选主流程。
def trigger_election(reason: str) -> Dict[str, Any]:
    strategy = get_election_strategy()
    return strategy.trigger_election(reason=reason)


# 中文：广播 coordinator 消息，让其他节点更新 leader 视图并执行降级。
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


# 中文：处理收到的 election 请求；返回 ok 与是否建议本节点启动本地 election。
def handle_incoming_election(candidate_id: str, candidate_epoch: int, lamport: int, reason: str) -> Dict[str, Any]:
    normalized_candidate_id = str(candidate_id).strip()
    normalized_epoch = max(0, int(candidate_epoch))
    before_epoch = get_leader_epoch()

    tick_lamport(event="recv_election", incoming_lamport=int(lamport))

    stale = normalized_epoch < before_epoch
    if not stale:
        # 中文：收到更高 epoch 的 candidate 时，先更新本地 epoch 并降级为 follower（fencing）。
        observe_candidate_epoch(candidate_epoch=normalized_epoch, reason=f"incoming_election:{reason}")

    local_role = get_node_role()
    has_higher_priority = META_NODE_ID > normalized_candidate_id
    is_self_leader = local_role == "leader" and get_current_leader_id() == META_NODE_ID
    should_start_local_election = bool(has_higher_priority and not is_self_leader and not stale)
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


# 中文：处理收到的 coordinator 消息；根据 epoch 执行 leader 视图更新与强制降级。
def handle_incoming_coordinator(leader_id: str, leader_epoch: int, lamport: int, reason: str) -> Dict[str, Any]:
    tick_lamport(event="recv_coordinator", incoming_lamport=int(lamport))
    observe_result = observe_leader(
        leader_id=str(leader_id).strip(),
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
