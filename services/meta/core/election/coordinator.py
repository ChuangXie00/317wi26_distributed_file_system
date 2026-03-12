import json
from typing import Any, Dict, List
from urllib.error import HTTPError, URLError

from ..config import META_NODE_ID, build_meta_base_url, get_meta_peer_nodes
from ..runtime import (
    get_current_leader_id,
    get_leader_epoch,
    get_node_role,
    observe_leader,
    tick_lamport,
)
from .common import known_meta_nodes, normalize_node_id
from .transport import post_json


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
            resp = post_json(peer_url, payload)
            tick_lamport(event="recv_coordinator_ack", incoming_lamport=int(resp.get("lamport", 0)))
            ok_nodes.append(peer_node_id)
        except (HTTPError, URLError, OSError, RuntimeError, json.JSONDecodeError) as exc:
            failed_nodes.append({"node_id": peer_node_id, "error": str(exc)})

    return {"attempted": len(get_meta_peer_nodes()), "ok_nodes": ok_nodes, "failed_nodes": failed_nodes}


# 处理收到的 coordinator 消息；根据 epoch 执行 leader 视图更新与强制降级。
def handle_incoming_coordinator(leader_id: str, leader_epoch: int, lamport: int, reason: str) -> Dict[str, Any]:
    tick_lamport(event="recv_coordinator", incoming_lamport=int(lamport))
    normalized_leader_id = normalize_node_id(leader_id)

    # 仅接受来自已知 meta 节点的 coordinator，防止未知节点污染 leader 视图。
    if normalized_leader_id not in known_meta_nodes():
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
