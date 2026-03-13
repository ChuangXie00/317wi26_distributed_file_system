import copy
import json
import time
from typing import Any, Dict, List
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest, urlopen

from ..config import (
    META_FOLLOWER_URLS,
    META_INTERNAL_TIMEOUT_SEC,
    META_NODE_ID,
    get_meta_peer_urls,
)
from ..runtime import (
    get_current_leader_id,
    get_lamport_clock,
    get_last_applied_lamport,
    get_leader_epoch,
    get_node_role,
    is_writable_leader,
    mark_last_applied_lamport,
    observe_leader,
    tick_lamport,
)
from ..state import (
    State,
    get_membership_snapshot,
    load_state,
    mutate_state,
    persist_state,
    refresh_cluster_membership,
)
from .state_store import now_iso, record_error, update_runtime


# 内部 HTTP JSON POST 工具，供 heartbeat/replicate/election 使用。
def _post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = UrlRequest(url, data=body, method="POST", headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=META_INTERNAL_TIMEOUT_SEC) as resp:
        if resp.status != 200:
            raise RuntimeError(f"internal POST failed: status={resp.status}, url={url}")
        raw = resp.read()
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}


# 内部 HTTP JSON GET 工具，供启动期重入探测使用。
def _get_json(url: str) -> Dict[str, Any]:
    req = UrlRequest(url, method="GET")
    with urlopen(req, timeout=META_INTERNAL_TIMEOUT_SEC) as resp:
        if resp.status != 200:
            raise RuntimeError(f"internal GET failed: status={resp.status}, url={url}")
        raw = resp.read()
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}


# 安全解析整数字段，避免远端 debug 字段异常导致启动探测中断。
def _safe_int(raw_value: Any, default: int = 0) -> int:
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return int(default)


# 组合 peer URL：优先使用 META_CLUSTER_NODES 计算出的 peers；仅在无 peers 时回退 legacy 配置。
def _peer_urls() -> List[str]:
    # 第一优先级：统一集群配置的 peer 列表。
    cluster_urls: List[str] = []
    for url in get_meta_peer_urls():
        clean = str(url).strip().rstrip("/")
        if clean:
            cluster_urls.append(clean)
    if cluster_urls:
        return cluster_urls

    # 兼容兜底：若未配置集群 peers，则回退 0.1p04 的 META_FOLLOWER_URLS。
    legacy_urls: List[str] = []
    for url in META_FOLLOWER_URLS:
        clean = str(url).strip().rstrip("/")
        if clean:
            legacy_urls.append(clean)
    return legacy_urls


# 启动重入探测：查询 peers 的 /debug/leader，尽可能识别当前集群 leader。
def probe_cluster_leader_for_rejoin() -> Dict[str, Any]:
    peers = get_meta_peer_urls()
    observed_nodes: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []
    best_leader_id = ""
    best_leader_epoch = 0

    for peer_base in peers:
        debug_url = f"{peer_base.rstrip('/')}/debug/leader"
        try:
            data = _get_json(debug_url)
            peer_node_id = str(data.get("node_id", "")).strip().lower()
            peer_role = str(data.get("role", "")).strip().lower()
            peer_leader_id = str(data.get("leader", "")).strip().lower()
            peer_leader_epoch = max(0, _safe_int(data.get("leader_epoch", 0)))

            observed_nodes.append(
                {
                    "peer_base": peer_base,
                    "node_id": peer_node_id,
                    "role": peer_role,
                    "leader_id": peer_leader_id,
                    "leader_epoch": peer_leader_epoch,
                }
            )

            if peer_leader_id:
                # 选择规则：先看更高 epoch；同 epoch 下按 leader_id 字典序收敛。
                if (
                    peer_leader_epoch > best_leader_epoch
                    or (peer_leader_epoch == best_leader_epoch and peer_leader_id > best_leader_id)
                ):
                    best_leader_id = peer_leader_id
                    best_leader_epoch = peer_leader_epoch
        except (HTTPError, URLError, OSError, RuntimeError) as exc:
            errors.append({"peer_base": peer_base, "error": str(exc)})

    return {
        "peer_count": len(peers),
        "reachable_peer_count": len(observed_nodes),
        "leader_id": best_leader_id,
        "leader_epoch": best_leader_epoch,
        "observed_nodes": observed_nodes,
        "errors": errors,
    }


# 清洗复制消息中的 membership 结构，兼容旧格式。
def _sanitize_membership(raw_membership: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(raw_membership, dict):
        raise ValueError("membership must be an object")

    out: Dict[str, Dict[str, Any]] = {}
    for node_id, entry in raw_membership.items():
        node_key = str(node_id).strip()
        if not node_key:
            continue

        if isinstance(entry, dict):
            out[node_key] = copy.deepcopy(entry)
        else:
            # 兼容旧数据（value 可能是纯状态字符串）。
            out[node_key] = {"status": str(entry)}
    return out


# 构造状态快照（仅同步 membership；files/chunks 由 PostgreSQL 承载）。
def build_state_snapshot(reason: str = "manual") -> Dict[str, Any]:
    state = load_state()
    # 可写 leader 推送快照前刷新完整 cluster membership（storage + meta）。
    if is_writable_leader() and refresh_cluster_membership(state):
        persist_state(state)

    lamport = tick_lamport(event="build_replicate_state")
    return {
        "source_node_id": META_NODE_ID,
        "leader_id": get_current_leader_id() or META_NODE_ID,
        "leader_epoch": get_leader_epoch(),
        "lamport": lamport,
        "generated_at": now_iso(),
        "reason": reason,
        "membership": get_membership_snapshot(state),
    }


# leader 向 peers 推送 replicate_state；失败只记录，不阻断主链路。
def push_state_to_followers(reason: str = "manual") -> Dict[str, Any]:
    if not is_writable_leader():
        return {"status": "skipped", "reason": "node is not writable leader", "attempted": 0, "succeeded": 0, "failed": []}

    peers = _peer_urls()
    if not peers:
        return {"status": "skipped", "reason": "no peer configured", "attempted": 0, "succeeded": 0, "failed": []}

    snapshot = build_state_snapshot(reason=reason)
    update_runtime(last_snapshot_sent_at=now_iso())

    failed: List[Dict[str, str]] = []
    success_count = 0
    for peer_base in peers:
        try:
            resp = _post_json(f"{peer_base}/internal/replicate_state", snapshot)
            tick_lamport(event="recv_replicate_ack", incoming_lamport=int(resp.get("lamport", 0)))
            success_count += 1
            update_runtime(last_snapshot_success_at=now_iso())
        except (HTTPError, URLError, OSError, RuntimeError) as exc:
            failed.append({"peer": peer_base, "error": str(exc)})
            record_error(f"replicate_state failed to {peer_base}: {exc}")

    return {
        "status": "ok",
        "reason": reason,
        "attempted": len(peers),
        "succeeded": success_count,
        "failed": failed,
    }


# 记录 leader 心跳并按 epoch 执行降级（fencing）。
def record_leader_heartbeat(leader_id: str, leader_epoch: int, lamport: int) -> Dict[str, Any]:
    observed_at = now_iso()
    tick_lamport(event="recv_leader_heartbeat", incoming_lamport=max(0, int(lamport)))
    observe_result = observe_leader(
        leader_id=str(leader_id).strip(),
        leader_epoch=max(0, int(leader_epoch)),
        reason="leader_heartbeat",
    )

    # 只有“未被忽略”的心跳才刷新超时计时器，避免旧 epoch 心跳干扰 takeover。
    if not bool(observe_result.get("ignored", False)):
        update_runtime(
            last_leader_heartbeat_at=observed_at,
            last_leader_heartbeat_from=str(leader_id).strip(),
            last_leader_heartbeat_epoch=max(0, int(leader_epoch)),
            last_leader_heartbeat_lamport=max(0, int(lamport)),
            last_leader_heartbeat_ts=time.time(),
        )
    return {
        "observed_at": observed_at,
        "changed": bool(observe_result.get("changed", False)),
        "ignored": bool(observe_result.get("ignored", False)),
        "role": get_node_role(),
        "leader_id": get_current_leader_id(),
        "leader_epoch": get_leader_epoch(),
    }


# 应用 leader 下发的复制状态，并基于 Lamport 拒绝旧消息覆盖新状态。
def apply_replicated_state(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    source_node_id = str(snapshot.get("source_node_id", "")).strip() or "unknown"
    leader_id = str(snapshot.get("leader_id", source_node_id)).strip() or source_node_id
    leader_epoch = max(0, int(snapshot.get("leader_epoch", 0)))
    incoming_lamport = max(0, int(snapshot.get("lamport", 0)))
    reason = str(snapshot.get("reason", "unknown")).strip() or "unknown"

    tick_lamport(event="recv_replicate_state", incoming_lamport=incoming_lamport)
    observe_result = observe_leader(leader_id=leader_id, leader_epoch=leader_epoch, reason=f"replicate_state:{reason}")
    if bool(observe_result.get("ignored", False)):
        return {
            "status": "ignored",
            "applied_at": now_iso(),
            "changed": False,
            "detail": f"stale epoch replicate_state ignored: incoming_epoch={leader_epoch}",
            "lamport": get_lamport_clock(),
        }

    last_applied = get_last_applied_lamport()
    if incoming_lamport <= last_applied:
        update_runtime(
            last_sync_source=source_node_id,
            last_sync_reason=f"ignored_stale_lamport:{reason}",
            last_sync_lamport=incoming_lamport,
        )
        return {
            "status": "ignored",
            "applied_at": now_iso(),
            "changed": False,
            "detail": f"stale replicate_state lamport={incoming_lamport}, last_applied={last_applied}",
            "lamport": get_lamport_clock(),
        }

    membership = _sanitize_membership(snapshot.get("membership"))
    changed_holder = {"changed": False}

    def _mutator(state: State) -> bool:
        current = state.get("membership", {})
        if current == membership:
            return False
        # follower 直接应用 leader 快照，保持 warm standby。
        state["membership"] = copy.deepcopy(membership)
        changed_holder["changed"] = True
        return True

    mutate_state(_mutator)
    mark_last_applied_lamport(incoming_lamport, reason=f"replicate_state:{reason}")

    applied_at = now_iso()
    update_runtime(
        last_sync_applied_at=applied_at,
        last_sync_source=source_node_id,
        last_sync_reason=reason,
        last_sync_lamport=incoming_lamport,
    )
    return {
        "status": "synced",
        "applied_at": applied_at,
        "changed": changed_holder["changed"],
        "detail": "",
        "lamport": get_lamport_clock(),
    }


# leader 定期向 peers 发送 heartbeat，附带 leader_epoch 与 Lamport。
def _send_heartbeat_to_peers() -> None:
    if not is_writable_leader():
        return

    peers = _peer_urls()
    if not peers:
        return

    sent_at = now_iso()
    update_runtime(last_heartbeat_sent_at=sent_at)
    for peer_base in peers:
        lamport = tick_lamport(event="send_leader_heartbeat")
        payload = {
            "leader_id": META_NODE_ID,
            "leader_epoch": get_leader_epoch(),
            "lamport": lamport,
            "sent_at": sent_at,
        }
        try:
            resp = _post_json(f"{peer_base}/internal/heartbeat", payload)
            tick_lamport(event="recv_heartbeat_ack", incoming_lamport=int(resp.get("lamport", 0)))
            # 利用 heartbeat ack 做反向 fencing 校验，发现更高 epoch 立刻降级。
            ack_leader_id = str(resp.get("current_leader_id", "")).strip()
            ack_leader_epoch = max(0, int(resp.get("leader_epoch", 0)))
            if ack_leader_id and ack_leader_id != META_NODE_ID:
                observe_leader(
                    leader_id=ack_leader_id,
                    leader_epoch=ack_leader_epoch,
                    reason="heartbeat_ack_fencing",
                )
            update_runtime(last_heartbeat_success_at=now_iso())
        except (HTTPError, URLError, OSError, RuntimeError) as exc:
            record_error(f"leader heartbeat failed to {peer_base}: {exc}")
