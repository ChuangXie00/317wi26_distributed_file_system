import copy
import json
import os
import threading
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


# peer 失败退避：某个 peer 连续不可达时，短时间内先跳过，
# 避免 DNS/连接超时反复阻塞 leader 周期，触发误超时选举。
_PEER_FAILURE_BACKOFF_SEC = max(1.0, float(os.getenv("META_PEER_FAILURE_BACKOFF_SEC", "12.0")))
_PEER_BACKOFF_LOCK = threading.RLock()
_PEER_BACKOFF_UNTIL: Dict[str, float] = {}


def _peer_in_backoff(peer_base: str, now_ts: float | None = None) -> bool:
    ts = time.time() if now_ts is None else float(now_ts)
    with _PEER_BACKOFF_LOCK:
        until_ts = float(_PEER_BACKOFF_UNTIL.get(peer_base, 0.0) or 0.0)
        if until_ts <= 0:
            return False
        if ts >= until_ts:
            _PEER_BACKOFF_UNTIL.pop(peer_base, None)
            return False
        return True


def _mark_peer_failed(peer_base: str) -> None:
    with _PEER_BACKOFF_LOCK:
        _PEER_BACKOFF_UNTIL[peer_base] = time.time() + _PEER_FAILURE_BACKOFF_SEC


def _mark_peer_success(peer_base: str) -> None:
    with _PEER_BACKOFF_LOCK:
        _PEER_BACKOFF_UNTIL.pop(peer_base, None)


# 鍐呴儴 HTTP JSON POST 宸ュ叿锛屼緵 heartbeat/replicate/election 浣跨敤銆?
def _post_json(url: str, payload: Dict[str, Any], timeout_sec: float | None = None) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = UrlRequest(url, data=body, method="POST", headers={"Content-Type": "application/json"})
    effective_timeout = META_INTERNAL_TIMEOUT_SEC if timeout_sec is None else max(0.1, float(timeout_sec))
    with urlopen(req, timeout=effective_timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"internal POST failed: status={resp.status}, url={url}")
        raw = resp.read()
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}


# 鍐呴儴 HTTP JSON GET 宸ュ叿锛屼緵鍚姩鏈熼噸鍏ユ帰娴嬩娇鐢ㄣ€?
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


# 瀹夊叏瑙ｆ瀽鏁存暟瀛楁锛岄伩鍏嶈繙绔?debug 瀛楁寮傚父瀵艰嚧鍚姩鎺㈡祴涓柇銆?
def _safe_int(raw_value: Any, default: int = 0) -> int:
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return int(default)


# 缁勫悎 peer URL锛氫紭鍏堜娇鐢?META_CLUSTER_NODES 璁＄畻鍑虹殑 peers锛涗粎鍦ㄦ棤 peers 鏃跺洖閫€ legacy 閰嶇疆銆?
def _peer_urls() -> List[str]:
    # 绗竴浼樺厛绾э細缁熶竴闆嗙兢閰嶇疆鐨?peer 鍒楄〃銆?
    cluster_urls: List[str] = []
    for url in get_meta_peer_urls():
        clean = str(url).strip().rstrip("/")
        if clean:
            cluster_urls.append(clean)
    if cluster_urls:
        return cluster_urls

    # 鍏煎鍏滃簳锛氳嫢鏈厤缃泦缇?peers锛屽垯鍥為€€ 0.1p04 鐨?META_FOLLOWER_URLS銆?
    legacy_urls: List[str] = []
    for url in META_FOLLOWER_URLS:
        clean = str(url).strip().rstrip("/")
        if clean:
            legacy_urls.append(clean)
    return legacy_urls


# 鍚姩閲嶅叆鎺㈡祴锛氭煡璇?peers 鐨?/debug/leader锛屽敖鍙兘璇嗗埆褰撳墠闆嗙兢 leader銆?
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
                # 閫夋嫨瑙勫垯锛氬厛鐪嬫洿楂?epoch锛涘悓 epoch 涓嬫寜 leader_id 瀛楀吀搴忔敹鏁涖€?
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


# 娓呮礂澶嶅埗娑堟伅涓殑 membership 缁撴瀯锛屽吋瀹规棫鏍煎紡銆?
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
            # 鍏煎鏃ф暟鎹紙value 鍙兘鏄函鐘舵€佸瓧绗︿覆锛夈€?
            out[node_key] = {"status": str(entry)}
    return out


# 鏋勯€犵姸鎬佸揩鐓э紙浠呭悓姝?membership锛沠iles/chunks 鐢?PostgreSQL 鎵胯浇锛夈€?
def build_state_snapshot(reason: str = "manual") -> Dict[str, Any]:
    state = load_state()
    # 鍙啓 leader 鎺ㄩ€佸揩鐓у墠鍒锋柊瀹屾暣 cluster membership锛坰torage + meta锛夈€?
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


# leader 鍚?peers 鎺ㄩ€?replicate_state锛涘け璐ュ彧璁板綍锛屼笉闃绘柇涓婚摼璺€?
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
    # 对 dead peer 采用短超时，避免单个不可达节点拖慢 leader 周期。
    replicate_timeout_sec = min(float(META_INTERNAL_TIMEOUT_SEC), 1.0)
    for peer_base in peers:
        if _peer_in_backoff(peer_base):
            failed.append({"peer": peer_base, "error": "skipped_by_backoff"})
            continue
        try:
            resp = _post_json(
                f"{peer_base}/internal/replicate_state",
                snapshot,
                timeout_sec=replicate_timeout_sec,
            )
            tick_lamport(event="recv_replicate_ack", incoming_lamport=int(resp.get("lamport", 0)))
            success_count += 1
            _mark_peer_success(peer_base)
            update_runtime(last_snapshot_success_at=now_iso())
        except (HTTPError, URLError, OSError, RuntimeError) as exc:
            _mark_peer_failed(peer_base)
            failed.append({"peer": peer_base, "error": str(exc)})
            record_error(f"replicate_state failed to {peer_base}: {exc}")

    return {
        "status": "ok",
        "reason": reason,
        "attempted": len(peers),
        "succeeded": success_count,
        "failed": failed,
    }


# 璁板綍 leader 蹇冭烦骞舵寜 epoch 鎵ц闄嶇骇锛坒encing锛夈€?
def record_leader_heartbeat(leader_id: str, leader_epoch: int, lamport: int) -> Dict[str, Any]:
    observed_at = now_iso()
    tick_lamport(event="recv_leader_heartbeat", incoming_lamport=max(0, int(lamport)))
    normalized_leader_id = str(leader_id).strip()
    observe_result = observe_leader(
        leader_id=normalized_leader_id,
        leader_epoch=max(0, int(leader_epoch)),
        reason="leader_heartbeat",
    )

    # 只要消息来自当前观测 leader，即便因旧 epoch 被忽略，也刷新超时计时器，
    # 避免 follower 进入“忽略心跳 -> 超时选举 -> epoch 再抬高”的重复抖动。
    current_leader_id = str(observe_result.get("leader_id", "")).strip()
    current_role = str(get_node_role()).strip().lower()
    should_refresh_timeout = (
        not bool(observe_result.get("ignored", False))
        or (normalized_leader_id and current_leader_id == normalized_leader_id)
        # 候选态/跟随态且暂时没有 leader 视图时，若高优先级节点仍在发心跳，
        # 视作“集群仍有可用 leader 信号”，防止本节点反复触发无效抢占。
        or (
            bool(observe_result.get("ignored", False))
            and normalized_leader_id
            and not current_leader_id
            and current_role in {"follower", "candidate"}
            and normalized_leader_id > META_NODE_ID
        )
    )
    if should_refresh_timeout:
        update_runtime(
            last_leader_heartbeat_at=observed_at,
            last_leader_heartbeat_from=normalized_leader_id,
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

# 搴旂敤 leader 涓嬪彂鐨勫鍒剁姸鎬侊紝骞跺熀浜?Lamport 鎷掔粷鏃ф秷鎭鐩栨柊鐘舵€併€?
def apply_replicated_state(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    source_node_id = str(snapshot.get("source_node_id", "")).strip() or "unknown"
    leader_id = str(snapshot.get("leader_id", source_node_id)).strip() or source_node_id
    leader_epoch = max(0, int(snapshot.get("leader_epoch", 0)))
    incoming_lamport = max(0, int(snapshot.get("lamport", 0)))
    reason = str(snapshot.get("reason", "unknown")).strip() or "unknown"

    tick_lamport(event="recv_replicate_state", incoming_lamport=incoming_lamport)
    observe_result = observe_leader(leader_id=leader_id, leader_epoch=leader_epoch, reason=f"replicate_state:{reason}")
    # replicate_state 也能作为“leader 仍在活动”的信号，降低短暂 heartbeat 抖动引发的误选举。
    current_leader_id = str(get_current_leader_id() or "").strip()
    if leader_id and (leader_id == current_leader_id or not current_leader_id):
        update_runtime(
            last_leader_heartbeat_at=now_iso(),
            last_leader_heartbeat_from=leader_id,
            last_leader_heartbeat_epoch=leader_epoch,
            last_leader_heartbeat_lamport=incoming_lamport,
            last_leader_heartbeat_ts=time.time(),
        )
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
        # follower 鐩存帴搴旂敤 leader 蹇収锛屼繚鎸?warm standby銆?
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


# leader 瀹氭湡鍚?peers 鍙戦€?heartbeat锛岄檮甯?leader_epoch 涓?Lamport銆?
def _send_heartbeat_to_peers() -> None:
    if not is_writable_leader():
        return

    peers = _peer_urls()
    if not peers:
        return

    sent_at = now_iso()
    update_runtime(last_heartbeat_sent_at=sent_at)
    # 心跳链路优先级最高：使用更短超时，减少对 dead peer 的阻塞。
    heartbeat_timeout_sec = min(float(META_INTERNAL_TIMEOUT_SEC), 0.6)
    for peer_base in peers:
        if _peer_in_backoff(peer_base):
            continue
        lamport = tick_lamport(event="send_leader_heartbeat")
        payload = {
            "leader_id": META_NODE_ID,
            "leader_epoch": get_leader_epoch(),
            "lamport": lamport,
            "sent_at": sent_at,
        }
        try:
            resp = _post_json(
                f"{peer_base}/internal/heartbeat",
                payload,
                timeout_sec=heartbeat_timeout_sec,
            )
            tick_lamport(event="recv_heartbeat_ack", incoming_lamport=int(resp.get("lamport", 0)))
            # 鍒╃敤 heartbeat ack 鍋氬弽鍚?fencing 鏍￠獙锛屽彂鐜版洿楂?epoch 绔嬪埢闄嶇骇銆?
            ack_leader_id = str(resp.get("current_leader_id", "")).strip()
            ack_leader_epoch = max(0, int(resp.get("leader_epoch", 0)))
            if ack_leader_id and ack_leader_id != META_NODE_ID:
                observe_leader(
                    leader_id=ack_leader_id,
                    leader_epoch=ack_leader_epoch,
                    reason="heartbeat_ack_fencing",
                )
            _mark_peer_success(peer_base)
            update_runtime(last_heartbeat_success_at=now_iso())
        except (HTTPError, URLError, OSError, RuntimeError) as exc:
            _mark_peer_failed(peer_base)
            record_error(f"leader heartbeat failed to {peer_base}: {exc}")

