import copy
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest, urlopen

from .config import (
    META_FOLLOWER_URLS,
    META_HEARTBEAT_INTERVAL_SEC,
    META_INTERNAL_TIMEOUT_SEC,
    META_LEADER_HEARTBEAT_TIMEOUT_SEC,
    META_LEADER_URL,
    META_NODE_ID,
    META_SYNC_INTERVAL_SEC,
    get_meta_peer_urls,
)
from .election import trigger_election
from .runtime import (
    get_current_leader_id,
    get_lamport_clock,
    get_last_applied_lamport,
    get_leader_epoch,
    get_node_role,
    get_runtime_snapshot,
    is_writable_leader,
    mark_last_applied_lamport,
    observe_leader,
    tick_lamport,
)
from .state import (
    State,
    get_membership_snapshot,
    load_state,
    mutate_state,
    persist_state,
    refresh_storage_membership,
)


# 中文：复制运行时锁，保护 debug 状态字典与线程句柄。
_RUNTIME_LOCK = threading.RLock()
# 中文：主循环停止信号。
_STOP_EVENT = threading.Event()
# 中文：复制/接管统一后台线程句柄。
_RUNTIME_THREAD: Optional[threading.Thread] = None
# 中文：选主互斥锁，避免并发触发多轮 election。
_ELECTION_LOCK = threading.Lock()


# 中文：保存复制、心跳、接管过程中的观测数据，供 /debug/replication 查询。
_RUNTIME: Dict[str, Any] = {
    "started_ts": time.time(),
    "last_heartbeat_sent_at": "",
    "last_heartbeat_success_at": "",
    "last_snapshot_sent_at": "",
    "last_snapshot_success_at": "",
    "last_error": "",
    "last_error_at": "",
    "last_leader_heartbeat_at": "",
    "last_leader_heartbeat_from": "",
    "last_leader_heartbeat_epoch": 0,
    "last_leader_heartbeat_lamport": 0,
    "last_leader_heartbeat_ts": 0.0,
    "last_sync_applied_at": "",
    "last_sync_source": "",
    "last_sync_reason": "",
    "last_sync_lamport": 0,
    "last_takeover_at": "",
    "last_takeover_reason": "",
    "last_takeover_result": "",
    "last_takeover_ts": 0.0,
    "last_takeover_detail": {},
    "election_in_progress": False,
}


# 中文：统一 UTC 时间格式，便于日志和 debug 输出对齐。
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# 中文：更新复制运行时状态字段。
def _update_runtime(**fields: Any) -> None:
    with _RUNTIME_LOCK:
        _RUNTIME.update(fields)


# 中文：记录运行异常，避免静默失败。
def _record_error(msg: str) -> None:
    _update_runtime(last_error=str(msg), last_error_at=_now_iso())


# 中文：内部 HTTP JSON POST 工具，供 heartbeat/replicate/election 使用。
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


# 中文：组合 peer URL，优先 cluster 配置，并兼容旧版 META_FOLLOWER_URLS。
def _peer_urls() -> List[str]:
    urls: List[str] = []
    seen = set()

    for url in get_meta_peer_urls():
        clean = str(url).strip().rstrip("/")
        if clean and clean not in seen:
            seen.add(clean)
            urls.append(clean)

    for url in META_FOLLOWER_URLS:
        clean = str(url).strip().rstrip("/")
        if clean and clean not in seen:
            seen.add(clean)
            urls.append(clean)

    return urls


# 中文：清洗复制消息中的 membership 结构，兼容旧格式。
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
            # 中文：兼容旧数据（value 可能是纯状态字符串）。
            out[node_key] = {"status": str(entry)}
    return out


# 中文：构造状态快照（仅同步 membership；files/chunks 由 PostgreSQL 承载）。
def build_state_snapshot(reason: str = "manual") -> Dict[str, Any]:
    state = load_state()
    if is_writable_leader() and refresh_storage_membership(state):
        persist_state(state)

    lamport = tick_lamport(event="build_replicate_state")
    return {
        "source_node_id": META_NODE_ID,
        "leader_id": get_current_leader_id() or META_NODE_ID,
        "leader_epoch": get_leader_epoch(),
        "lamport": lamport,
        "generated_at": _now_iso(),
        "reason": reason,
        "membership": get_membership_snapshot(state),
    }


# 中文：leader 向 peers 推送 replicate_state；失败只记录，不阻断主链路。
def push_state_to_followers(reason: str = "manual") -> Dict[str, Any]:
    if not is_writable_leader():
        return {"status": "skipped", "reason": "node is not writable leader", "attempted": 0, "succeeded": 0, "failed": []}

    peers = _peer_urls()
    if not peers:
        return {"status": "skipped", "reason": "no peer configured", "attempted": 0, "succeeded": 0, "failed": []}

    snapshot = build_state_snapshot(reason=reason)
    _update_runtime(last_snapshot_sent_at=_now_iso())

    failed: List[Dict[str, str]] = []
    success_count = 0
    for peer_base in peers:
        try:
            resp = _post_json(f"{peer_base}/internal/replicate_state", snapshot)
            tick_lamport(event="recv_replicate_ack", incoming_lamport=int(resp.get("lamport", 0)))
            success_count += 1
            _update_runtime(last_snapshot_success_at=_now_iso())
        except (HTTPError, URLError, OSError, RuntimeError) as exc:
            failed.append({"peer": peer_base, "error": str(exc)})
            _record_error(f"replicate_state failed to {peer_base}: {exc}")

    return {
        "status": "ok",
        "reason": reason,
        "attempted": len(peers),
        "succeeded": success_count,
        "failed": failed,
    }


# 中文：记录 leader 心跳并按 epoch 执行降级（fencing）。
def record_leader_heartbeat(leader_id: str, leader_epoch: int, lamport: int) -> Dict[str, Any]:
    observed_at = _now_iso()
    tick_lamport(event="recv_leader_heartbeat", incoming_lamport=max(0, int(lamport)))
    observe_result = observe_leader(
        leader_id=str(leader_id).strip(),
        leader_epoch=max(0, int(leader_epoch)),
        reason="leader_heartbeat",
    )

    # 中文：只有“未被忽略”的心跳才刷新超时计时器，避免旧 epoch 心跳干扰 takeover。
    if not bool(observe_result.get("ignored", False)):
        _update_runtime(
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


# 中文：应用 leader 下发的复制状态，并基于 Lamport 拒绝旧消息覆盖新状态。
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
            "applied_at": _now_iso(),
            "changed": False,
            "detail": f"stale epoch replicate_state ignored: incoming_epoch={leader_epoch}",
            "lamport": get_lamport_clock(),
        }

    last_applied = get_last_applied_lamport()
    if incoming_lamport <= last_applied:
        _update_runtime(
            last_sync_source=source_node_id,
            last_sync_reason=f"ignored_stale_lamport:{reason}",
            last_sync_lamport=incoming_lamport,
        )
        return {
            "status": "ignored",
            "applied_at": _now_iso(),
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
        # 中文：follower 直接应用 leader 快照，保持 warm standby。
        state["membership"] = copy.deepcopy(membership)
        changed_holder["changed"] = True
        return True

    mutate_state(_mutator)
    mark_last_applied_lamport(incoming_lamport, reason=f"replicate_state:{reason}")

    applied_at = _now_iso()
    _update_runtime(
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


# 中文：leader 定期向 peers 发送 heartbeat，附带 leader_epoch 与 Lamport。
def _send_heartbeat_to_peers() -> None:
    if not is_writable_leader():
        return

    peers = _peer_urls()
    if not peers:
        return

    sent_at = _now_iso()
    _update_runtime(last_heartbeat_sent_at=sent_at)
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
            # 中文：利用 heartbeat ack 做反向 fencing 校验，发现更高 epoch 立刻降级。
            ack_leader_id = str(resp.get("current_leader_id", "")).strip()
            ack_leader_epoch = max(0, int(resp.get("leader_epoch", 0)))
            if ack_leader_id and ack_leader_id != META_NODE_ID:
                observe_leader(
                    leader_id=ack_leader_id,
                    leader_epoch=ack_leader_epoch,
                    reason="heartbeat_ack_fencing",
                )
            _update_runtime(last_heartbeat_success_at=_now_iso())
        except (HTTPError, URLError, OSError, RuntimeError) as exc:
            _record_error(f"leader heartbeat failed to {peer_base}: {exc}")


# 中文：执行一次 takeover 流程（超时触发或内部预抢占触发）。
def trigger_takeover(reason: str) -> Dict[str, Any]:
    acquired = _ELECTION_LOCK.acquire(blocking=False)
    if not acquired:
        return {"status": "skipped", "reason": "election already in progress"}

    _update_runtime(election_in_progress=True)
    try:
        if is_writable_leader():
            return {"status": "skipped", "reason": "node is already writable leader"}

        result = trigger_election(reason=reason)
        _update_runtime(
            last_takeover_at=_now_iso(),
            last_takeover_reason=reason,
            last_takeover_result=str(result.get("status", "unknown")),
            last_takeover_ts=time.time(),
            last_takeover_detail=copy.deepcopy(result),
        )

        # 中文：当选后立即推送一次状态，尽快收敛 follower 视图。
        if is_writable_leader():
            push_state_to_followers(reason="takeover_elected")
        return result
    except Exception as exc:  # pragma: no cover - 防御性兜底
        _record_error(f"trigger takeover failed: {exc}")
        return {"status": "error", "reason": reason, "error": str(exc)}
    finally:
        _update_runtime(election_in_progress=False)
        _ELECTION_LOCK.release()


# 中文：异步触发 takeover，避免在 API 线程内阻塞。
def trigger_takeover_async(reason: str) -> None:
    def _runner() -> None:
        trigger_takeover(reason=reason)

    thread = threading.Thread(target=_runner, name="meta-takeover", daemon=True)
    thread.start()


# 中文：follower/candidate 周期检查 leader 心跳超时，超时后触发 election。
def _maybe_takeover_by_timeout() -> None:
    runtime_snapshot = get_runtime_snapshot()
    role = str(runtime_snapshot.get("role", "follower"))
    if role == "leader":
        return

    with _RUNTIME_LOCK:
        last_ts = float(_RUNTIME.get("last_leader_heartbeat_ts", 0.0) or 0.0)
        started_ts = float(_RUNTIME.get("started_ts", time.time()))
        last_takeover_ts = float(_RUNTIME.get("last_takeover_ts", 0.0) or 0.0)

    reference_ts = last_ts if last_ts > 0 else started_ts
    elapsed = max(0.0, time.time() - reference_ts)
    if elapsed <= META_LEADER_HEARTBEAT_TIMEOUT_SEC:
        return

    # 中文：简单冷却窗口，避免连续 timeout 触发过于频繁。
    if last_takeover_ts > 0 and (time.time() - last_takeover_ts) < 1.5:
        return

    trigger_takeover(reason=f"leader_timeout_{round(elapsed, 3)}s")


# 中文：统一后台循环：leader 负责发送 heartbeat+sync；follower 负责超时接管。
def _replication_runtime_loop() -> None:
    hb_interval = max(0.5, float(META_HEARTBEAT_INTERVAL_SEC))
    sync_interval = max(hb_interval, float(META_SYNC_INTERVAL_SEC))
    next_sync_monotonic = 0.0

    while not _STOP_EVENT.is_set():
        if is_writable_leader():
            _send_heartbeat_to_peers()
            now_monotonic = time.monotonic()
            if now_monotonic >= next_sync_monotonic:
                push_state_to_followers(reason="periodic_sync")
                next_sync_monotonic = now_monotonic + sync_interval
        else:
            _maybe_takeover_by_timeout()

        _STOP_EVENT.wait(hb_interval)


# 中文：启动复制/接管运行时线程。
def start_replication_runtime() -> None:
    global _RUNTIME_THREAD
    with _RUNTIME_LOCK:
        if _RUNTIME_THREAD is not None and _RUNTIME_THREAD.is_alive():
            return
        _STOP_EVENT.clear()
        _RUNTIME["started_ts"] = time.time()
        _RUNTIME_THREAD = threading.Thread(target=_replication_runtime_loop, name="meta-replication-runtime", daemon=True)
        _RUNTIME_THREAD.start()


# 中文：停止复制/接管运行时线程。
def stop_replication_runtime() -> None:
    global _RUNTIME_THREAD
    _STOP_EVENT.set()
    if _RUNTIME_THREAD is not None and _RUNTIME_THREAD.is_alive():
        _RUNTIME_THREAD.join(timeout=2.0)
    _RUNTIME_THREAD = None


# 中文：输出复制与接管状态，供 debug API 观测真实 leader/runtime 信息。
def get_replication_status() -> Dict[str, Any]:
    with _RUNTIME_LOCK:
        data = copy.deepcopy(_RUNTIME)
        thread_alive = _RUNTIME_THREAD.is_alive() if _RUNTIME_THREAD is not None else False

    last_ts = float(data.get("last_leader_heartbeat_ts", 0.0) or 0.0)
    elapsed_sec = max(0.0, time.time() - last_ts) if last_ts > 0 else None
    leader_alive = (elapsed_sec is not None) and (elapsed_sec <= META_LEADER_HEARTBEAT_TIMEOUT_SEC)
    runtime_snapshot = get_runtime_snapshot()

    return {
        "node_id": META_NODE_ID,
        "runtime": runtime_snapshot,
        "runtime_thread_alive": thread_alive,
        "peer_urls": _peer_urls(),
        "legacy_leader_url": META_LEADER_URL,
        "leader_heartbeat": {
            "source_node_id": data.get("last_leader_heartbeat_from", ""),
            "leader_epoch": int(data.get("last_leader_heartbeat_epoch", 0)),
            "leader_lamport": int(data.get("last_leader_heartbeat_lamport", 0)),
            "last_observed_at": data.get("last_leader_heartbeat_at", ""),
            "elapsed_sec": round(elapsed_sec, 3) if elapsed_sec is not None else None,
            "timeout_sec": META_LEADER_HEARTBEAT_TIMEOUT_SEC,
            "alive": bool(leader_alive),
        },
        "replication": {
            "last_heartbeat_sent_at": data.get("last_heartbeat_sent_at", ""),
            "last_heartbeat_success_at": data.get("last_heartbeat_success_at", ""),
            "last_snapshot_sent_at": data.get("last_snapshot_sent_at", ""),
            "last_snapshot_success_at": data.get("last_snapshot_success_at", ""),
            "last_sync_applied_at": data.get("last_sync_applied_at", ""),
            "last_sync_source": data.get("last_sync_source", ""),
            "last_sync_reason": data.get("last_sync_reason", ""),
            "last_sync_lamport": int(data.get("last_sync_lamport", 0)),
            "last_applied_lamport": int(runtime_snapshot.get("last_applied_lamport", 0)),
        },
        "takeover": {
            "last_takeover_at": data.get("last_takeover_at", ""),
            "last_takeover_reason": data.get("last_takeover_reason", ""),
            "last_takeover_result": data.get("last_takeover_result", ""),
            "last_takeover_detail": data.get("last_takeover_detail", {}),
            "election_in_progress": bool(data.get("election_in_progress", False)),
        },
        "error": {
            "last_error": data.get("last_error", ""),
            "last_error_at": data.get("last_error_at", ""),
        },
    }
