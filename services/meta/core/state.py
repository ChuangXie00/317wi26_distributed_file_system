import json
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.error import URLError
from urllib.request import urlopen

from .config import (
    DATA_DIR,
    METADATA_FILE,
    ENABLE_STORAGE_HEALTHCHECK,
    HEARTBEAT_TIMEOUT_SEC,
    STORAGE_HEALTHCHECK_TIMEOUT_SEC,
    STORAGE_NODES,
    STORAGE_PORT,
)

State = Dict[str, Any]
MembershipEntry = Dict[str, Any]

def _now_timestamp() -> float:
    return time.time()

def _timestamp_to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")

def _normalize_status(raw_status: str) -> str:
    normalized = str(raw_status or "").strip().lower()
    if normalized in {"alive", "dead", "suspected"}:
        return normalized
    return "dead"

def _new_membership_entry(now_ts: float, status: str = "alive") -> MembershipEntry:
    return {
        "status": _normalize_status(status),
        "last_heartbeat_ts": float(now_ts),
        "last_heartbeat_at": _timestamp_to_iso(float(now_ts)),
    }

def _coerce_membership_entry(raw: Any, now_ts: float) -> MembershipEntry:
    # compatible with old format in 0.1p02(membership may be str)
    if isinstance(raw, str):
        return _new_membership_entry(now_ts, status = raw)
    
    if isinstance(raw, dict):
        status = _normalize_status(raw.get("status", "alive"))
        ts_raw = raw.get("last_heartbeat_ts")
        if isinstance(ts_raw, (int, float)):
            hb_ts = float(ts_raw)
        else:
            hb_ts = float(now_ts)
        
        hb_at = raw.get("last_heartbeat_at")
        if not hb_at:
            hb_at = _timestamp_to_iso(hb_ts)

        return {
            "status": status,
            "last_heartbeat_ts": hb_ts,
            "last_heartbeat_at": str(hb_at),
        }
    
    return _new_membership_entry(now_ts, status="alive")

# check if the metadata file exists
def ensure_metadata_file() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not METADATA_FILE.exists():
        init_content = {"files": {}, "chunks": {}, "membership": {}, "version": 1}
        METADATA_FILE.write_text(json.dumps(init_content, indent=2), encoding="utf-8")

# load state from metadata file
def load_state() -> State:
    ensure_metadata_file()
    try:
        return json.loads(METADATA_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to load metadata.json: {exc}") from exc
    
# save
def persist_state(state: State) -> None:
    ensure_metadata_file()
    temp_path = METADATA_FILE.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    temp_path.replace(METADATA_FILE)


def ensure_membership_schema(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = False

    membership_raw = state.setdefault("membership", {})
    if not isinstance(membership_raw, dict):
        state["membership"] = {}
        membership_raw = state["membership"]
        changed = True

    # normalize current str(compatible with old format)
    for node_id in list(membership_raw.keys()):
        normalized = _coerce_membership_entry(membership_raw[node_id], now_ts)
        if membership_raw[node_id] != normalized:
            membership_raw[node_id] = normalized
            changed = True

    for node_id in STORAGE_NODES:
        if node_id not in membership_raw:
            membership_raw[node_id] = _new_membership_entry(now_ts, status="alive")
            changed = True
    
    return changed

def mark_storage_heartbeat(state: State, node_id: str, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = ensure_membership_schema(state, now_ts=now_ts)
    membership = state.setdefault("membership", {})

    new_entry = _new_membership_entry(now_ts, status="alive")
    old_entry = _coerce_membership_entry(membership.get(node_id), now_ts)
    if old_entry != new_entry:
        changed = True
    membership[node_id] = new_entry
    return changed


def apply_storage_heartbeat_timeout(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = ensure_membership_schema(state, now_ts=now_ts)
    membership = state.setdefault("membership", {})

    for node_id in STORAGE_NODES:
        entry = _coerce_membership_entry(membership.get(node_id), now_ts)
        elapsed = now_ts - float(entry.get("last_heartbeat_ts", 0.0))

        # alive/suspected 节点超时后统一标记 dead
        if entry["status"] in {"alive", "suspected"} and elapsed > HEARTBEAT_TIMEOUT_SEC:
            entry["status"] = "dead"
            changed = True

        membership[node_id] = entry

    return changed


def refresh_storage_membership(state: State, now_ts: Optional[float] = None) -> bool:
    now_ts = _now_timestamp() if now_ts is None else float(now_ts)
    changed = ensure_membership_schema(state, now_ts=now_ts)
    if apply_storage_heartbeat_timeout(state, now_ts=now_ts):
        changed = True
    return changed


def get_membership_snapshot(state: State) -> Dict[str, MembershipEntry]:
    now_ts = _now_timestamp()
    ensure_membership_schema(state, now_ts=now_ts)
    membership = state.get("membership", {})

    out: Dict[str, MembershipEntry] = {}
    for node_id in sorted(membership.keys()):
        out[node_id] = _coerce_membership_entry(membership.get(node_id), now_ts)
    return out

# lightweight health check for storage nodes
# used in 0.1_phase02
def _storage_health_url(node_id: str) -> str:
    return f"http://{node_id}:{STORAGE_PORT}/health"

def _is_storage_alive(node_id: str) -> bool:
    try:
        with urlopen(_storage_health_url(node_id), timeout=STORAGE_HEALTHCHECK_TIMEOUT_SEC) as response:
            return response.status == 200
    except (URLError, TimeoutError, OSError):
        return False
    
def get_alive_storage_nodes(state: State) -> List[str]:
    # 0.1p03 use membership status to determine alive nodes
    membership = get_membership_snapshot(state)
    alive_nodes = [node_id for node_id in STORAGE_NODES if membership.get(node_id, {}).get("status") == "alive"]

    # keep 0.1p02 behavior if health check is disabled
    if not ENABLE_STORAGE_HEALTHCHECK:
        return alive_nodes

    return [node_id for node_id in alive_nodes if _is_storage_alive(node_id)]

# choose n nodes as replicas from alive nodes
def choose_replicas(alive_nodes: List[str], replica_count: int) -> List[str]:
    # select n nodes from alive_nodes, if not enough, raise error
    if replica_count <= 0:
        return []
    if len(alive_nodes) < replica_count:
        raise ValueError("not enough replicas available")
    return random.sample(alive_nodes, replica_count)