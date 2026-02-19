import json
import random
from typing import Any, Dict, List
from urllib.error import URLError
from urllib.request import urlopen

from .config import (
    DATA_DIR,
    METADATA_FILE,
    ENABLE_STORAGE_HEALTHCHECK,
    STORAGE_HEALTHCHECK_TIMEOUT_SEC,
    STORAGE_NODES,
    STORAGE_PORT,
)

State = Dict[str, Any]

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
    # Phase02
    # membership is not implemented yet
    # configure + health check
    membership = state.get("membership") or {}
    if membership:
        configured = [node for node in STORAGE_NODES if membership.get(node, "alive") == "alive"]
    else:
        configured = STORAGE_NODES[:]

    if not ENABLE_STORAGE_HEALTHCHECK:
        return configured

    return [node for node in configured if _is_storage_alive(node)]

def choose_replicas(alive_nodes: List[str], replica_count: int) -> List[str]:
    # select n nodes from alive_nodes, if not enough, raise error
    if replica_count <= 0:
        return []
    if len(alive_nodes) < replica_count:
        raise ValueError("not enough replicas available")
    return random.sample(alive_nodes, replica_count)