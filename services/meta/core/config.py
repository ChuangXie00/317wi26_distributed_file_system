import os
from pathlib import Path
from typing import Optional, List

# read storage nodes from env, spilt by comma
def _parse_csv_env(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]

def _parse_bool_env(name: str, default: str) -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in {"1", "true", "yes", "no"}

# role of this meta node
META_NODE_ID = os.getenv("META_NODE_ID", os.getenv("NODE_ID", "meta-01"))
ROLE = os.getenv("META_ROLE", os.getenv("ROLE", "leader"))

# num of replicas
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "1"))
STORAGE_NODES = _parse_csv_env("STORAGE_NODES", "storage-01")

# storage node health check
STORAGE_PORT = int(os.getenv("STORAGE_PORT", "9009"))
STORAGE_HEALTHCHECK_TIMEOUT_SEC = float(os.getenv("STORAGE_HEALTHCHECK_TIMEOUT_SEC", "0.5"))
ENABLE_STORAGE_HEALTHCHECK = _parse_bool_env("ENABLE_STORAGE_HEALTHCHECK", "0")

# metadata.json file path
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
METADATA_FILE = DATA_DIR / os.getenv("METADATA_FILE", "metadata.json")