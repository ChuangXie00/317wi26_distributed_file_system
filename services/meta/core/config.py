import os
from pathlib import Path
from typing import Optional, List, Dict, Any

# read storage nodes from env, spilt by comma
def _parse_csv_env(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]

def _parse_bool_env(name: str, default: str) -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in {"1", "true", "yes", "on"}

def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    return int(raw) 

def _parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    return float(raw)

# PostgreSQL connection params, repository layer
def build_pg_conn_kwargs() -> Dict[str, Any]:
    return {
        "host": PG_HOST,
        "port": PG_PORT,
        "dbname": PG_DATABASE,
        "user": PG_USER,
        "password": PG_PASSWORD,
        "connect_timeout": PG_CONNECT_TIMEOUT_SEC,
        "sslmode": PG_SSLMODE,
        # add statement_timeout to every connection
        "options": f"-c statement_timeout={PG_STATEMENT_TIMEOUT_MS}",
    }



# role of this meta node
META_NODE_ID = os.getenv("META_NODE_ID", os.getenv("NODE_ID", "meta-01"))
ROLE = os.getenv("META_ROLE", os.getenv("ROLE", "leader"))

# num of replicas
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "1"))
STORAGE_NODES = _parse_csv_env("STORAGE_NODES", "storage-01")
DEFAULT_NAMESPACE = os.getenv("DEFAULT_NAMESPACE", "default")

# phase 2 storage node health check
STORAGE_PORT = int(os.getenv("STORAGE_PORT", "9009"))
STORAGE_HEALTHCHECK_TIMEOUT_SEC = float(os.getenv("STORAGE_HEALTHCHECK_TIMEOUT_SEC", "0.2"))
ENABLE_STORAGE_HEALTHCHECK = _parse_bool_env("ENABLE_STORAGE_HEALTHCHECK", "0")

# phase 3 heartbeat, membership management, write throttling
HEARTBEAT_INTERVAL_SEC = _parse_float_env("HEARTBEAT_INTERVAL_SEC", 3.0)
HEARTBEAT_TIMEOUT_SEC = _parse_float_env("HEARTBEAT_TIMEOUT_SEC", 9.0)
MEMBERSHIP_SWEEP_INTERVAL_SEC = _parse_float_env("MEMBERSHIP_SWEEP_INTERVAL_SEC", 1.0)
HEARTBEAT_WRITE_MIN_INTERVAL_SEC = _parse_float_env("HEARTBEAT_WRITE_MIN_INTERVAL_SEC", 1.0)

# metadata.json file path
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
METADATA_FILE = DATA_DIR / os.getenv("METADATA_FILE", "metadata.json")

# 0.1p3.1 PostgreSQL connection params
# English: Postgres connection parameters (new durable metadata store in 3.1).
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = _parse_int_env("PG_PORT", 5432)
PG_DATABASE = os.getenv("PG_DATABASE", "dfs_meta")
PG_USER = os.getenv("PG_USER", "dfs_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "dfs_pass")
PG_SSLMODE = os.getenv("PG_SSLMODE", "disable")
PG_CONNECT_TIMEOUT_SEC = _parse_int_env("PG_CONNECT_TIMEOUT_SEC", 5)
PG_STATEMENT_TIMEOUT_MS = _parse_int_env("PG_STATEMENT_TIMEOUT_MS", 8000)

# 0.1p04: meta leader/follower 同步参数
META_FOLLOWER_URLS = _parse_csv_env("META_FOLLOWER_URLS", "")
META_LEADER_URL = os.getenv("META_LEADER_URL", "http://meta-01:8000").strip().rstrip("/")
META_INTERNAL_TIMEOUT_SEC = _parse_float_env("META_INTERNAL_TIMEOUT_SEC", 2.0)
META_HEARTBEAT_INTERVAL_SEC = _parse_float_env("META_HEARTBEAT_INTERVAL_SEC", 3.0)
META_SYNC_INTERVAL_SEC = _parse_float_env("META_SYNC_INTERVAL_SEC", 5.0)
META_LEADER_HEARTBEAT_TIMEOUT_SEC = _parse_float_env("META_LEADER_HEARTBEAT_TIMEOUT_SEC", 9.0)