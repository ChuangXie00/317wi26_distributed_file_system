import os
from pathlib import Path
from typing import Any, Dict, List


# 文件作用：集中管理 meta 运行参数、集群成员配置与节点间 peer 发现规则。

# 读取 CSV 环境变量，并清理空白项。
def _parse_csv_env(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


# 读取布尔环境变量（支持 1/true/yes/on）。
def _parse_bool_env(name: str, default: str) -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in {"1", "true", "yes", "on"}


# 读取整型环境变量。
def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    return int(raw)


# 读取浮点型环境变量。
def _parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    return float(raw)


# 规范化节点 ID（去空白并统一小写），避免大小写或空白导致 peer 识别不一致。
def _normalize_meta_node_id(raw: str) -> str:
    return str(raw).strip().lower()


# 对字符串列表去重并保持原始顺序，避免重复节点导致重复广播。
def _dedupe_keep_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


# 规范化 URL 列表（去尾斜杠 + 去重），统一 legacy peer URL 表达。
def _normalize_base_urls(urls: List[str]) -> List[str]:
    normalized: List[str] = []
    for raw in urls:
        clean = str(raw).strip().rstrip("/")
        if not clean:
            continue
        normalized.append(clean)
    return _dedupe_keep_order(normalized)


# 当前版本允许的选主模式，后续可在此处集中扩展。
SUPPORTED_LEADER_ELECTION_MODES = {"bully", "quorum"}


# 校验选主模式是否合法；非法模式在启动阶段直接 fail-fast。
def _validate_leader_election_mode(raw_mode: str) -> str:
    normalized_mode = str(raw_mode or "").strip().lower() or "bully"
    if normalized_mode not in SUPPORTED_LEADER_ELECTION_MODES:
        supported = ", ".join(sorted(SUPPORTED_LEADER_ELECTION_MODES))
        raise RuntimeError(
            f"unsupported LEADER_ELECTION_MODE={normalized_mode!r}, supported modes: {supported}"
        )
    return normalized_mode


# quorum 模式预检查，防止少于 3 节点时误启动造成不可达多数票。
def _validate_quorum_cluster_precheck(election_mode: str, cluster_nodes: List[str]) -> None:
    if election_mode != "quorum":
        return
    if len(cluster_nodes) < 3:
        raise RuntimeError(
            "quorum election requires at least 3 meta nodes, "
            f"got {len(cluster_nodes)} from META_CLUSTER_NODES={cluster_nodes!r}"
        )


# 当前 meta 节点 ID（优先读取 META_NODE_ID，兼容旧 NODE_ID）。
META_NODE_ID = _normalize_meta_node_id(os.getenv("META_NODE_ID", os.getenv("NODE_ID", "meta-01")))
if not META_NODE_ID:
    META_NODE_ID = "meta-01"
# 启动时的初始角色，仅用于 bootstrap，后续以运行时状态为准。
META_BOOTSTRAP_ROLE = os.getenv("META_ROLE", os.getenv("ROLE", "leader")).strip().lower() or "leader"
if META_BOOTSTRAP_ROLE not in {"leader", "follower"}:
    META_BOOTSTRAP_ROLE = "follower"

# 选主模式配置，当前支持 bully / quorum 两种模式。
LEADER_ELECTION_MODE = _validate_leader_election_mode(os.getenv("LEADER_ELECTION_MODE", "bully"))

# meta 集群节点列表（用于 election / heartbeat / replicate 广播）。
_META_CLUSTER_NODES_RAW = [_normalize_meta_node_id(node_id) for node_id in _parse_csv_env("META_CLUSTER_NODES", "meta-01,meta-02")]
META_CLUSTER_NODES = _dedupe_keep_order([node_id for node_id in _META_CLUSTER_NODES_RAW if node_id])
if not META_CLUSTER_NODES:
    META_CLUSTER_NODES = ["meta-01"]
if META_NODE_ID not in META_CLUSTER_NODES:
    META_CLUSTER_NODES.append(META_NODE_ID)
# 当前节点感知到的 meta 集群规模，供启动校验与调试复用。
META_CLUSTER_SIZE = len(META_CLUSTER_NODES)
# 在配置加载阶段执行 quorum 最小规模校验。
_validate_quorum_cluster_precheck(LEADER_ELECTION_MODE, META_CLUSTER_NODES)
# meta 节点内部 HTTP 端口（容器内服务端口）。
META_INTERNAL_PORT = _parse_int_env("META_INTERNAL_PORT", 8000)


# 构造指定 meta 节点的容器内访问地址。
def build_meta_base_url(node_id: str) -> str:
    return f"http://{str(node_id).strip()}:{META_INTERNAL_PORT}"


# 返回除当前节点外的 peer 节点 ID。
def get_meta_peer_nodes() -> List[str]:
    # 统一按字典序输出，保证广播顺序和 debug 观测稳定可复现。
    peers = [node_id for node_id in META_CLUSTER_NODES if node_id != META_NODE_ID]
    return sorted(peers)


# 返回除当前节点外的 peer 节点 base URL。
def get_meta_peer_urls() -> List[str]:
    return [build_meta_base_url(node_id) for node_id in get_meta_peer_nodes()]


# 副本数量与 storage 节点配置。
REPLICATION_FACTOR = _parse_int_env("REPLICATION_FACTOR", 1)
STORAGE_NODES = _parse_csv_env("STORAGE_NODES", "storage-01")
DEFAULT_NAMESPACE = os.getenv("DEFAULT_NAMESPACE", "default")

# Phase2 storage 存活探测配置。
STORAGE_PORT = _parse_int_env("STORAGE_PORT", 9009)
STORAGE_HEALTHCHECK_TIMEOUT_SEC = _parse_float_env("STORAGE_HEALTHCHECK_TIMEOUT_SEC", 0.2)
ENABLE_STORAGE_HEALTHCHECK = _parse_bool_env("ENABLE_STORAGE_HEALTHCHECK", "0")

# Phase3 membership 心跳与超时配置。
HEARTBEAT_INTERVAL_SEC = _parse_float_env("HEARTBEAT_INTERVAL_SEC", 3.0)
HEARTBEAT_TIMEOUT_SEC = _parse_float_env("HEARTBEAT_TIMEOUT_SEC", 9.0)
MEMBERSHIP_SWEEP_INTERVAL_SEC = _parse_float_env("MEMBERSHIP_SWEEP_INTERVAL_SEC", 1.0)
HEARTBEAT_WRITE_MIN_INTERVAL_SEC = _parse_float_env("HEARTBEAT_WRITE_MIN_INTERVAL_SEC", 1.0)

# metadata.json 路径（membership 运行态持久化）。
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
METADATA_FILE = DATA_DIR / os.getenv("METADATA_FILE", "metadata.json")

# 0.1p3.1 PostgreSQL 连接参数（元数据持久层）。
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = _parse_int_env("PG_PORT", 5432)
PG_DATABASE = os.getenv("PG_DATABASE", "dfs_meta")
PG_USER = os.getenv("PG_USER", "dfs_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "dfs_pass")
PG_SSLMODE = os.getenv("PG_SSLMODE", "disable")
PG_CONNECT_TIMEOUT_SEC = _parse_int_env("PG_CONNECT_TIMEOUT_SEC", 5)
PG_STATEMENT_TIMEOUT_MS = _parse_int_env("PG_STATEMENT_TIMEOUT_MS", 8000)

# 0.1p04 历史配置兼容项；p5 主要以 META_CLUSTER_NODES 为准。
META_FOLLOWER_URLS = _normalize_base_urls(_parse_csv_env("META_FOLLOWER_URLS", ""))
META_LEADER_URL = os.getenv("META_LEADER_URL", "http://meta-01:8000").strip().rstrip("/")
META_INTERNAL_TIMEOUT_SEC = _parse_float_env("META_INTERNAL_TIMEOUT_SEC", 2.0)
META_HEARTBEAT_INTERVAL_SEC = _parse_float_env("META_HEARTBEAT_INTERVAL_SEC", 3.0)
META_SYNC_INTERVAL_SEC = _parse_float_env("META_SYNC_INTERVAL_SEC", 5.0)
META_LEADER_HEARTBEAT_TIMEOUT_SEC = _parse_float_env("META_LEADER_HEARTBEAT_TIMEOUT_SEC", 9.0)
# 0.2p01: background chunk re-replication settings (minimal self-heal path).
REREPLICATION_ENABLE = _parse_bool_env("REREPLICATION_ENABLE", "1")
REREPLICATION_DEAD_GRACE_SEC = max(0.0, _parse_float_env("REREPLICATION_DEAD_GRACE_SEC", 15.0))
REREPLICATION_SCAN_INTERVAL_SEC = max(1.0, _parse_float_env("REREPLICATION_SCAN_INTERVAL_SEC", 3.0))
REREPLICATION_SCAN_CHUNK_LIMIT = max(1, _parse_int_env("REREPLICATION_SCAN_CHUNK_LIMIT", 5000))
REREPLICATION_BATCH_LIMIT = max(1, _parse_int_env("REREPLICATION_BATCH_LIMIT", 100))
REREPLICATION_MAX_CONCURRENCY = max(1, _parse_int_env("REREPLICATION_MAX_CONCURRENCY", 1))
REREPLICATION_RETRY_MAX = max(1, _parse_int_env("REREPLICATION_RETRY_MAX", 6))
REREPLICATION_RETRY_BACKOFF_SEC = max(0.1, _parse_float_env("REREPLICATION_RETRY_BACKOFF_SEC", 1.0))
REREPLICATION_RETRY_BACKOFF_MAX_SEC = max(
    1.0,
    _parse_float_env("REREPLICATION_RETRY_BACKOFF_MAX_SEC", 30.0),
)
REREPLICATION_HTTP_TIMEOUT_SEC = max(0.2, _parse_float_env("REREPLICATION_HTTP_TIMEOUT_SEC", 2.0))
# 重入节点发起本地选举前的冷却秒数，默认采用“心跳超时 + 心跳间隔”。
META_REJOIN_ELECTION_HOLDOFF_SEC = max(
    0.0,
    _parse_float_env(
        "META_REJOIN_ELECTION_HOLDOFF_SEC",
        float(META_LEADER_HEARTBEAT_TIMEOUT_SEC + META_HEARTBEAT_INTERVAL_SEC),
    ),
)


# 构造 PostgreSQL 连接参数，供 repository 工厂统一使用。
def build_pg_conn_kwargs() -> Dict[str, Any]:
    return {
        "host": PG_HOST,
        "port": PG_PORT,
        "dbname": PG_DATABASE,
        "user": PG_USER,
        "password": PG_PASSWORD,
        "connect_timeout": PG_CONNECT_TIMEOUT_SEC,
        "sslmode": PG_SSLMODE,
        # 为每条连接注入 statement_timeout，避免 SQL 长时间阻塞。
        "options": f"-c statement_timeout={PG_STATEMENT_TIMEOUT_MS}",
    }
