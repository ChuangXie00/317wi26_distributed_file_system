from .config import META_BOOTSTRAP_ROLE, META_NODE_ID, REPLICATION_FACTOR, STORAGE_NODES
from .runtime import get_runtime_snapshot, is_writable_leader
from .state import choose_replicas, get_alive_storage_nodes, load_state, persist_state

# core 层对外导出常用配置、状态函数与运行态辅助函数。
__all__ = [
    "META_NODE_ID",
    "META_BOOTSTRAP_ROLE",
    "REPLICATION_FACTOR",
    "STORAGE_NODES",
    "choose_replicas",
    "get_alive_storage_nodes",
    "load_state",
    "persist_state",
    "get_runtime_snapshot",
    "is_writable_leader",
]
