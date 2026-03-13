from typing import List

from ..config import META_NODE_ID, get_meta_peer_nodes


# 规范化节点 ID，避免大小写/空白导致优先级比较不一致。
def normalize_node_id(raw_node_id: str) -> str:
    return str(raw_node_id or "").strip().lower()


# 返回当前可识别的 meta 节点集合（含自身），用于过滤未知节点的内部协议请求。
def known_meta_nodes() -> List[str]:
    return sorted(set(get_meta_peer_nodes() + [META_NODE_ID]))


# 计算当前集群所需法定票数（quorum），规则为 floor(N/2)+1。
def quorum_required(total_nodes: int) -> int:
    normalized_total = max(1, int(total_nodes))
    return (normalized_total // 2) + 1
