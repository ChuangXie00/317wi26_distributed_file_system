from typing import Any, Callable, Dict

from ..config import LEADER_ELECTION_MODE
from .bully_strategy import BullyElectionStrategy
from .common import known_meta_nodes, normalize_node_id, quorum_required
from .coordinator import broadcast_coordinator, handle_incoming_coordinator
from .protocol import ElectionStrategy
from .quorum_strategy import QuorumElectionStrategy
from .transport import post_json


# 兼容旧模块的私有工具函数命名，避免外部隐式引用在重构后失效。
_post_json = post_json
_normalize_node_id = normalize_node_id
_known_meta_nodes = known_meta_nodes
_quorum_required = quorum_required


# 策略工厂映射，统一管理不同选主模式对应的策略实现。
_ELECTION_STRATEGY_FACTORIES: Dict[str, Callable[[], ElectionStrategy]] = {
    "bully": BullyElectionStrategy,
    "quorum": QuorumElectionStrategy,
}


# 根据配置获取选主策略实例；具体模式能力在各策略内自行约束。
def get_election_strategy() -> ElectionStrategy:
    strategy_factory = _ELECTION_STRATEGY_FACTORIES.get(LEADER_ELECTION_MODE)
    if strategy_factory is None:
        raise RuntimeError(f"unsupported LEADER_ELECTION_MODE={LEADER_ELECTION_MODE!r}")
    return strategy_factory()


# 对外统一入口，触发一次选主流程。
def trigger_election(reason: str) -> Dict[str, Any]:
    strategy = get_election_strategy()
    return strategy.trigger_election(reason=reason)


# 处理收到的 election 请求；返回 ok 与是否建议本节点启动本地 election。
def handle_incoming_election(candidate_id: str, candidate_epoch: int, lamport: int, reason: str) -> Dict[str, Any]:
    strategy = get_election_strategy()
    return strategy.handle_incoming_election(
        candidate_id=candidate_id,
        candidate_epoch=candidate_epoch,
        lamport=lamport,
        reason=reason,
    )


# 处理收到的 quorum vote 请求；用于 candidate 统计多数票结果。
def handle_incoming_vote_request(
    candidate_id: str,
    candidate_term: int,
    candidate_epoch: int,
    lamport: int,
    reason: str,
) -> Dict[str, Any]:
    strategy = get_election_strategy()
    return strategy.handle_incoming_vote_request(
        candidate_id=candidate_id,
        candidate_term=candidate_term,
        candidate_epoch=candidate_epoch,
        lamport=lamport,
        reason=reason,
    )


__all__ = [
    "ElectionStrategy",
    "BullyElectionStrategy",
    "QuorumElectionStrategy",
    "get_election_strategy",
    "trigger_election",
    "broadcast_coordinator",
    "handle_incoming_election",
    "handle_incoming_vote_request",
    "handle_incoming_coordinator",
]
