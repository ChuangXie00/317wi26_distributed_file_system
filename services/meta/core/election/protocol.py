from typing import Any, Dict, Protocol


# 选主策略抽象接口，统一 Bully/Quorum 两种模式的核心能力。
class ElectionStrategy(Protocol):
    # 中文：触发本节点发起一轮选主流程，返回本轮执行结果。
    def trigger_election(self, reason: str) -> Dict[str, Any]:
        ...

    # 中文：处理来自其他节点的 election 请求，返回协议响应字段。
    def handle_incoming_election(
        self,
        candidate_id: str,
        candidate_epoch: int,
        lamport: int,
        reason: str,
    ) -> Dict[str, Any]:
        ...

    # 中文：处理投票请求（quorum 协议），返回是否授票及响应方状态。
    def handle_incoming_vote_request(
        self,
        candidate_id: str,
        candidate_term: int,
        candidate_epoch: int,
        lamport: int,
        reason: str,
    ) -> Dict[str, Any]:
        ...
