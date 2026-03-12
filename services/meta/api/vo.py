from typing import Any, Dict, List, Literal

from pydantic import BaseModel, Field


# chunk 去重查询请求体。
class ChunkCheckReq(BaseModel):
    fingerprint: str = Field(..., min_length=1)


# chunk 去重查询响应体。
class ChunkCheckResp(BaseModel):
    exists: bool
    locations: List[str]


# chunk 注册请求体。
class ChunkRegisterReq(BaseModel):
    fingerprint: str = Field(..., min_length=1)


# chunk 注册响应体（保留旧字段兼容历史 client）。
class ChunkRegisterResp(BaseModel):
    assigned_nodes: List[str]
    assigned_node: List[str] = Field(default_factory=list)


# 文件提交请求体。
class FileCommitReq(BaseModel):
    file_name: str = Field(..., min_length=1)
    chunks: List[str] = Field(..., min_length=1)


# 文件提交响应体。
class FileCommitResp(BaseModel):
    status: Literal["ok", "error"]


# 文件查询中的单个 chunk 条目。
class FileGetItem(BaseModel):
    fingerprint: str
    locations: List[str]


# 文件查询响应体。
class FileGetResp(BaseModel):
    chunks: List[FileGetItem]


# storage -> meta 心跳请求体。
class StorageHeartbeatReq(BaseModel):
    node_id: str = Field(..., min_length=1)


# storage -> meta 心跳响应体。
class StorageHeartbeatResp(BaseModel):
    status: Literal["alive"]
    node_id: str
    observed_at: str


# leader -> follower 心跳请求体（含 epoch 与 Lamport）。
class LeaderHeartbeatReq(BaseModel):
    leader_id: str = Field(..., min_length=1)
    leader_epoch: int = Field(default=0, ge=0)
    lamport: int = Field(default=0, ge=0)
    sent_at: str = ""


# leader -> follower 心跳响应体。
class LeaderHeartbeatResp(BaseModel):
    status: Literal["alive"]
    follower_id: str
    observed_at: str
    role: str
    current_leader_id: str
    leader_epoch: int = Field(default=0, ge=0)
    lamport: int = Field(default=0, ge=0)


# leader -> follower 状态复制请求体（含 epoch 与 Lamport）。
class ReplicateStateReq(BaseModel):
    source_node_id: str = Field(..., min_length=1)
    leader_id: str = Field(default="", min_length=0)
    leader_epoch: int = Field(default=0, ge=0)
    lamport: int = Field(default=0, ge=0)
    generated_at: str = ""
    reason: str = "manual"
    membership: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


# 状态复制响应体（可返回 synced 或 ignored）。
class ReplicateStateResp(BaseModel):
    status: Literal["synced", "ignored"]
    follower_id: str
    applied_at: str
    detail: str = ""
    lamport: int = Field(default=0, ge=0)


# Bully election 请求体。
class ElectionReq(BaseModel):
    candidate_id: str = Field(..., min_length=1)
    candidate_epoch: int = Field(default=0, ge=0)
    lamport: int = Field(default=0, ge=0)
    reason: str = "manual"


# Bully election 响应体（ok 表示存在更高优先级节点）。
class ElectionResp(BaseModel):
    status: Literal["ok"]
    ok: bool
    stale: bool
    should_start_local_election: bool
    responder_id: str
    responder_role: str
    responder_epoch: int = Field(default=0, ge=0)
    lamport: int = Field(default=0, ge=0)


# quorum 投票请求体（candidate 向 peer 请求投票）。
class VoteReq(BaseModel):
    candidate_id: str = Field(..., min_length=1)
    candidate_term: int = Field(default=0, ge=0)
    candidate_epoch: int = Field(default=0, ge=0)
    lamport: int = Field(default=0, ge=0)
    reason: str = "vote_request"


# quorum 投票响应体（granted=true 表示本节点已投票给该 candidate）。
class VoteResp(BaseModel):
    status: Literal["ok"]
    granted: bool
    stale: bool
    supported: bool
    responder_id: str
    responder_role: str
    responder_term: int = Field(default=0, ge=0)
    responder_epoch: int = Field(default=0, ge=0)
    voted_for: str = ""
    lamport: int = Field(default=0, ge=0)
    detail: str = ""


# coordinator 广播请求体。
class CoordinatorReq(BaseModel):
    leader_id: str = Field(..., min_length=1)
    leader_epoch: int = Field(default=0, ge=0)
    lamport: int = Field(default=0, ge=0)
    reason: str = "coordinator"


# coordinator 广播响应体。
class CoordinatorResp(BaseModel):
    status: Literal["ack"]
    changed: bool
    ignored: bool
    node_id: str
    role: str
    leader_id: str
    leader_epoch: int = Field(default=0, ge=0)
    lamport: int = Field(default=0, ge=0)
