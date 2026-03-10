from typing import Any, Dict, List, Literal

from pydantic import BaseModel, Field


class ChunkCheckReq(BaseModel):
    fingerprint: str = Field(..., min_length=1)


class ChunkCheckResp(BaseModel):
    exists: bool
    locations: List[str]


class ChunkRegisterReq(BaseModel):
    fingerprint: str = Field(..., min_length=1)


class ChunkRegisterResp(BaseModel):
    # 新字段
    assigned_nodes: List[str]
    # 兼容 0.1p02 旧字段，后续可移除
    assigned_node: List[str] = Field(default_factory=list)


class FileCommitReq(BaseModel):
    file_name: str = Field(..., min_length=1)
    chunks: List[str] = Field(..., min_length=1)


class FileCommitResp(BaseModel):
    status: Literal["ok", "error"]


class FileGetItem(BaseModel):
    fingerprint: str
    locations: List[str]


class FileGetResp(BaseModel):
    chunks: List[FileGetItem]


class StorageHeartbeatReq(BaseModel):
    node_id: str = Field(..., min_length=1)


class StorageHeartbeatResp(BaseModel):
    status: Literal["alive"]
    node_id: str
    observed_at: str


class LeaderHeartbeatReq(BaseModel):
    leader_id: str = Field(..., min_length=1)
    sent_at: str = ""


class LeaderHeartbeatResp(BaseModel):
    status: Literal["alive"]
    follower_id: str
    observed_at: str


class ReplicateStateReq(BaseModel):
    source_node_id: str = Field(..., min_length=1)
    generated_at: str = ""
    reason: str = "manual"
    membership: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class ReplicateStateResp(BaseModel):
    status: Literal["synced"]
    follower_id: str
    applied_at: str

