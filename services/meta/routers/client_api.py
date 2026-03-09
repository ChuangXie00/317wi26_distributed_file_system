import random 
from typing import Dict, List, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from core.config import (
    HEARTBEAT_WRITE_MIN_INTERVAL_SEC,
    META_NODE_ID,
    REPLICATION_FACTOR,
    ROLE,
    STORAGE_NODES
)
from core.state import (
    State,
    choose_replicas, 
    get_alive_storage_nodes, 
    get_membership_snapshot,
    load_state, 
    mark_storage_heartbeat,
    mutate_state,
    persist_state,
    refresh_storage_membership,
)
from repository import get_repository

router = APIRouter()
REPO = get_repository()

def _load_state_with_membership_refresh() -> State:
    # refresh membership timeout info first b4 ecah API call
    # avoid dead storage node to be assignded for chunk
    state = load_state()
    if refresh_storage_membership(state):
        persist_state(state)
    return state

# dedup node list and preserve original order
def _unique_nodes(nodes: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for node in nodes:
        value = str(node).strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out

# 修复副本列表，去除dead节点，补齐副本数
def _repair_chunk_replicas(current_replicas: List[str], alive_nodes: List[str]) -> List[str]:
    current = [node for node in _unique_nodes(current_replicas) if node in alive_nodes]

    if len(current) >= REPLICATION_FACTOR:
        return current[:REPLICATION_FACTOR]

    needed = REPLICATION_FACTOR - len(current)
    candidates = [node for node in alive_nodes if node not in current]

    if len(candidates) < needed:
        raise HTTPException(status_code=500, detail="not enough replicas available")

    return current + random.sample(candidates, needed)

class ChunkCheckReq(BaseModel):
    fingerprint: str = Field(..., min_length = 1)

class ChunkCheckResp(BaseModel):
    exists: bool
    locations: List[str]

class ChunkRegisterReq(BaseModel):
    fingerprint: str = Field(..., min_length=1)

class ChunkRegisterResp(BaseModel):
    # new field
    assigned_nodes: List[str]
    # compatible with 0.1p02
    # this field is selected for backward compatibility
    # will be removed in future, use assigned_nodes instead
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

def _build_chunk_register_resp(nodes: List[str]) -> ChunkRegisterResp:
    # return new and old field at same time for compatibility, will remove assigned_node in future
    unique = _unique_nodes(nodes)
    return ChunkRegisterResp(assigned_nodes=unique, assigned_node=unique)

@router.get("/health")
def health() -> dict:
    return {"role": "meta", "ok": True}

# 处理storage心跳，加锁完成超时刷新、心跳写入(0.1p3.1)
@router.post("/internal/storage_heartbeat", response_model=StorageHeartbeatResp)
def storage_heartbeat(req: StorageHeartbeatReq) -> StorageHeartbeatResp:
    node_id = req.node_id.strip()
    if not node_id:
        raise HTTPException(status_code=400, detail="node_id is required")
    if node_id not in STORAGE_NODES:
        raise HTTPException(status_code=400, detail=f"unknown storage node: {node_id}")

    holder: Dict[str, str] = {"observed_at": ""}


    def _mutator(state: State) -> bool:
        changed = refresh_storage_membership(state)
        if mark_storage_heartbeat(state, node_id, min_interval_sec=HEARTBEAT_WRITE_MIN_INTERVAL_SEC):
            changed = True

        snapshot = get_membership_snapshot(state)
        holder["observed_at"] = str(snapshot.get(node_id, {}).get("last_heartbeat_at", ""))
        return changed

    mutate_state(_mutator)
    return StorageHeartbeatResp(status="alive", node_id=node_id, observed_at=holder["observed_at"])

# 检查chunk是否存在可用副本(0.1p3.1引入postgre)
@router.post("/chunk/check", response_model=ChunkCheckResp)
def chunk_check(req: ChunkCheckReq) -> ChunkCheckResp:
    state = _load_state_with_membership_refresh()
    replicas = _unique_nodes(REPO.get_chunk_replicas(req.fingerprint))
    if not replicas:
        return ChunkCheckResp(exists=False, locations=[])

    alive_set = set(get_alive_storage_nodes(state))
    locations = [node for node in replicas if node in alive_set]

    # only replicas larger than or equal to REPLICATION_FACTOR is considered exists
    exists = len(locations) >= REPLICATION_FACTOR
    return ChunkCheckResp(exists=exists, locations=locations)

# 注册chunk，有记录则执行按当前存活节点修复副本
@router.post("/chunk/register", response_model=ChunkRegisterResp)
def chunk_register(req: ChunkRegisterReq) -> ChunkRegisterResp:
    state = _load_state_with_membership_refresh()
    alive_nodes = get_alive_storage_nodes(state)

    if len(alive_nodes) < REPLICATION_FACTOR:
        raise HTTPException(status_code=500, detail="not enough replicas(storage nodes) available")

    current = _unique_nodes(REPO.get_chunk_replicas(req.fingerprint))
    if not current:
        assigned = choose_replicas(alive_nodes, REPLICATION_FACTOR)
        REPO.upsert_chunk_replicas(req.fingerprint, assigned)
        return _build_chunk_register_resp(assigned)

    repaired = _repair_chunk_replicas(current, alive_nodes)
    if repaired != current:
        REPO.upsert_chunk_replicas(req.fingerprint, repaired)

    return _build_chunk_register_resp(repaired)

# commit文件meta数据到DB，事务内更新file到chunks的映射
@router.post("/file/commit", response_model=FileCommitResp)
def file_commit(req: FileCommitReq) -> FileCommitResp:
    state = _load_state_with_membership_refresh()
    alive_nodes = get_alive_storage_nodes(state)

    if len(alive_nodes) < REPLICATION_FACTOR:
        raise HTTPException(status_code=500, detail="not enough replicas(storage nodes) available")

    missing = REPO.list_missing_chunks(req.chunks)
    if missing:
        raise HTTPException(status_code=400, detail=f"chunks not registered: {missing}")

    # 在写文件映射前，先修复所有涉及chunk的副本
    for fp in set(req.chunks):
        current = _unique_nodes(REPO.get_chunk_replicas(fp))
        repaired = _repair_chunk_replicas(current, alive_nodes)
        if repaired != current:
            REPO.upsert_chunk_replicas(fp, repaired)

    try:
        REPO.upsert_file(req.file_name, req.chunks)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return FileCommitResp(status="ok")

# 查询文件分块与可用副本位置(过滤dead)
@router.get("/file/{file_name}", response_model=FileGetResp)
def file_get(file_name: str) -> FileGetResp:
    state = _load_state_with_membership_refresh()
    alive_set = set(get_alive_storage_nodes(state))

    chunk_fps = REPO.get_file_chunks(file_name)
    if chunk_fps is None:
        raise HTTPException(status_code=404, detail="file not found")

    out: List[FileGetItem] = []
    for fp in chunk_fps:
        replicas = _unique_nodes(REPO.get_chunk_replicas(fp))
        locations = [node for node in replicas if node in alive_set]
        out.append(FileGetItem(fingerprint=fp, locations=locations))

    return FileGetResp(chunks=out)

@router.get("/debug/leader")
def debug_leader() -> dict:
    leader = META_NODE_ID if ROLE == "leader" else "meta-01"
    return {"leader": leader}


@router.get("/debug/membership")
def debug_membership() -> dict:
    state = load_state()
    changed = refresh_storage_membership(state)
    snapshot = get_membership_snapshot(state)

    if changed:
        persist_state(state)

    alive_count = 0
    dead_count = 0
    suspected_count = 0

    for entry in snapshot.values():
        status = str(entry.get("status", "dead"))
        if status == "alive":
            alive_count += 1
        elif status == "suspected":
            suspected_count += 1
        else:
            dead_count += 1

    return {
        "membership": snapshot,
        "summary": {
            "alive": alive_count,
            "suspected": suspected_count,
            "dead": dead_count,
            "total": len(snapshot),
        },
    }

# 检测DB的连通性
@router.get("/debug/repository")
def debug_repository() -> dict:
    return REPO.db_health()
