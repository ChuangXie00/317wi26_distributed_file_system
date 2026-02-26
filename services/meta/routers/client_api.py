import random 
from typing import Dict, List, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from core.config import META_NODE_ID, REPLICATION_FACTOR, ROLE, STORAGE_NODES
from core.state import (
    State,
    choose_replicas, 
    get_alive_storage_nodes, 
    get_membership_snapshot,
    load_state, 
    mark_storage_heartbeat,
    persist_state,
    refresh_storage_membership,
)

router = APIRouter()

def _load_state_with_membership_refresh() -> State:
    # refresh membership timeout info first b4 ecah API call
    # avoid dead storage node to be assignded for chunk
    state = load_state()
    if refresh_storage_membership(state):
        persist_state(state)
    return state


def _unique_nodes(nodes: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for node in nodes:
        if node and node not in seen:
            seen.add(node)
            out.append(node)
    return out

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

@router.post("/internal/storage_heartbeat", response_model=StorageHeartbeatResp)
def storage_heartbeat(req: StorageHeartbeatReq) -> StorageHeartbeatResp:
    node_id = req.node_id.strip()
    if not node_id:
        raise HTTPException(status_code=400, detail="node_id is required")
    if node_id not in STORAGE_NODES:
        raise HTTPException(status_code=400, detail=f"unknown storage node: {node_id}")

    state = load_state()

    # 先刷新超时状态，再写入当前心跳，保证状态演进顺序可解释
    changed = refresh_storage_membership(state)
    if mark_storage_heartbeat(state, node_id):
        changed = True

    if changed:
        persist_state(state)

    snapshot = get_membership_snapshot(state)
    observed_at = str(snapshot.get(node_id, {}).get("last_heartbeat_at", ""))
    return StorageHeartbeatResp(status="alive", node_id=node_id, observed_at=observed_at)


@router.post("/chunk/check", response_model=ChunkCheckResp)
def chunk_check(req: ChunkCheckReq) -> ChunkCheckResp:
    state = _load_state_with_membership_refresh()
    chunk_info = state.get("chunks", {}).get(req.fingerprint)
    if not chunk_info:
        return ChunkCheckResp(exists=False, locations=[])

    replicas = _unique_nodes(chunk_info.get("replicas", []))
    alive_set = set(get_alive_storage_nodes(state))
    replicas = [node for node in replicas if node in alive_set]

    # only replicas larger than or equal to REPLICATION_FACTOR is considered exists
    exists = len(replicas) >= REPLICATION_FACTOR
    return ChunkCheckResp(exists=exists, locations=replicas)

@router.post("/chunk/register", response_model=ChunkRegisterResp)
def chunk_register(req: ChunkRegisterReq) -> ChunkRegisterResp:
    state = _load_state_with_membership_refresh()
    chunks = state.setdefault("chunks", {})
    alive_nodes = get_alive_storage_nodes(state)

    if len(alive_nodes) < REPLICATION_FACTOR:
        raise HTTPException(status_code=500, detail="not enough replicas(storage nodes) available")

    chunk_info = chunks.get(req.fingerprint)
    if not chunk_info:
        assigned = choose_replicas(alive_nodes, REPLICATION_FACTOR)
        chunks[req.fingerprint] = {"replicas": assigned}
        persist_state(state)
        return _build_chunk_register_resp(assigned)
    
    # for existing chunk, if the current replicas are less than REPLICATION_FACTOR
    # repair it by assign new replicas, and update metadata
    current_replicas = chunk_info.get("replicas", [])
    repaired = _repair_chunk_replicas(current_replicas, alive_nodes)
    if repaired != _unique_nodes(current_replicas):
        chunks[req.fingerprint]["replicas"] = repaired
        persist_state(state)

    return _build_chunk_register_resp(repaired)


@router.post("/file/commit", response_model=FileCommitResp)
def file_commit(req: FileCommitReq) -> FileCommitResp:
    state = _load_state_with_membership_refresh()
    files = state.setdefault("files", {})
    chunks = state.setdefault("chunks", {})

    missing = [fp for fp in req.chunks if fp not in chunks]
    if missing:
        raise HTTPException(status_code=400, detail=f"chunks not registered: {missing}")

    alive_nodes = get_alive_storage_nodes(state)
    if len(alive_nodes) < REPLICATION_FACTOR:
        raise HTTPException(status_code=500, detail="not enough replicas(storage nodes) available")

    # repair replicas b4 commit
    # ensure metadata doesn't point to dead nodes
    for fp in set(req.chunks):
        current_replicas = chunks.get(fp, {}).get("replicas", [])
        repaired = _repair_chunk_replicas(current_replicas, alive_nodes)
        chunks[fp] = {"replicas": repaired}

    files[req.file_name] = {"chunks": req.chunks}
    persist_state(state)
    return FileCommitResp(status="ok")


@router.get("/file/{file_name}", response_model=FileGetResp)
def file_get(file_name: str) -> FileGetResp:
    state = _load_state_with_membership_refresh()
    files = state.get("files", {})
    chunks = state.get("chunks", {})
    alive_set = set(get_alive_storage_nodes(state))

    if file_name not in files:
        raise HTTPException(status_code=404, detail="file not found")

    out: List[FileGetItem] = []
    for fp in files[file_name].get("chunks", []):
        # when download file, only return alive nodes
        # reduce unneccessary retry to dead nodes on client side
        replicas = _unique_nodes(chunks.get(fp, {}).get("replicas", []))
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