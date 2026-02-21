import random 
from typing import List, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from core.config import REPLICATION_FACTOR, META_NODE_ID, ROLE
from core.state import choose_replicas, get_alive_storage_nodes, load_state, persist_state

router = APIRouter()

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
    assigned_node: List[str]

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

@router.get("/health")
def health() -> dict:
    return {"role": "meta", "ok": True}

@router.post("/chunk/check", response_model=ChunkCheckResp)
def chunk_check(req: ChunkCheckReq) -> ChunkCheckResp:
    state = load_state()
    chunk_info = state.get("chunks", {}).get(req.fingerprint)
    if not chunk_info:
        return ChunkCheckResp(exists=False, locations=[])
    
    replicas = _unique_nodes(chunk_info.get("replicas", []))
    alive_nodes = get_alive_storage_nodes(state)
    if alive_nodes:
        alive_set = set(alive_nodes)
        replicas = [node for node in replicas if node in alive_set]
    else:
        replicas = []

    # only replicas larger than or equal to REPLICATION_FACTOR is considered exists
    exists = len(replicas) >= REPLICATION_FACTOR
    return ChunkCheckResp(exists=exists, locations=replicas)

@router.post("/chunk/register", response_model=ChunkRegisterResp)
def chunk_register(req: ChunkRegisterReq) -> ChunkRegisterResp:
    state = load_state()
    chunks = state.setdefault("chunks", {})
    alive_nodes = get_alive_storage_nodes(state)

    if len(alive_nodes) < REPLICATION_FACTOR:
        raise HTTPException(status_code=500, detail="not enough replicas(storage nodes) available")
    
    chunk_info = chunks.get(req.fingerprint)
    if not chunk_info:
        assigned = choose_replicas(alive_nodes, REPLICATION_FACTOR)
        chunks[req.fingerprint] = {"replicas": assigned}
        persist_state(state)
        return ChunkRegisterResp(assigned_node=assigned)
    
    # for existing chunk, if the current replicas are less than REPLICATION_FACTOR
    # repair it by assign new replicas, and update metadata
    current_replicas = chunk_info.get("replicas", [])
    repaired = _repair_chunk_replicas(current_replicas, alive_nodes)
    if repaired != current_replicas:
        chunks[req.fingerprint]["replicas"] = repaired
        persist_state(state)
    
    return ChunkRegisterResp(assigned_node=repaired)


@router.post("/file/commit", response_model=FileCommitResp)
def file_commit(req: FileCommitReq) -> FileCommitResp:
    state = load_state()
    files = state.setdefault("files", {})
    chunks = state.setdefault("chunks", {})

    missing = [fp for fp in req.chunks if fp not in chunks]
    if missing:
        raise HTTPException(status_code=400, detail=f"chunks not registered: {missing}")
    
    alive_nodes = get_alive_storage_nodes(state)
    if len(alive_nodes) < REPLICATION_FACTOR:
        raise HTTPException(status_code=500, detail="not enough replicas(storage nodes) available")

    # repair replicas b4 commit
    # ensure the chunk replicas are health when commit
    for fp in set(req.chunks):
        current_replicas = chunks.get(fp, {}).get("replicas", [])
        repaired = _repair_chunk_replicas(current_replicas, alive_nodes)
        chunks[fp] = {"replicas": repaired}
    
    files[req.file_name] = {"chunks": req.chunks}
    persist_state(state)
    return FileCommitResp(status="ok")


@router.get("/file/{file_name}", response_model=FileGetResp)
def file_get(file_name: str) -> FileGetResp:
    state = load_state()
    files = state.get("files", {})
    chunks = state.get("chunks", {})

    if file_name not in files:
        raise HTTPException(status_code=404, detail="file not found")
    
    out: List[FileGetItem] = []
    for fp in files[file_name].get("chunks", []):
        locations = _unique_nodes(chunks.get(fp, {}).get("replicas", []))
        out.append(FileGetItem(fingerprint=fp, locations=locations))

    return FileGetResp(chunks=out)

@router.get("/debug/leader")
def debug_leader() -> dict:
    leader = META_NODE_ID if ROLE == "leader" else "meta-01"
    return {"leader": leader}


@router.get("/debug/membership")
def debug_membership() -> dict:
    state = load_state()
    return {"membership": state.get("membership") or {}}