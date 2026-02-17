import json
import os 
import random
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

####################
# config (phase 1) #
####################

META_NODE_ID = os.getenv("META_NODE_ID", "meta-01")
# fixed leader in phase 1, no leader election
ROLE = os.getenv("META_ROLE", "leader")
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "1"))
# single storage in phase 1, no membership and heartbeat
STORAGE_NODES = [s.strip() for s in os.getenv("STORAGE_NODES", "storage-01").split(",") if s.strip()]

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
METADATA_FILE = DATA_DIR / os.getenv("METADATA_FILE", "metadata.json")

app = FastAPI(title="317_DFS_Meta", version="0.1-p1")




##########
# Models #
##########
class ChunkCheckReq(BaseModel):
    fingerprint: str = Field(..., min_length=1, description="the fingerprint of the chunk to check")

class ChunkCheckResp(BaseModel):
    exists: bool
    locations: List[str]

class ChunkRegisterReq(BaseModel):
    fingerprint: str = Field(..., min_length=1, description="the fingerprint of the chunk to check")

class ChunkRegisterResp(BaseModel):
    assigned_node: List[str]

class FileCommitReq(BaseModel):
    file_name: str = Field(..., min_length=1, description="the name of the file to commit")
    chunks: List[str] = Field(..., min_length=1) 

class FileCommitResp(BaseModel):
    status: Literal["ok", "error"]

class FileGetItem(BaseModel):
    fingerprint: str
    locations: List[str]

class FileGetResp(BaseModel):
    chunks: List[FileGetItem]


#######################################################
# Persistence                                         #
# metadata.json single source of truth for metadata   #
# no relational db used in v0.1                       #
# stored as a dict:                                   #
#######################################################

# check if metadata file exists
def _ensure_metadata_file() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not METADATA_FILE.exists():
        init_content = {"files":{}, "chunks":{}, "membership":{}, "version": 1}
        METADATA_FILE.write_text(json.dumps(init_content, indent = 2), encoding="utf-8")

# load
def _load_state() -> Dict:
    _ensure_metadata_file()
    try:
        return json.loads(METADATA_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to load metadata.json: {e}")

# save
def _persist_state(state: Dict) -> None:
    _ensure_metadata_file()
    temp = METADATA_FILE.with_suffix(".json.tmp")
    temp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    temp.replace(METADATA_FILE)


def _get_alive_storage_nodes(state: Dict) -> List[str]:
    """
    0.1_Phase1: membership 还没做 heartbeat，所以默认 STORAGE_NODES 都可用。
    0.1_Phase3 再改成：只选 membership==alive 的节点。
    """
    # 如果未来 state["membership"] 有数据，也允许按它过滤
    membership = state.get("membership") or {}
    if not membership:
        return STORAGE_NODES[:]
    alive = [n for n in STORAGE_NODES if membership.get(n, "alive") == "alive"]
    return alive


def _choose_replicas(alive_nodes: List[str], k: int) -> List[str]:
    if k <= 0:
        return []
    if len(alive_nodes) < k:
        raise HTTPException(status_code=500, detail="not enough replicas available")
    nodes = alive_nodes[:]
    random.shuffle(nodes)
    return nodes[:k]



#######################################
# meta-entry API for client           #
#######################################

# basic endpoint, health check
@app.get("/health")
def health():
    return { "role":"meta", "ok": True }

# chunk check
@app.post("/chunk/check", response_model=ChunkCheckResp)
def chunk_check(req: ChunkCheckReq):
    state = _load_state()
    chunk_info = (state.get("chunks") or {}).get(req.fingerprint)
    if not chunk_info:
        return ChunkCheckResp(exists=False, locations=[])
    replicas = chunk_info.get("replicas", [])
    return ChunkCheckResp(exists=True, locations=replicas)

# chunk register
@app.post("/chunk/register", response_model=ChunkRegisterResp)
def chunk_register(req: ChunkRegisterReq):
    state = _load_state()
    chunks = state.setdefault("chunks", {})

    # 如果 chunk 已经存在了，就直接返回它的 locations（虽然理论上不应该让 client 重复注册同一个 chunk）
    if req.fingerprint in chunks:
        replicas = chunks[req.fingerprint].get("replicas") or []
        return ChunkRegisterResp(assigned_node=replicas)
    
    alive_nodes = _get_alive_storage_nodes(state)
    assigned = _choose_replicas(alive_nodes, REPLICATION_FACTOR)

    chunks[req.fingerprint] = {"replicas": assigned}
    _persist_state(state)

    return ChunkRegisterResp(assigned_node=assigned)


# commit(upload) file
@app.post("/file/commit", response_model=FileCommitResp)
def file_commit(req: FileCommitReq):
    state = _load_state()
    files = state.setdefault("files", {})
    chunks = state.setdefault("chunks", {})

    # basic validation: all chunks must be registered
    missing = [fp for fp in req.chunks if fp not in chunks]
    if missing:
        raise HTTPException(status_code=400, detail=f"chunks not registered: {missing}")

    files[req.file_name] = {"chunks": req.chunks}

    # Phase1: persist metadata.json -> (no follower replicate yet) -> return ok
    _persist_state(state)

    return FileCommitResp(status="ok")

# get(download) file
@app.get("/file/{file_name}", response_model=FileGetResp)
def file_get(file_name: str):
    state = _load_state()
    files = state.get("files") or {}
    chunks = state.get("chunks") or {}

    if file_name not in files:
        raise HTTPException(status_code=404, detail="file not found")
    fps = files[file_name].get("chunks") or []
    out: List[FileGetItem] = []
    for fp in fps:
        locs = (chunks.get(fp) or {}).get("replicas") or []
        out.append(FileGetItem(fingerprint=fp, locations=locs))
    
    return FileGetResp(chunks=out)

#################################
#   debug APIs  (0.1phase1)     #
#################################
@app.get("/debug/leader")
def debug_leader():
    # fixed leader in phase 
    return {"leader": "meta-01"}

@app.get("/debug/membership")
def debug_membership():
    # no heartbeat in phase 1
    state = _load_state()
    return {"membership": state.get("membership") or {}}