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