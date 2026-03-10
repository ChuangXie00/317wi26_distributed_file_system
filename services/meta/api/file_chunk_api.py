import random
from typing import List

from fastapi import APIRouter, HTTPException

from core.config import REPLICATION_FACTOR
from core.replication import push_state_to_followers
from core.runtime import get_runtime_snapshot, is_writable_leader, tick_lamport
from core.state import (
    State,
    choose_replicas,
    get_alive_storage_nodes,
    load_state,
    persist_state,
    refresh_storage_membership,
)
from repository import get_repository

from .vo import (
    ChunkCheckReq,
    ChunkCheckResp,
    ChunkRegisterReq,
    ChunkRegisterResp,
    FileCommitReq,
    FileCommitResp,
    FileGetItem,
    FileGetResp,
)

router = APIRouter()
REPO = get_repository()


def _ensure_leader_write_api() -> None:
    # 中文：Phase5 写门禁（fencing）；只有“我就是当前 leader”时才允许写。
    if not is_writable_leader():
        runtime = get_runtime_snapshot()
        raise HTTPException(
            status_code=409,
            detail=(
                "node is not writable leader: "
                f"role={runtime.get('role')}, "
                f"leader={runtime.get('current_leader_id')}, "
                f"epoch={runtime.get('leader_epoch')}"
            ),
        )


def _load_state_with_membership_refresh() -> State:
    state = load_state()
    # 中文：仅可写 leader 主动刷新 membership 超时；follower 被动接收同步。
    if is_writable_leader() and refresh_storage_membership(state):
        persist_state(state)
    return state


def _unique_nodes(nodes: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for node in nodes:
        value = str(node).strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
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


def _build_chunk_register_resp(nodes: List[str]) -> ChunkRegisterResp:
    # 新旧字段同时返回，兼容历史 client
    unique = _unique_nodes(nodes)
    return ChunkRegisterResp(assigned_nodes=unique, assigned_node=unique)


@router.get("/health")
def health() -> dict:
    runtime = get_runtime_snapshot()
    return {
        "role": "meta",
        "meta_role": runtime.get("role"),
        "current_leader_id": runtime.get("current_leader_id"),
        "leader_epoch": runtime.get("leader_epoch"),
        "ok": True,
    }


@router.post("/chunk/check", response_model=ChunkCheckResp)
def chunk_check(req: ChunkCheckReq) -> ChunkCheckResp:
    state = _load_state_with_membership_refresh()
    replicas = _unique_nodes(REPO.get_chunk_replicas(req.fingerprint))
    if not replicas:
        return ChunkCheckResp(exists=False, locations=[])

    alive_set = set(get_alive_storage_nodes(state))
    locations = [node for node in replicas if node in alive_set]

    # 仅当可用副本数满足 REPLICATION_FACTOR 时视为存在
    exists = len(locations) >= REPLICATION_FACTOR
    return ChunkCheckResp(exists=exists, locations=locations)


@router.post("/chunk/register", response_model=ChunkRegisterResp)
def chunk_register(req: ChunkRegisterReq) -> ChunkRegisterResp:
    _ensure_leader_write_api()
    # 中文：写事件推进 Lamport，便于 debug 追踪时序。
    tick_lamport(event="chunk_register")

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


@router.post("/file/commit", response_model=FileCommitResp)
def file_commit(req: FileCommitReq) -> FileCommitResp:
    _ensure_leader_write_api()
    # 中文：commit 是主链路关键事件，必须推进 Lamport。
    tick_lamport(event="file_commit")

    state = _load_state_with_membership_refresh()
    alive_nodes = get_alive_storage_nodes(state)
    if len(alive_nodes) < REPLICATION_FACTOR:
        raise HTTPException(status_code=500, detail="not enough replicas(storage nodes) available")

    missing = REPO.list_missing_chunks(req.chunks)
    if missing:
        raise HTTPException(status_code=400, detail=f"chunks not registered: {missing}")

    # 写文件映射前，先修复涉及 chunk 的副本
    for fp in set(req.chunks):
        current = _unique_nodes(REPO.get_chunk_replicas(fp))
        repaired = _repair_chunk_replicas(current, alive_nodes)
        if repaired != current:
            REPO.upsert_chunk_replicas(fp, repaired)

    try:
        REPO.upsert_file(req.file_name, req.chunks)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # 主链路成功后尽力同步到 follower（失败不阻断）
    push_state_to_followers(reason="file_commit")
    return FileCommitResp(status="ok")


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

