from fastapi import APIRouter

from core.config import META_NODE_ID, ROLE
from core.replication import get_replication_status
from core.state import get_membership_snapshot, load_state, persist_state, refresh_storage_membership
from repository import get_repository

router = APIRouter()
REPO = get_repository()


@router.get("/debug/leader")
def debug_leader() -> dict:
    leader = META_NODE_ID if ROLE == "leader" else "meta-01"
    return {"leader": leader}


@router.get("/debug/membership")
def debug_membership() -> dict:
    state = load_state()
    changed = False

    if ROLE == "leader":
        changed = refresh_storage_membership(state)
        if changed:
            persist_state(state)

    snapshot = get_membership_snapshot(state)
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
        "refreshed_by_leader": changed if ROLE == "leader" else False,
    }


@router.get("/debug/repository")
def debug_repository() -> dict:
    return REPO.db_health()


@router.get("/debug/replication")
def debug_replication() -> dict:
    return get_replication_status()

