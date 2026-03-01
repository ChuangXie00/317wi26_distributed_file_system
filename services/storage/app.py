import json
import os
import threading
from pathlib import Path
from typing import Optional
from urllib.error import URLError, HTTPError
from urllib.request import Request as UrlRequest, urlopen

from fastapi import FastAPI, Header, HTTPException, Request, Response


###########################
#  config (0.1p03)        #
###########################
NODE_ID = os.getenv("NODE_ID", "storage-01")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
CHUNK_DIR = DATA_DIR / "chunks"

# send heartbeat to meta-entry(stable endpoint)
# every interval sec
META_ENTRY_BASE_URL = os.getenv("META_ENTRY_BASE_URL", "http://meta-entry:8000").rstrip("/")
STORAGE_HEARTBEAT_INTERVAL_SEC = float(os.getenv("STORAGE_HEARTBEAT_INTERVAL_SEC", "3"))
STORAGE_HEARTBEAT_TIMEOUT_SEC = float(os.getenv("STORAGE_HEARTBEAT_TIMEOUT_SEC", "2"))

_HEARTBEAT_STOP_EVENT = threading.Event()
_HEARTBEAT_THREAD: Optional[threading.Thread] = None


app = FastAPI(title="317_DFS_Storage", version="0.1-p3")


def _chunk_path(fp: str) -> Path:
    return CHUNK_DIR / f"{fp}.chunk"

def _heartbeat_url() -> str:
    return f"{META_ENTRY_BASE_URL}/internal/storage_heartbeat"

def _send_heartbeat_once() -> None:
    payload = json.dumps({"node_id": NODE_ID}).encode("utf-8")
    req = UrlRequest(
        _heartbeat_url(),
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=STORAGE_HEARTBEAT_TIMEOUT_SEC) as response:
        if response.status != 200:
            raise RuntimeError(f"heartbeat failed, status={response.status}")

def _heartbeat_loop() -> None:
    while not _HEARTBEAT_STOP_EVENT.is_set():
        try:
            _send_heartbeat_once()
        except (HTTPError, URLError, OSError, RuntimeError) as e:
            print(f"[{NODE_ID}] heartbeat error: {e}")
        
        _HEARTBEAT_STOP_EVENT.wait(STORAGE_HEARTBEAT_INTERVAL_SEC)


@app.on_event("startup")
def startup():
    global _HEARTBEAT_THREAD
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)

    _HEARTBEAT_STOP_EVENT.clear()
    _HEARTBEAT_THREAD = threading.Thread(
        target=_heartbeat_loop,
        name=f"{NODE_ID}-heartbeat-thread",
        daemon=True,
    )
    _HEARTBEAT_THREAD.start()


@app.on_event("shutdown")
def shutdown() -> None:
    _HEARTBEAT_STOP_EVENT.set()
    
    global _HEARTBEAT_THREAD
    if _HEARTBEAT_THREAD is not None and _HEARTBEAT_THREAD.is_alive():
        _HEARTBEAT_THREAD.join(timeout=2.0)
    _HEARTBEAT_THREAD = None

###########################
#  APIs                   #
###########################
@app.get("/health")
def health():
    return {"role": "storage", "ok": True}

@app.put("/chunk/upload")
async def chunk_upload(request: Request, fingerprint: str = Header(None)):
    if not fingerprint:
        raise HTTPException(status_code=400, detail="Missing fingerprint header")
    body = await request.body()
    if body is None or len(body) == 0:
        body = b""
    
    p = _chunk_path(fingerprint)

    # 幂等，同fingerprint已存在则不重写（dedup）
    if not p.exists():
        p.write_bytes(body)
    
    return {"status": "ok", "bytes": len(body)}


@app.get("/chunk/{fingerprint}")
def chunk_get(fingerprint: str):
    p = _chunk_path(fingerprint)
    if not p.exists():
        raise HTTPException(status_code=404, detail="Chunk not found")
    
    data = p.read_bytes()
    # raw bin resp, not json
    return Response(content=data, media_type="application/octet-stream")