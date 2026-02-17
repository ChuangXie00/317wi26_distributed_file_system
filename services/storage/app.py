import os
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request, Response


###########################
#  config (phase 1)       #
###########################
NODE_ID = os.getenv("NODE_ID", "storage-01")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
CHUNK_DIR = DATA_DIR / "chunks"

app = FastAPI(title="317_DFS_Storage", version="0.1-p1")


def _chunk_path(fp: str) -> Path:
    return CHUNK_DIR / f"{fp}.chunk"

@app.on_event("startup")
def startup():
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)


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