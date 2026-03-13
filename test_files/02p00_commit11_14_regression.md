# 0.2p00 Regression (Commit 11-14)
Date: 2026-03-13 (America/Los_Angeles)  
Workspace: `D:\csen317_proj\dfs`

## Covered Commits

1. Commit 11: File Panel upload/download
2. Commit 12: Replica Matrix live mapping
3. Commit 13: Election & Replication live cards
4. Commit 14: Timeline newest-first rendering

## Commands

```powershell
# Backend compile
.\.venv\Scripts\python.exe -m compileall services/demo_backend

# Frontend build
cd frontend
npm.cmd run build
cd ..
```

```powershell
# API smoke (run in services/demo_backend)
$env:DEMO_STORAGE_HOSTS='storage-01=http://127.0.0.1:9009,storage-02=http://127.0.0.1:9009,storage-03=http://127.0.0.1:9011'
# TestClient checks:
# - POST /api/demo/file/upload
# - GET  /api/demo/file/replicas
# - GET  /api/demo/file/download
# - downloaded bytes == uploaded bytes
```

```powershell
# Replication mapping check
# verify core.aggregator._normalize_replication handles:
# leader_heartbeat + replication + takeover
```

```powershell
# Timeline order check (frontend code)
rg -n "reverse\\(\\)|最新在最上|倒序" frontend/src/components/demo/timeline/EventStream.vue -S
```

## Results

1. Backend compile: `PASS`
2. Frontend build: `PASS`
3. Upload API: `PASS` (`upload_status 200`)
4. Replica Matrix API: `PASS` (`replicas_status 200`)
5. Download API: `PASS` (`download_status 200`, `content_match True`)
6. Replication mapping unit-style check: `PASS` (`hb_alive=True`, `snapshot_ok=True`, `sync_source=meta-01`)
7. Timeline newest-first rendering check: `PASS` (`reverse()` found in EventStream)

## Environment Note

On this machine, host port `9010` may be occupied by a non-storage process (`HTTP 426` observed once).  
Regression smoke sets `DEMO_STORAGE_HOSTS` explicitly to keep tests deterministic.
