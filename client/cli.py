######################################
#  client cli, run in host terminal  # 
######################################
import hashlib
import os

from pathlib import Path
from typing import Dict, List

import requests
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(add_completion=False)
console = Console()

META_BASE = os.getenv("META_BASE", "http://localhost:8000")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", str(1 * 1024 * 1024)))  # 1MB - default chunk size

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def iter_chunks(file_path: Path, chunk_size: int):
    with file_path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            yield b


def storage_host_url(node_id: str) -> str:
    # Phase1 只映射 storage-01 -> localhost:9009
    # Phase2 可改成从配置文件/映射表解析
    if node_id == "storage-01":
        return "http://localhost:9009"
    raise RuntimeError(f"storage node not mapped to host: {node_id}")


@app.command()
def upload(file: str, name: str = typer.Option(None, help="file name to store (default: basename)")):
    file_path = Path(file)
    if not file_path.exists():
        raise typer.BadParameter("file not found")

    file_name = name or file_path.name
    fps: List[str] = []

    # 1) chunk -> fp -> check/register -> upload (if needed)
    for chunk_bytes in iter_chunks(file_path, CHUNK_SIZE):
        fp = sha256_hex(chunk_bytes)
        fps.append(fp)

        r = requests.post(f"{META_BASE}/chunk/check", json={"fingerprint": fp}, timeout=10)
        r.raise_for_status()
        exists = r.json().get("exists", False)
        locations = r.json().get("locations", [])

        if not exists:
            r2 = requests.post(f"{META_BASE}/chunk/register", json={"fingerprint": fp}, timeout=10)
            r2.raise_for_status()
            locations = r2.json().get("assigned_nodes", [])

        for node_id in locations:
            base = storage_host_url(node_id)
            up = requests.put(
                f"{base}/chunk/upload",
                headers={"fingerprint": fp},
                data=chunk_bytes,
                timeout=30,
            )
            up.raise_for_status()

    # 2) commit
    c = requests.post(f"{META_BASE}/file/commit", json={"file_name": file_name, "chunks": fps}, timeout=10)
    c.raise_for_status()

    console.print(f"[green]OK[/green] uploaded: {file_name}  chunks={len(fps)}")


@app.command()
def download(name: str, out: str = typer.Option(".", help="output dir")):
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)

    r = requests.get(f"{META_BASE}/file/{name}", timeout=10)
    if r.status_code == 404:
        console.print("[red]file not found[/red]")
        raise typer.Exit(code=1)
    r.raise_for_status()

    items = r.json().get("chunks", [])
    if not items:
        console.print("[red]empty file metadata[/red]")
        raise typer.Exit(code=1)

    out_path = out_dir / name
    with out_path.open("wb") as f:
        for it in items:
            fp = it["fingerprint"]
            locs = it.get("locations") or []
            if not locs:
                raise RuntimeError(f"no replica locations for chunk {fp}")

            # Phase1：只取第一个 location
            node_id = locs[0]
            base = storage_host_url(node_id)
            g = requests.get(f"{base}/chunk/{fp}", timeout=30)
            g.raise_for_status()
            f.write(g.content)

    console.print(f"[green]OK[/green] downloaded to: {out_path}")


@app.command()
def show(name: str):
    r = requests.get(f"{META_BASE}/file/{name}", timeout=10)
    r.raise_for_status()
    items = r.json().get("chunks", [])

    table = Table(title=f"file: {name}")
    table.add_column("#", justify="right")
    table.add_column("fingerprint")
    table.add_column("locations")
    for i, it in enumerate(items):
        table.add_row(str(i), it["fingerprint"], ",".join(it.get("locations") or []))
    console.print(table)


if __name__ == "__main__":
    app()