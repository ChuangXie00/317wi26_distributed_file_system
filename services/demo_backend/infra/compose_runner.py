import os
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ACTION_TO_COMPOSE_SUBCOMMAND = {
    "stop": "stop",
    "start": "start",
    "reload": "restart",
}


@dataclass
class ComposeRunResult:
    command: str
    started_at: str
    finished_at: str
    duration_ms: int
    exit_code: int | None
    stdout_tail: str
    stderr_tail: str
    timeout: bool


class ComposeRunner:
    def __init__(self, timeout_sec: float | None = None, compose_file: str | None = None) -> None:
        self.timeout_sec = timeout_sec or float(os.getenv("DEMO_COMPOSE_TIMEOUT_SEC", "15"))
        self.repo_root = Path(__file__).resolve().parents[3]
        raw_compose = compose_file or os.getenv("DEMO_COMPOSE_FILE", "deploy/docker-compose.yml")
        self.compose_file_display = raw_compose
        compose_path = Path(raw_compose)
        if compose_path.is_absolute():
            self.compose_file = compose_path
        else:
            self.compose_file = (self.repo_root / compose_path).resolve()

    def run(self, normalized_action: str, target: str, dry_run: bool = False) -> ComposeRunResult:
        compose_subcommand = ACTION_TO_COMPOSE_SUBCOMMAND[normalized_action]
        command_parts = [
            "docker",
            "compose",
            "-f",
            str(self.compose_file),
            compose_subcommand,
            target,
        ]
        command = " ".join(
            shlex.quote(part)
            for part in ["docker", "compose", "-f", self.compose_file_display, compose_subcommand, target]
        )
        started_at = _utc_now()
        started_perf = _perf_counter()

        if dry_run:
            finished_at = _utc_now()
            return ComposeRunResult(
                command=command,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=_elapsed_ms(started_perf),
                exit_code=0,
                stdout_tail="dry_run: skipped docker compose execution",
                stderr_tail="",
                timeout=False,
            )

        try:
            completed = subprocess.run(
                command_parts,
                cwd=str(self.repo_root),
                capture_output=True,
                text=True,
                timeout=self.timeout_sec,
                check=False,
            )
            finished_at = _utc_now()
            return ComposeRunResult(
                command=command,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=_elapsed_ms(started_perf),
                exit_code=completed.returncode,
                stdout_tail=_tail(completed.stdout),
                stderr_tail=_tail(completed.stderr),
                timeout=False,
            )
        except subprocess.TimeoutExpired as exc:
            finished_at = _utc_now()
            return ComposeRunResult(
                command=command,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=_elapsed_ms(started_perf),
                exit_code=None,
                stdout_tail=_tail(exc.stdout if isinstance(exc.stdout, str) else ""),
                stderr_tail=_tail(exc.stderr if isinstance(exc.stderr, str) else ""),
                timeout=True,
            )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _tail(text: str, max_chars: int = 600) -> str:
    value = (text or "").strip()
    if len(value) <= max_chars:
        return value
    return value[-max_chars:]


def _perf_counter() -> float:
    # Local import keeps startup path minimal for non-action routes.
    import time

    return time.perf_counter()


def _elapsed_ms(started_perf: float) -> int:
    import time

    return int((time.perf_counter() - started_perf) * 1000)
