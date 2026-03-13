import os
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


# 归一化动作到 docker compose 子命令的映射。
ACTION_TO_COMPOSE_SUBCOMMAND = {
    "stop": "stop",
    "start": "start",
    "reload": "restart",
}


@dataclass
class ComposeRunResult:
    # compose 执行结果结构，供 action_executor 直接映射响应与错误码。
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
        # 命令执行超时（秒），默认 15s，可由环境变量覆盖。
        self.timeout_sec = timeout_sec or float(os.getenv("DEMO_COMPOSE_TIMEOUT_SEC", "15"))
        # 以仓库根目录为执行 cwd，确保 compose 文件相对路径稳定。
        self.repo_root = Path(__file__).resolve().parents[3]
        raw_compose = compose_file or os.getenv("DEMO_COMPOSE_FILE", "deploy/docker-compose.yml")
        # 对外展示命令时保留原始路径，便于与设计文档对齐。
        self.compose_file_display = raw_compose
        compose_path = Path(raw_compose)
        if compose_path.is_absolute():
            self.compose_file = compose_path
        else:
            self.compose_file = (self.repo_root / compose_path).resolve()

    def run(self, normalized_action: str, target: str, dry_run: bool = False) -> ComposeRunResult:
        # 组装真实执行命令（绝对路径）与展示命令（文档路径）。
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
        # 使用 monotonic 计时，避免系统时间跳变影响耗时计算。
        started_perf = _perf_counter()

        if dry_run:
            # dry_run 仅返回命令与模拟结果，不触发任何实际容器操作。
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
            # 同步阻塞执行 compose，stdout/stderr 供回执和排障查看。
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
            # 超时场景仍返回部分输出尾部，便于定位卡在哪一步。
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
    # 统一 UTC 时间格式，方便前后端与日志系统对齐。
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _tail(text: str, max_chars: int = 600) -> str:
    # 仅保留输出尾部，避免回执内容过大。
    value = (text or "").strip()
    if len(value) <= max_chars:
        return value
    return value[-max_chars:]


def _perf_counter() -> float:
    # 局部导入可避免非 action 路径初始化额外依赖。
    import time

    return time.perf_counter()


def _elapsed_ms(started_perf: float) -> int:
    # 统一以毫秒返回耗时，满足 API 契约字段 duration_ms。
    import time

    return int((time.perf_counter() - started_perf) * 1000)
