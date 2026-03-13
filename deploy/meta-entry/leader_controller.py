#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def iso_from_ts(ts: float) -> str:
    value = float(ts)
    if value <= 0:
        return ""
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_node_id(raw: str) -> str:
    return str(raw or "").strip().lower()


def parse_csv_nodes(raw: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in str(raw or "").split(","):
        node = normalize_node_id(item)
        if not node or node in seen:
            continue
        seen.add(node)
        out.append(node)
    return out


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def split_shell_words(command: str, fallback: List[str]) -> List[str]:
    parts = [item.strip() for item in str(command or "").split(" ") if item.strip()]
    return parts if parts else fallback


@dataclass
class ControllerConfig:
    # 集群探测与 leader 查询参数。
    meta_nodes: List[str]
    meta_internal_port: int
    leader_query_path: str
    # 切换节奏控制参数。
    refresh_interval_sec: float
    switch_debounce_sec: float
    switch_cooldown_sec: float
    request_timeout_sec: float
    require_single_writable_leader: bool
    # 默认目标与动态配置落盘路径。
    default_leader_id: str
    upstream_conf_path: str
    status_path: str
    # nginx 变更执行参数。
    enable_nginx_reload: bool
    nginx_test_cmd: List[str]
    nginx_reload_cmd: List[str]

    @classmethod
    def from_env(cls) -> "ControllerConfig":
        # 读取并规范化 meta 节点列表，避免空值或重复节点影响探测结果。
        meta_nodes = parse_csv_nodes(os.getenv("ENTRY_META_NODES", "meta-01,meta-02,meta-03"))
        default_leader = normalize_node_id(os.getenv("ENTRY_DEFAULT_LEADER_ID", "meta-01"))
        if not meta_nodes:
            meta_nodes = [default_leader or "meta-01"]
        if default_leader and default_leader not in meta_nodes:
            meta_nodes = [default_leader] + meta_nodes

        # 允许通过环境变量覆盖 leader 查询路径，默认走内部统一出口。
        query_path = str(os.getenv("ENTRY_LEADER_QUERY_PATH", "/internal/current_leader")).strip() or "/internal/current_leader"
        if not query_path.startswith("/"):
            query_path = f"/{query_path}"

        # nginx 执行命令支持配置化，便于本地与容器环境复用同一控制器。
        nginx_test_cmd = split_shell_words(os.getenv("ENTRY_NGINX_TEST_CMD", "nginx -t"), ["nginx", "-t"])
        nginx_reload_cmd = split_shell_words(
            os.getenv("ENTRY_NGINX_RELOAD_CMD", "nginx -s reload"),
            ["nginx", "-s", "reload"],
        )

        return cls(
            meta_nodes=meta_nodes,
            meta_internal_port=max(1, safe_int(os.getenv("ENTRY_META_INTERNAL_PORT", "8000"), 8000)),
            leader_query_path=query_path,
            refresh_interval_sec=max(0.2, float(os.getenv("ENTRY_REFRESH_INTERVAL_SEC", "1.5"))),
            switch_debounce_sec=max(0.0, float(os.getenv("ENTRY_SWITCH_DEBOUNCE_SEC", "2.0"))),
            switch_cooldown_sec=max(0.0, float(os.getenv("ENTRY_SWITCH_COOLDOWN_SEC", "6.0"))),
            request_timeout_sec=max(0.1, float(os.getenv("ENTRY_REQUEST_TIMEOUT_SEC", "1.2"))),
            # 开启后，entry 仅在“唯一可写 leader”成立时才允许切换。
            require_single_writable_leader=str(os.getenv("ENTRY_REQUIRE_SINGLE_WRITABLE_LEADER", "0")).strip().lower()
            in {"1", "true", "yes", "on"},
            default_leader_id=default_leader or meta_nodes[0],
            upstream_conf_path=str(os.getenv("ENTRY_UPSTREAM_CONF_PATH", "/etc/nginx/conf.d/meta_upstream.conf")).strip()
            or "/etc/nginx/conf.d/meta_upstream.conf",
            status_path=str(os.getenv("ENTRY_STATUS_PATH", "/var/lib/meta-entry/debug/entry_status.json")).strip()
            or "/var/lib/meta-entry/debug/entry_status.json",
            enable_nginx_reload=str(os.getenv("ENTRY_ENABLE_NGINX_RELOAD", "1")).strip().lower() in {"1", "true", "yes", "on"},
            nginx_test_cmd=nginx_test_cmd,
            nginx_reload_cmd=nginx_reload_cmd,
        )


class LeaderSwitchController:
    def __init__(self, config: ControllerConfig) -> None:
        self.config = config
        # active_* 代表当前实际生效的路由目标。
        self.active_leader_id = config.default_leader_id
        self.active_since_ts = 0.0
        # pending_* 代表候选目标，需通过防抖与冷却后才可生效。
        self.pending_leader_id = ""
        self.pending_since_ts = 0.0
        # 最近一次切换与决策结果，供 /debug/entry 直接观测。
        self.last_switch_at_ts = 0.0
        self.last_switch_reason = "bootstrap"
        self.last_decision = "bootstrap"
        self.last_error = ""
        self.poll_seq = 0

        # 启动时尽量复用上次状态，避免重启后立即误切。
        self._load_previous_status()
        self._ensure_upstream_file_initialized()
        self._write_status([], [], {}, self._build_probe_consensus([]))

    def run(self, once: bool = False) -> None:
        # 常驻循环：探测 -> 决策 -> 落盘，按固定节拍执行。
        while True:
            started_at = time.time()
            self._run_once()
            if once:
                return
            elapsed = time.time() - started_at
            sleep_sec = max(0.05, float(self.config.refresh_interval_sec) - elapsed)
            time.sleep(sleep_sec)

    def _run_once(self) -> None:
        self.poll_seq += 1
        now_ts = time.time()
        # 先收集观测样本，再按统一规则选候选 leader。
        success_items, error_items = self._probe_meta_nodes()
        consensus = self._build_probe_consensus(success_items)
        candidate_leader_id, candidate_epoch, selection_reason = self._select_candidate_leader(success_items)
        # 切换决策会处理防抖、冷却和 reload 成功性。
        decision = self._decide_switch(now_ts, candidate_leader_id, candidate_epoch, selection_reason, consensus)
        self._write_status(success_items, error_items, decision, consensus)

    def _load_previous_status(self) -> None:
        status_path = Path(self.config.status_path)
        if not status_path.exists():
            return
        try:
            payload = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            # 状态文件损坏时按冷启动处理，不中断主流程。
            return
        if not isinstance(payload, dict):
            return

        previous_leader = normalize_node_id(payload.get("active_leader_id", ""))
        if previous_leader:
            self.active_leader_id = previous_leader
        self.active_since_ts = float(payload.get("active_since_ts", 0.0) or 0.0)
        self.pending_leader_id = normalize_node_id(payload.get("pending_leader_id", ""))
        self.pending_since_ts = float(payload.get("pending_since_ts", 0.0) or 0.0)
        self.last_switch_at_ts = float(payload.get("last_switch_at_ts", 0.0) or 0.0)
        self.last_switch_reason = str(payload.get("last_switch_reason", "bootstrap"))
        self.last_decision = str(payload.get("last_decision", "bootstrap"))

    def _probe_meta_nodes(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        success_items: List[Dict[str, Any]] = []
        error_items: List[Dict[str, Any]] = []
        for node_id in self.config.meta_nodes:
            query_url = f"http://{node_id}:{self.config.meta_internal_port}{self.config.leader_query_path}"
            started = time.time()
            try:
                # 每个节点独立探测，单点超时不会拖垮整轮决策。
                request = Request(query_url, method="GET")
                with urlopen(request, timeout=self.config.request_timeout_sec) as response:
                    body_raw = response.read()
                    body = json.loads(body_raw.decode("utf-8")) if body_raw else {}
                    if response.status != 200 or not isinstance(body, dict):
                        raise RuntimeError(f"invalid probe response status={response.status}")

                observed_leader = normalize_node_id(body.get("current_leader_id", ""))
                leader_epoch = max(0, safe_int(body.get("leader_epoch", 0), 0))
                node_role = str(body.get("role", "follower")).strip().lower()
                writable_leader = bool(body.get("writable_leader", False))
                observed_node_id = normalize_node_id(body.get("node_id", node_id)) or node_id
                success_items.append(
                    {
                        "probe_node_id": node_id,
                        "node_id": observed_node_id,
                        "query_url": query_url,
                        "role": node_role,
                        "writable_leader": writable_leader,
                        "current_leader_id": observed_leader,
                        "leader_epoch": leader_epoch,
                        "current_term": max(0, safe_int(body.get("current_term", 0), 0)),
                        "lamport": max(0, safe_int(body.get("lamport", 0), 0)),
                        "observed_at": str(body.get("observed_at", "")),
                        "latency_ms": int((time.time() - started) * 1000),
                    }
                )
            except (HTTPError, URLError, OSError, ValueError, RuntimeError) as exc:
                error_items.append(
                    {
                        "probe_node_id": node_id,
                        "query_url": query_url,
                        # 保留错误明细，便于定位 DNS、连接超时或接口异常。
                        "error": str(exc),
                        "latency_ms": int((time.time() - started) * 1000),
                    }
                )
        return success_items, error_items

    def _build_probe_consensus(self, success_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        # 汇总本轮探测的一致性信号，供切换门禁与 debug 观测共用。
        writable_nodes = set()
        observed_leader_ids = set()
        for item in success_items:
            node_id = normalize_node_id(item.get("node_id", ""))
            if item.get("writable_leader") and node_id:
                writable_nodes.add(node_id)
            leader_id = normalize_node_id(item.get("current_leader_id", ""))
            if leader_id:
                observed_leader_ids.add(leader_id)

        sorted_writable_nodes = sorted(writable_nodes)
        sorted_observed_leaders = sorted(observed_leader_ids)
        unique_writable_leader_id = sorted_writable_nodes[0] if len(sorted_writable_nodes) == 1 else ""
        return {
            "require_single_writable_leader": bool(self.config.require_single_writable_leader),
            "single_writable_leader": len(sorted_writable_nodes) == 1,
            "writable_leader_nodes": sorted_writable_nodes,
            "unique_writable_leader_id": unique_writable_leader_id,
            "observed_leader_ids": sorted_observed_leaders,
        }

    def _select_candidate_leader(
        self, success_items: List[Dict[str, Any]]
    ) -> Tuple[str, int, str]:
        ranked_candidates: List[Tuple[int, str]] = []
        for item in success_items:
            leader_id = normalize_node_id(item.get("current_leader_id", ""))
            if not leader_id:
                continue
            leader_epoch = max(0, safe_int(item.get("leader_epoch", 0), 0))
            ranked_candidates.append((leader_epoch, leader_id))

        if ranked_candidates:
            # 优先更高 epoch；同 epoch 下按 leader_id 稳定收敛，避免随机切换。
            ranked_candidates.sort(key=lambda pair: (pair[0], pair[1]), reverse=True)
            candidate_epoch, candidate_leader = ranked_candidates[0]
            return candidate_leader, candidate_epoch, "best_epoch_leader"

        # 本轮无有效 leader 观测时，保持现有目标以降低抖动风险。
        fallback_leader = self.active_leader_id or self.config.default_leader_id
        return fallback_leader, 0, "fallback_to_active_or_default"

    def _decide_switch(
        self,
        now_ts: float,
        candidate_leader_id: str,
        candidate_epoch: int,
        selection_reason: str,
        consensus: Dict[str, Any],
    ) -> Dict[str, Any]:
        decision: Dict[str, Any] = {
            "candidate_leader_id": candidate_leader_id,
            "candidate_epoch": int(candidate_epoch),
            "selection_reason": selection_reason,
            "switched": False,
            "switch_blocked_by": "",
        }

        # 候选与当前一致时清空 pending，保持稳定状态。
        if candidate_leader_id == self.active_leader_id:
            self.pending_leader_id = ""
            self.pending_since_ts = 0.0
            self.last_decision = "stable_active_leader"
            decision["decision"] = self.last_decision
            return decision

        if self.config.require_single_writable_leader:
            writable_nodes = list(consensus.get("writable_leader_nodes", []))
            unique_writable_leader = normalize_node_id(consensus.get("unique_writable_leader_id", ""))
            if not bool(consensus.get("single_writable_leader", False)):
                self.pending_leader_id = ""
                self.pending_since_ts = 0.0
                self.last_decision = "switch_blocked_no_single_writable_leader"
                decision["decision"] = self.last_decision
                decision["switch_blocked_by"] = "single_writable_leader"
                decision["writable_leader_nodes"] = writable_nodes
                return decision

            # 只有唯一可写 leader 与候选 leader 一致时才允许进入切换路径。
            if unique_writable_leader and unique_writable_leader != candidate_leader_id:
                self.pending_leader_id = ""
                self.pending_since_ts = 0.0
                self.last_decision = "switch_blocked_writable_leader_mismatch"
                decision["decision"] = self.last_decision
                decision["switch_blocked_by"] = "writable_leader_mismatch"
                decision["unique_writable_leader_id"] = unique_writable_leader
                return decision

        # 首次观察到新候选时只进入 pending，不立即切流。
        if candidate_leader_id != self.pending_leader_id:
            self.pending_leader_id = candidate_leader_id
            self.pending_since_ts = now_ts
            self.last_decision = "pending_new_candidate"
            decision["decision"] = self.last_decision
            decision["pending_elapsed_sec"] = 0.0
            return decision

        # 防抖窗口内仅记录候选稳定时间，避免瞬时波动触发切换。
        pending_elapsed = max(0.0, now_ts - self.pending_since_ts)
        decision["pending_elapsed_sec"] = round(pending_elapsed, 3)
        if pending_elapsed < self.config.switch_debounce_sec:
            self.last_decision = "pending_wait_debounce"
            decision["decision"] = self.last_decision
            decision["switch_blocked_by"] = "debounce"
            return decision

        # 冷却窗口限制切换频率，防止连续主变更放大入口抖动。
        cooldown_elapsed = max(0.0, now_ts - self.last_switch_at_ts) if self.last_switch_at_ts > 0 else float("inf")
        decision["cooldown_elapsed_sec"] = round(cooldown_elapsed, 3) if cooldown_elapsed != float("inf") else None
        if cooldown_elapsed < self.config.switch_cooldown_sec:
            self.last_decision = "pending_wait_cooldown"
            decision["decision"] = self.last_decision
            decision["switch_blocked_by"] = "cooldown"
            return decision

        # 真正切换时需要先落盘 upstream，再执行 nginx 校验与 reload。
        switched, detail = self._apply_switch(candidate_leader_id)
        decision["switched"] = switched
        decision["switch_result"] = detail
        if switched:
            self.active_leader_id = candidate_leader_id
            self.active_since_ts = now_ts
            self.last_switch_at_ts = now_ts
            self.last_switch_reason = (
                f"switch_to={candidate_leader_id},"
                f"selection={selection_reason},"
                f"epoch={candidate_epoch}"
            )
            self.pending_leader_id = ""
            self.pending_since_ts = 0.0
            self.last_decision = "switched"
        else:
            self.last_decision = "switch_failed"
        decision["decision"] = self.last_decision
        return decision

    def _apply_switch(self, leader_id: str) -> Tuple[bool, str]:
        try:
            # 先尝试更新目标配置；内容不变时视为幂等成功。
            changed = self._write_upstream_conf(leader_id)
            if not changed:
                return True, "target_unchanged"
            if not self.config.enable_nginx_reload:
                return True, "reload_disabled"

            # reload 前先做配置检查，避免坏配置直接打断入口流量。
            self._run_cmd(self.config.nginx_test_cmd)
            self._run_cmd(self.config.nginx_reload_cmd)
            return True, "nginx_reloaded"
        except Exception as exc:  # pylint: disable=broad-except
            self.last_error = str(exc)
            return False, self.last_error

    def _write_upstream_conf(self, leader_id: str) -> bool:
        conf_path = Path(self.config.upstream_conf_path)
        conf_path.parent.mkdir(parents=True, exist_ok=True)

        target_url = f"http://{leader_id}:{self.config.meta_internal_port}"
        content = (
            "# auto-generated by entry leader controller\n"
            f"# generated_at={utc_now_iso()}\n"
            f"set $meta_upstream {target_url};\n"
            f"set $meta_leader_id {leader_id};\n"
        )
        current = ""
        if conf_path.exists():
            current = conf_path.read_text(encoding="utf-8")
        if current == content:
            return False

        # 采用临时文件 + 原子替换，避免并发读取时出现半写入内容。
        temp_path = conf_path.with_suffix(".tmp")
        temp_path.write_text(content, encoding="utf-8")
        os.replace(temp_path, conf_path)
        return True

    def _ensure_upstream_file_initialized(self) -> None:
        if self.active_since_ts <= 0:
            self.active_since_ts = time.time()
        try:
            # 启动阶段先写默认上游，确保 nginx 在首次请求前就有可用目标。
            self._write_upstream_conf(self.active_leader_id)
        except Exception as exc:  # pylint: disable=broad-except
            self.last_error = str(exc)

    def _write_status(
        self,
        success_items: List[Dict[str, Any]],
        error_items: List[Dict[str, Any]],
        decision: Dict[str, Any],
        consensus: Dict[str, Any],
    ) -> None:
        # 每轮决策都落盘，便于 /debug/entry 实时观测切换状态与探测细节。
        status_path = Path(self.config.status_path)
        status_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "status": "ok",
            "updated_at": utc_now_iso(),
            "poll_seq": self.poll_seq,
            "active_leader_id": self.active_leader_id,
            "active_target_url": f"http://{self.active_leader_id}:{self.config.meta_internal_port}",
            "active_since_ts": round(self.active_since_ts, 3),
            "active_since": iso_from_ts(self.active_since_ts),
            "pending_leader_id": self.pending_leader_id,
            "pending_since_ts": round(self.pending_since_ts, 3),
            "pending_since": iso_from_ts(self.pending_since_ts),
            "last_switch_at_ts": round(self.last_switch_at_ts, 3),
            "last_switch_at": iso_from_ts(self.last_switch_at_ts),
            "last_switch_reason": self.last_switch_reason,
            "last_decision": self.last_decision,
            "last_error": self.last_error,
            "switch_policy": {
                "refresh_interval_sec": self.config.refresh_interval_sec,
                "debounce_sec": self.config.switch_debounce_sec,
                "cooldown_sec": self.config.switch_cooldown_sec,
                "require_single_writable_leader": bool(self.config.require_single_writable_leader),
            },
            "probes": {
                "meta_nodes": self.config.meta_nodes,
                "success_count": len(success_items),
                "error_count": len(error_items),
                "successes": success_items,
                "errors": error_items,
            },
            "consensus": consensus,
            "decision": decision,
        }
        status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _run_cmd(command: List[str]) -> None:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            # 拼接 stdout/stderr 便于排查 reload 失败根因。
            raise RuntimeError(
                f"command failed: {' '.join(command)}; "
                f"stdout={completed.stdout.strip()}; stderr={completed.stderr.strip()}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="meta-entry leader switch controller")
    # 用于容器启动时做一次初始化写入，也便于本地快速验证单轮行为。
    parser.add_argument("--once", action="store_true", help="run one polling cycle and exit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    controller = LeaderSwitchController(config=ControllerConfig.from_env())
    controller.run(once=bool(args.once))


if __name__ == "__main__":
    main()
