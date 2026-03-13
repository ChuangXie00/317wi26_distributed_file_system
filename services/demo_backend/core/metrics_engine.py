from dataclasses import dataclass
from typing import Any

from core.event_engine import EventEngine


@dataclass
class MetricsResult:
    # 指标查询结果：has_sample 用于 API 决定返回 200 还是 503。
    has_sample: bool
    data: dict[str, Any]


class MetricsEngine:
    def __init__(self, event_engine: EventEngine, sampling_window_seconds: int = 900) -> None:
        # 指标由事件引擎维护，本类只负责对外读取与无样本策略。
        self._event_engine = event_engine
        self._sampling_window_seconds = sampling_window_seconds

    def collect(self) -> MetricsResult:
        # 汇总指标快照并判定是否已有有效恢复样本。
        data = self._event_engine.get_metrics_snapshot(
            sampling_window_seconds=self._sampling_window_seconds
        )
        has_sample = self._event_engine.has_recovery_sample()

        # 无样本时写入 recovery.incomplete 事件，满足 8890 可观测要求。
        if not has_sample:
            self._event_engine.emit_recovery_incomplete_if_needed()
            data = self._event_engine.get_metrics_snapshot(
                sampling_window_seconds=self._sampling_window_seconds
            )

        return MetricsResult(has_sample=has_sample, data=data)
