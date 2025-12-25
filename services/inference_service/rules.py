from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass
class InferenceResult:
    achievement_pct: float
    gap_tmt: float
    flags: list[str]


def compute_achievement(actual_tmt: float, target_tmt: float) -> float:
    if target_tmt == 0:
        return 0.0
    return round((actual_tmt / target_tmt) * 100.0, 2)


def compute_gap(actual_tmt: float, target_tmt: float) -> float:
    return round(actual_tmt - target_tmt, 3)


def flag_underperformance(achievement_pct: float, threshold: float = 90.0) -> bool:
    return achievement_pct < threshold


def flag_declining_trend(values: Iterable[float]) -> bool:
    values_list = list(values)
    if len(values_list) < 3:
        return False
    return values_list[-1] < values_list[-2] < values_list[-3]


def flag_key_driver(contribution_pct: float, threshold: float = 30.0) -> bool:
    return contribution_pct > threshold


def run_inference(actual_tmt: float, target_tmt: float, trend_values: Iterable[float] | None = None,
                  contribution_pct: float | None = None) -> InferenceResult:
    achievement_pct = compute_achievement(actual_tmt, target_tmt)
    gap_tmt = compute_gap(actual_tmt, target_tmt)

    flags: list[str] = []
    if flag_underperformance(achievement_pct):
        flags.append("UNDERPERFORMANCE")

    if trend_values is not None and flag_declining_trend(trend_values):
        flags.append("DECLINING_TREND")

    if contribution_pct is not None and flag_key_driver(contribution_pct):
        flags.append("KEY_DRIVER")

    return InferenceResult(achievement_pct=achievement_pct, gap_tmt=gap_tmt, flags=flags)
