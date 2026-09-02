"""Pure calculation helpers for Silver aggregation (Spec 003).

Deliberately dependency-free (no PySpark) so the actual math — interval
energy conversion, weighted averages, zero-safe percentages — is unit
tested directly in plain Python, then reused (via UDFs, where it isn't
natively expressible as a Spark column expression) inside the
Databricks-only Silver builders in this package. This is the real
business logic; nothing here is a stand-in for it.
"""
from __future__ import annotations

from typing import Iterable, Optional, Tuple


def energy_mwh(power_mw: float, interval_hours: float) -> float:
    """Convert a power reading over one source interval into energy.

    Spec 003: `energy_mwh = load_mw x interval_duration_hours`.
    """
    return power_mw * interval_hours


def safe_percentage(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    """Return numerator/denominator*100, or None when not calculable.

    Spec 003/004: "Handle zero-generation cases explicitly. Do not
    silently divide by zero." A None result (not 0) signals "not
    calculable" so it is never mistaken for a real 0%.
    """
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return (numerator / denominator) * 100.0


def weighted_average(values_and_weights: Iterable[Tuple[Optional[float], Optional[float]]]) -> Optional[float]:
    """Interval-duration-weighted average (Spec 003: day-ahead price across mixed resolutions).

    Entries with a None value or None/zero weight are skipped rather than
    treated as 0, per the spec's null-handling rules.
    """
    total_weighted = 0.0
    total_weight = 0.0
    for value, weight in values_and_weights:
        if value is None or weight is None or weight <= 0:
            continue
        total_weighted += value * weight
        total_weight += weight

    if total_weight == 0:
        return None
    return total_weighted / total_weight
