"""Whole-month data-coverage verification for the backfill orchestrator.

The backfill walker must only advance past a month once every day in it
is genuinely accounted for — real data, or a confirmed legitimate
absence — never merely because *some* row exists for that month
(see project memory `project_backfill_architecture_plan`: "does a
record exist" is not the same question as "does the whole month have
data", and conflating them can silently skip past a partially-ingested
month forever).

Pure Python: the caller queries Bronze (or an ingestion result) for the
actual covered dates and hands them in here as a plain `set`/`frozenset`
of `date` objects — no Spark dependency in this module.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import FrozenSet, List

from src.orchestration.backfill_checkpoint import (
    STATUS_FAILED,
    STATUS_SUCCESS,
    STATUS_UNAVAILABLE,
    month_date_range,
)


@dataclass(frozen=True)
class MonthCoverage:
    month_start: date
    expected_dates: FrozenSet[date]
    covered_dates: FrozenSet[date]
    missing_dates: List[date] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return not self.missing_dates

    @property
    def is_empty(self) -> bool:
        return len(self.covered_dates) == 0


def _calendar_dates(month_start: date) -> FrozenSet[date]:
    start, end = month_date_range(month_start)
    days = (end - start).days + 1
    return frozenset(start + timedelta(days=i) for i in range(days))


def evaluate_month_coverage(month_start: date, covered_dates: FrozenSet[date]) -> MonthCoverage:
    """Compare `covered_dates` (whatever Bronze actually has for one
    country/dataset, restricted to this month) against every calendar
    day the month should have. Dates in `covered_dates` outside the
    month are ignored — callers may pass in a buffered ENTSO-E range's
    results untrimmed.
    """
    expected = _calendar_dates(month_start)
    covered_within_month = frozenset(d for d in covered_dates if d in expected)
    missing = sorted(expected - covered_within_month)
    return MonthCoverage(
        month_start=month_start,
        expected_dates=expected,
        covered_dates=covered_within_month,
        missing_dates=missing,
    )


def classify_month_result(coverage: MonthCoverage, ingestion_reported_no_data: bool) -> str:
    """Decide the checkpoint status for one (source, country, dataset,
    month) combination.

    `success`: every calendar day in the month has real data.
    `unavailable`: zero data for the whole month AND the ingestion run
    itself explicitly reported "no data" for every window it tried —
    a confirmed, legitimate absence, not a guess from an empty result
    alone (a technical failure can also produce zero rows).
    `failed`: anything else — including a *partial* month (some days
    covered, some not), which must never be treated as good enough to
    advance past. This is the exact "only whole-month coverage counts"
    rule this module exists to enforce.
    """
    if coverage.is_complete:
        return STATUS_SUCCESS
    if coverage.is_empty and ingestion_reported_no_data:
        return STATUS_UNAVAILABLE
    return STATUS_FAILED
