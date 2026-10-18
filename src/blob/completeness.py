"""Whole-month coverage classification for the historical layer (Spec 010).

A month is only accounted for once every calendar date in it is covered.
A month with some days present and some missing is `failed` and
re-attempted — never advanced past on the grounds that it looks
populated. The distinction that matters is between a month the source
confirmed has no data (`unavailable`) and a month that merely came back
empty (`failed`): only the former is a legitimate end state.

Coverage is computed from the records an attempt just produced, in
process, rather than by querying what has already been stored. That
keeps this module free of any storage or engine dependency, and means
the classification reflects exactly what the attempt achieved.
"""
from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from typing import FrozenSet, Iterable, List, Optional

from src.blob.manifest import STATUS_FAILED, STATUS_SUCCESS, STATUS_UNAVAILABLE

_ONE_DAY = timedelta(days=1)


def month_date_range(month_start: date) -> tuple:
    """First and last calendar date of `month_start`'s month."""
    last_day = monthrange(month_start.year, month_start.month)[1]
    return month_start.replace(day=1), month_start.replace(day=last_day)


def month_dates(month_start: date) -> List[date]:
    """Every calendar date in `month_start`'s month."""
    first, last = month_date_range(month_start)
    dates = []
    current = first
    while current <= last:
        dates.append(current)
        current += _ONE_DAY
    return dates


@dataclass(frozen=True)
class MonthCoverage:
    """How much of one month one combination actually covers."""

    month_start: date
    expected_days: int
    covered_days: int

    @property
    def missing_days(self) -> int:
        return self.expected_days - self.covered_days

    @property
    def is_complete(self) -> bool:
        return self.covered_days == self.expected_days

    @property
    def is_empty(self) -> bool:
        return self.covered_days == 0


def evaluate_coverage(month_start: date, covered: Iterable[date]) -> MonthCoverage:
    """Compare the dates an attempt covered against the month's calendar dates.

    Dates outside the month are ignored rather than counted: a source
    window may deliberately overlap the month boundary so local
    calendar dates can be reconstructed at the edges, and those extra
    days are not evidence of coverage inside the month.
    """
    expected: FrozenSet[date] = frozenset(month_dates(month_start))
    covered_in_month = expected.intersection(set(covered))
    return MonthCoverage(
        month_start=month_start,
        expected_days=len(expected),
        covered_days=len(covered_in_month),
    )


def classify(coverage: MonthCoverage, *, source_reported_no_data: bool) -> str:
    """Classify one combination's month as success, unavailable or failed.

    `source_reported_no_data` is the source's own positive statement that
    it holds nothing for the window — an ENTSO-E acknowledgement
    document, for instance. Only that turns an empty month into
    `unavailable`; an empty month without it is `failed`, because
    "nothing came back" and "there is nothing there" are not the same
    claim and must not be collapsed.
    """
    if coverage.is_complete:
        return STATUS_SUCCESS
    if coverage.is_empty and source_reported_no_data:
        return STATUS_UNAVAILABLE
    return STATUS_FAILED


def covered_dates_from_records(
    records: Iterable[dict], *, date_field: str, country_code: Optional[str] = None,
    dataset_field: Optional[str] = None, dataset: Optional[str] = None,
) -> FrozenSet[date]:
    """Distinct calendar dates present in `records`, optionally narrowed.

    `date_field` holds either a date or a timestamp as an ISO-8601
    string; only its first ten characters are read, so both grains work
    without separate parsing paths.
    """
    dates = set()
    for record in records:
        if country_code is not None and record.get("country_code") != country_code:
            continue
        if dataset_field is not None and dataset is not None:
            if record.get(dataset_field) != dataset:
                continue
        raw = record.get(date_field)
        if not raw:
            continue
        dates.add(date.fromisoformat(str(raw)[:10]))
    return frozenset(dates)
