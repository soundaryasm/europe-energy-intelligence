"""Tests for whole-month backfill data-coverage verification.

The core invariant under test: a month is only ever "done" when every
calendar day is accounted for — never because *a* record exists
somewhere in it (that's exactly the gap this module exists to close).
"""
from datetime import date

from src.orchestration.backfill_checkpoint import STATUS_FAILED, STATUS_SUCCESS, STATUS_UNAVAILABLE
from src.orchestration.backfill_completeness import classify_month_result, evaluate_month_coverage


def _all_august_2026_dates():
    return frozenset(date(2026, 8, d) for d in range(1, 32))


def test_evaluate_month_coverage_full_month_is_complete():
    coverage = evaluate_month_coverage(date(2026, 8, 1), _all_august_2026_dates())

    assert coverage.is_complete is True
    assert coverage.missing_dates == []


def test_evaluate_month_coverage_partial_month_lists_missing_dates():
    covered = _all_august_2026_dates() - {date(2026, 8, 15), date(2026, 8, 16)}

    coverage = evaluate_month_coverage(date(2026, 8, 1), covered)

    assert coverage.is_complete is False
    assert coverage.missing_dates == [date(2026, 8, 15), date(2026, 8, 16)]


def test_evaluate_month_coverage_empty_month():
    coverage = evaluate_month_coverage(date(2026, 8, 1), frozenset())

    assert coverage.is_complete is False
    assert coverage.is_empty is True
    assert len(coverage.missing_dates) == 31


def test_evaluate_month_coverage_ignores_dates_outside_the_month():
    # A buffered ENTSO-E fetch (e.g. Jul 30 - Sep 2 for an August month)
    # must not count buffer-only dates toward August's own completeness.
    covered = _all_august_2026_dates() | {date(2026, 7, 30), date(2026, 9, 1)}

    coverage = evaluate_month_coverage(date(2026, 8, 1), covered)

    assert coverage.is_complete is True
    assert date(2026, 7, 30) not in coverage.covered_dates


def test_classify_month_result_success_when_fully_covered():
    coverage = evaluate_month_coverage(date(2026, 8, 1), _all_august_2026_dates())

    assert classify_month_result(coverage, ingestion_reported_no_data=False) == STATUS_SUCCESS


def test_classify_month_result_unavailable_when_empty_and_confirmed_no_data():
    coverage = evaluate_month_coverage(date(2026, 8, 1), frozenset())

    assert classify_month_result(coverage, ingestion_reported_no_data=True) == STATUS_UNAVAILABLE


def test_classify_month_result_failed_when_empty_but_not_confirmed_no_data():
    # Zero rows without an explicit "no data" acknowledgement could be a
    # technical failure, not a legitimate absence — must not be waved
    # through as `unavailable`.
    coverage = evaluate_month_coverage(date(2026, 8, 1), frozenset())

    assert classify_month_result(coverage, ingestion_reported_no_data=False) == STATUS_FAILED


def test_classify_month_result_failed_when_partial_even_if_ingestion_reported_no_data():
    # The exact scenario this module exists to catch: some days present,
    # so this is not a legitimate whole-month absence — it must stay
    # `failed`/retryable, never waved through as good enough.
    covered = _all_august_2026_dates() - {date(2026, 8, 20)}
    coverage = evaluate_month_coverage(date(2026, 8, 1), covered)

    assert classify_month_result(coverage, ingestion_reported_no_data=True) == STATUS_FAILED
