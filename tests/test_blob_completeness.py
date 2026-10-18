"""Tests for whole-month coverage classification (Spec 010)."""
from datetime import date

from src.blob.completeness import (
    classify,
    covered_dates_from_records,
    evaluate_coverage,
    month_date_range,
    month_dates,
)
from src.blob.manifest import STATUS_FAILED, STATUS_SUCCESS, STATUS_UNAVAILABLE

MARCH = date(2024, 3, 1)
FEBRUARY_LEAP = date(2024, 2, 1)


def test_month_date_range_spans_the_whole_month():
    assert month_date_range(MARCH) == (date(2024, 3, 1), date(2024, 3, 31))


def test_month_date_range_handles_a_leap_february():
    assert month_date_range(FEBRUARY_LEAP)[1] == date(2024, 2, 29)


def test_month_dates_lists_every_calendar_day():
    assert len(month_dates(MARCH)) == 31
    assert len(month_dates(FEBRUARY_LEAP)) == 29


def test_a_fully_covered_month_is_complete():
    coverage = evaluate_coverage(MARCH, month_dates(MARCH))

    assert coverage.is_complete
    assert coverage.missing_days == 0


def test_a_partially_covered_month_is_not_complete():
    coverage = evaluate_coverage(MARCH, month_dates(MARCH)[:10])

    assert not coverage.is_complete
    assert coverage.covered_days == 10
    assert coverage.missing_days == 21


def test_dates_outside_the_month_do_not_count_as_coverage():
    # ENTSO-E windows deliberately overlap the month boundary so local
    # calendar dates reconstruct at the edges; those days are not
    # evidence of coverage inside the month.
    padded = [date(2024, 2, 27), date(2024, 2, 28)] + month_dates(MARCH) + [date(2024, 4, 1)]
    coverage = evaluate_coverage(MARCH, padded)

    assert coverage.covered_days == 31
    assert coverage.is_complete


def test_complete_coverage_classifies_as_success():
    coverage = evaluate_coverage(MARCH, month_dates(MARCH))

    assert classify(coverage, source_reported_no_data=False) == STATUS_SUCCESS


def test_partial_coverage_is_failed_even_when_the_source_reported_no_data():
    # A month that is partly there is retryable, never advanced past on
    # the grounds that it looks populated.
    coverage = evaluate_coverage(MARCH, month_dates(MARCH)[:5])

    assert classify(coverage, source_reported_no_data=True) == STATUS_FAILED


def test_an_empty_month_is_unavailable_only_when_the_source_said_so():
    coverage = evaluate_coverage(MARCH, [])

    assert classify(coverage, source_reported_no_data=True) == STATUS_UNAVAILABLE


def test_an_empty_month_without_that_signal_is_failed():
    # "Nothing came back" and "there is nothing there" are different
    # claims and must not be collapsed.
    coverage = evaluate_coverage(MARCH, [])

    assert classify(coverage, source_reported_no_data=False) == STATUS_FAILED


def test_covered_dates_reads_a_date_field():
    records = [
        {"country_code": "IE", "observation_date": "2024-03-01"},
        {"country_code": "IE", "observation_date": "2024-03-02"},
    ]

    assert covered_dates_from_records(records, date_field="observation_date") == frozenset(
        {date(2024, 3, 1), date(2024, 3, 2)}
    )


def test_covered_dates_reads_a_timestamp_field_at_day_grain():
    records = [{"country_code": "IE", "source_timestamp": "2024-03-01T23:00:00Z"}]

    assert covered_dates_from_records(records, date_field="source_timestamp") == frozenset(
        {date(2024, 3, 1)}
    )


def test_covered_dates_can_be_narrowed_by_country():
    records = [
        {"country_code": "IE", "observation_date": "2024-03-01"},
        {"country_code": "DE", "observation_date": "2024-03-02"},
    ]

    covered = covered_dates_from_records(
        records, date_field="observation_date", country_code="IE",
    )

    assert covered == frozenset({date(2024, 3, 1)})


def test_covered_dates_can_be_narrowed_by_dataset():
    records = [
        {"country_code": "IE", "dataset_type": "load", "source_timestamp": "2024-03-01"},
        {"country_code": "IE", "dataset_type": "price", "source_timestamp": "2024-03-02"},
    ]

    covered = covered_dates_from_records(
        records, date_field="source_timestamp", country_code="IE",
        dataset_field="dataset_type", dataset="load",
    )

    assert covered == frozenset({date(2024, 3, 1)})


def test_records_with_no_date_value_are_ignored():
    records = [{"country_code": "IE", "observation_date": None}]

    assert covered_dates_from_records(records, date_field="observation_date") == frozenset()
