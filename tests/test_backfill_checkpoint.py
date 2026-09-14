"""Tests for historical backfill month selection and checkpoint logic.

Pure date-math/dict logic — no Spark/Delta involved (that half of the
module is exercised on Databricks only, same as every other Spark writer
in this codebase).
"""
from datetime import date

import pytest

from src.orchestration.backfill_checkpoint import (
    BACKFILL_HISTORICAL_START,
    STATUS_FAILED,
    STATUS_GIVEN_UP,
    STATUS_SUCCESS,
    STATUS_UNAVAILABLE,
    build_checkpoint_row,
    buffered_entsoe_range,
    CheckpointEntry,
    CheckpointKey,
    month_date_range,
    month_is_complete,
    next_backfill_month,
    previous_complete_month,
    walk_backfill_months,
)


def test_previous_complete_month_within_same_year():
    assert previous_complete_month(date(2026, 9, 8)) == date(2026, 8, 1)


def test_previous_complete_month_crosses_year_boundary():
    assert previous_complete_month(date(2026, 1, 15)) == date(2025, 12, 1)


def test_month_date_range_regular_month():
    assert month_date_range(date(2026, 8, 1)) == (date(2026, 8, 1), date(2026, 8, 31))


def test_month_date_range_non_leap_february():
    assert month_date_range(date(2026, 2, 1)) == (date(2026, 2, 1), date(2026, 2, 28))


def test_month_date_range_leap_february():
    assert month_date_range(date(2028, 2, 1)) == (date(2028, 2, 1), date(2028, 2, 29))


def test_month_date_range_rejects_non_first_of_month():
    with pytest.raises(ValueError):
        month_date_range(date(2026, 8, 15))


def test_buffered_entsoe_range_pads_both_sides():
    assert buffered_entsoe_range(date(2026, 8, 1), buffer_days=2) == (
        date(2026, 7, 30), date(2026, 9, 2),
    )


def test_walk_backfill_months_stops_at_historical_start_inclusive():
    months = list(walk_backfill_months(reference_date=date(2015, 2, 15)))
    assert months == [date(2015, 1, 1)]


def test_walk_backfill_months_walks_backward_in_order():
    months = list(walk_backfill_months(reference_date=date(2026, 1, 1)))
    assert months[0] == date(2025, 12, 1)
    assert months[1] == date(2025, 11, 1)
    assert months[-1] == BACKFILL_HISTORICAL_START


def test_next_backfill_month_returns_none_when_everything_complete():
    assert next_backfill_month(lambda month: True, reference_date=date(2026, 9, 8)) is None


def test_next_backfill_month_returns_newest_incomplete_month():
    target = date(2026, 6, 1)

    def is_complete(month):
        return month != target

    assert next_backfill_month(is_complete, reference_date=date(2026, 9, 8)) == target


def test_month_is_complete_true_when_all_combos_done():
    entries = {
        ("entsoe", "IE", "load"): CheckpointEntry(STATUS_SUCCESS, 1),
        ("entsoe", "IE", "generation"): CheckpointEntry(STATUS_UNAVAILABLE, 1),
    }
    combos = [("entsoe", "IE", "load"), ("entsoe", "IE", "generation")]
    assert month_is_complete(entries, combos) is True


def test_month_is_complete_true_when_a_combo_has_given_up():
    # given_up counts as done too — one stuck combo must not block the
    # walker forever.
    entries = {
        ("entsoe", "IE", "load"): CheckpointEntry(STATUS_SUCCESS, 1),
        ("entsoe", "IE", "generation"): CheckpointEntry(STATUS_GIVEN_UP, 3),
    }
    combos = [("entsoe", "IE", "load"), ("entsoe", "IE", "generation")]
    assert month_is_complete(entries, combos) is True


def test_month_is_complete_false_when_a_combo_is_missing():
    entries = {("entsoe", "IE", "load"): CheckpointEntry(STATUS_SUCCESS, 1)}
    combos = [("entsoe", "IE", "load"), ("entsoe", "IE", "generation")]
    assert month_is_complete(entries, combos) is False


def test_month_is_complete_false_when_a_combo_failed():
    entries = {
        ("entsoe", "IE", "load"): CheckpointEntry(STATUS_SUCCESS, 1),
        ("entsoe", "IE", "generation"): CheckpointEntry(STATUS_FAILED, 1),
    }
    combos = [("entsoe", "IE", "load"), ("entsoe", "IE", "generation")]
    assert month_is_complete(entries, combos) is False


def test_build_checkpoint_row_shape():
    key = CheckpointKey(source="entsoe", country_code="IE", dataset="load", month_start=date(2026, 8, 1))
    row = build_checkpoint_row(key, STATUS_SUCCESS, attempt_count=1, completed_at="2026-09-08T00:00:00+00:00")

    assert row["source"] == "entsoe"
    assert row["country_code"] == "IE"
    assert row["dataset"] == "load"
    assert row["month_start"] == "2026-08-01"
    assert row["status"] == STATUS_SUCCESS
    assert row["attempt_count"] == 1
    assert row["completed_at"] == "2026-09-08T00:00:00+00:00"
    assert "updated_at" in row and row["updated_at"]
