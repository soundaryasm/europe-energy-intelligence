"""Tests for the incremental Silver / serving date window (Spec 006)."""
from datetime import date

import pytest

from src.transformations.silver_window import (
    DEFAULT_PAD_DAYS,
    bronze_bounds,
    date_key_bounds,
)


def test_bronze_bounds_pad_a_whole_day_either_side():
    # Silver's grain is the country-local date while Bronze is UTC, so
    # the window's edge days draw on instants outside its UTC days.
    low, high = bronze_bounds(date(2026, 11, 1), date(2026, 11, 3))
    assert low == "2026-10-31"
    assert high == "2026-11-05"


def test_the_upper_bound_is_exclusive_and_admits_the_whole_last_day():
    # A lexicographic string compare is the point: an ISO timestamp
    # anywhere in 2026-11-04 must sort below the upper bound.
    _, high = bronze_bounds(date(2026, 11, 1), date(2026, 11, 3))
    assert "2026-11-04T23:45:00Z" < high
    assert "2026-11-05T00:00:00Z" >= high


def test_a_single_day_window_is_three_days_of_bronze():
    low, high = bronze_bounds(date(2026, 11, 3), date(2026, 11, 3))
    assert (low, high) == ("2026-11-02", "2026-11-05")


def test_padding_is_configurable_and_zero_is_allowed():
    assert bronze_bounds(date(2026, 11, 1), date(2026, 11, 1), pad_days=0) == (
        "2026-11-01", "2026-11-02",
    )


def test_the_default_pad_covers_european_offsets():
    # UTC+0..UTC+3 across the countries in scope; one whole day is ample
    # and needs no revisiting when a timezone's rules change.
    assert DEFAULT_PAD_DAYS >= 1


def test_an_inverted_window_is_rejected():
    with pytest.raises(ValueError, match="must not be after"):
        bronze_bounds(date(2026, 11, 3), date(2026, 11, 1))


def test_negative_padding_is_rejected():
    with pytest.raises(ValueError, match="pad_days"):
        bronze_bounds(date(2026, 11, 1), date(2026, 11, 3), pad_days=-1)


def test_date_key_bounds_match_dim_dates_yyyymmdd_integer():
    assert date_key_bounds(date(2015, 1, 1), date(2026, 11, 3)) == (20150101, 20261103)


def test_date_key_bounds_are_inclusive_on_both_ends():
    assert date_key_bounds(date(2026, 11, 3), date(2026, 11, 3)) == (20261103, 20261103)


def test_date_key_bounds_reject_an_inverted_window():
    with pytest.raises(ValueError, match="must not be after"):
        date_key_bounds(date(2026, 11, 3), date(2026, 11, 1))


def test_the_full_history_window_spans_every_loaded_year():
    # A full-history range, as `--params start_date=2015-01-01` passes.
    low, high = bronze_bounds(date(2015, 1, 1), date(2026, 11, 3))
    assert low == "2014-12-31"
    assert high == "2026-11-05"
