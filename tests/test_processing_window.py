"""Tests for the canonical processing-window resolution (Spec 006).

One function, one rule: two optional dates and a default width. The
previous version of this file tested the same logic twice over, once per
source, through `resolve_entsoe_window` and `resolve_open_meteo_window`
— which were byte-identical once the mode was known — and again per
`execution_mode`. All of that collapsed into `resolve_window`.
"""
from datetime import date

import pytest

from src.orchestration.processing_window import (
    DEFAULT_LOOKBACK_DAYS,
    ENTSOE_FETCH_PAD_DAYS,
    ProcessingWindowError,
    entsoe_fetch_window,
    latest_completed_date,
    resolve_window,
)

REFERENCE = date(2026, 9, 4)  # a run "today" — yesterday is 2026-09-03
YESTERDAY = date(2026, 9, 3)


def test_latest_completed_date_is_yesterday_relative_to_reference():
    assert latest_completed_date(REFERENCE) == YESTERDAY


class TestDefaults:
    """What the schedule gets: no dates at all."""

    def test_no_dates_resolves_to_the_trailing_lookback(self):
        start, end = resolve_window(lookback_days=3, reference_date=REFERENCE)
        assert end == YESTERDAY
        assert start == date(2026, 9, 1)  # 3 calendar days: Sep 1, 2, 3

    def test_the_window_is_inclusive_of_both_ends(self):
        start, end = resolve_window(lookback_days=7, reference_date=REFERENCE)
        assert (end - start).days == 6  # 7 calendar days inclusive

    def test_the_default_lookback_is_three_days(self):
        # Spec 006: "Initial MVP lookback: 3 calendar days."
        assert DEFAULT_LOOKBACK_DAYS == 3
        start, end = resolve_window(reference_date=REFERENCE)
        assert (start, end) == (date(2026, 9, 1), YESTERDAY)

    def test_a_non_positive_lookback_raises(self):
        with pytest.raises(ProcessingWindowError):
            resolve_window(lookback_days=0, reference_date=REFERENCE)


class TestExplicitDates:
    """What a manual run sends: either date, or both."""

    def test_both_dates_are_used_verbatim(self):
        assert resolve_window(
            start_date=date(2024, 1, 1), end_date=date(2024, 12, 31)
        ) == (date(2024, 1, 1), date(2024, 12, 31))

    def test_a_start_alone_runs_to_the_latest_completed_date(self):
        # The one-day repair case: give a start, let the end follow today.
        assert resolve_window(
            start_date=date(2026, 9, 1), reference_date=REFERENCE
        ) == (date(2026, 9, 1), YESTERDAY)

    def test_an_end_alone_still_applies_the_lookback_behind_it(self):
        assert resolve_window(
            end_date=date(2026, 6, 10), lookback_days=3
        ) == (date(2026, 6, 8), date(2026, 6, 10))

    def test_lookback_is_ignored_when_a_start_is_given(self):
        # An explicit start is already the answer lookback would compute,
        # so the two must not fight.
        assert resolve_window(
            start_date=date(2015, 1, 1), lookback_days=3, reference_date=REFERENCE
        ) == (date(2015, 1, 1), YESTERDAY)

    def test_a_single_day_window_is_allowed(self):
        day = date(2026, 11, 1)
        assert resolve_window(start_date=day, end_date=day) == (day, day)

    def test_start_after_end_raises(self):
        with pytest.raises(ProcessingWindowError):
            resolve_window(start_date=date(2024, 6, 1), end_date=date(2024, 1, 1))


class TestEntsoeFetchWindow:
    """The fetch is wider than the window, by one day at the start.

    ENTSO-E reads request dates as 00:00 UTC, but Silver's grain is the
    country-local calendar day, which for any UTC+ zone begins before
    00:00 UTC. Without this pad the oldest day of every window loses its
    first local hour, Silver measures 23 covered hours and marks the day
    `partial`, and Gold withholds the metric. Observed for real: a
    one-day run for 2026-11-01 left DE with zero intervals at
    2026-10-31 23:00 UTC and 13 of 15 countries `partial`.
    """

    def test_the_fetch_starts_a_day_before_the_window(self):
        start, end = date(2026, 11, 1), date(2026, 11, 1)
        assert entsoe_fetch_window(start, end) == (date(2026, 10, 31), date(2026, 11, 1))

    def test_the_end_is_untouched(self):
        _, end = entsoe_fetch_window(date(2026, 11, 1), date(2026, 11, 3))
        assert end == date(2026, 11, 3)

    def test_the_pad_is_one_day(self):
        # A local day can precede UTC by at most a few hours, so one
        # calendar day always covers it.
        assert ENTSOE_FETCH_PAD_DAYS == 1

    def test_padding_applies_to_the_scheduled_window_too(self):
        # Not a reprocess-only fix: the oldest day of any window is
        # exposed, the nightly one included.
        start, end = resolve_window(lookback_days=3, reference_date=REFERENCE)
        fetch_start, _ = entsoe_fetch_window(start, end)
        assert fetch_start == start - __import__("datetime").timedelta(days=1)
