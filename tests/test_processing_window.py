"""Tests for the canonical processing-window resolution (Spec 006)."""
from datetime import date

import pytest

from src.orchestration.processing_window import (
    ProcessingWindowError,
    latest_completed_date,
    resolve_entsoe_window,
    resolve_open_meteo_window,
)

REFERENCE = date(2026, 9, 4)  # a run "today" — yesterday is 2026-09-03


def test_latest_completed_date_is_yesterday_relative_to_reference():
    assert latest_completed_date(REFERENCE) == date(2026, 9, 3)


def test_open_meteo_daily_mode_processes_only_the_latest_completed_date():
    start, end = resolve_open_meteo_window("daily", reference_date=REFERENCE)
    assert start == end == date(2026, 9, 3)


def test_open_meteo_backfill_mode_uses_explicit_range():
    start, end = resolve_open_meteo_window(
        "backfill", start_date=date(2024, 1, 1), end_date=date(2024, 12, 31)
    )
    assert (start, end) == (date(2024, 1, 1), date(2024, 12, 31))


def test_open_meteo_reprocess_mode_uses_explicit_range():
    start, end = resolve_open_meteo_window(
        "reprocess", start_date=date(2026, 8, 1), end_date=date(2026, 8, 3)
    )
    assert (start, end) == (date(2026, 8, 1), date(2026, 8, 3))


def test_open_meteo_backfill_without_explicit_dates_raises():
    with pytest.raises(ProcessingWindowError):
        resolve_open_meteo_window("backfill")


def test_unsupported_execution_mode_raises():
    with pytest.raises(ProcessingWindowError):
        resolve_open_meteo_window("weekly")


def test_entsoe_daily_mode_uses_the_configured_lookback():
    start, end = resolve_entsoe_window("daily", lookback_days=3, reference_date=REFERENCE)
    assert end == date(2026, 9, 3)
    assert start == date(2026, 9, 1)  # 3 calendar days: Sep 1, 2, 3


def test_entsoe_daily_mode_lookback_is_not_hardcoded():
    start, end = resolve_entsoe_window("daily", lookback_days=7, reference_date=REFERENCE)
    assert (end - start).days == 6  # 7 calendar days inclusive


def test_entsoe_daily_mode_rejects_non_positive_lookback():
    with pytest.raises(ProcessingWindowError):
        resolve_entsoe_window("daily", lookback_days=0, reference_date=REFERENCE)


def test_entsoe_backfill_mode_uses_explicit_range():
    start, end = resolve_entsoe_window(
        "backfill", start_date=date(2024, 1, 1), end_date=date(2024, 12, 31)
    )
    assert (start, end) == (date(2024, 1, 1), date(2024, 12, 31))


def test_entsoe_reprocess_without_explicit_dates_raises():
    with pytest.raises(ProcessingWindowError):
        resolve_entsoe_window("reprocess")


def test_entsoe_start_after_end_raises():
    with pytest.raises(ProcessingWindowError):
        resolve_entsoe_window("backfill", start_date=date(2024, 6, 1), end_date=date(2024, 1, 1))
