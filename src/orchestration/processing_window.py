"""Canonical processing-window resolution shared by every task (Spec 006).

Spec 006 requires the workflow to "derive a single canonical processing
window and pass it to downstream tasks" rather than "independently
calculate date ranges inside every task." This module is that single
place: given the job-level `execution_mode` / `start_date` / `end_date` /
`lookback_days` parameters, it resolves the concrete (start_date,
end_date) window each source should process.

Pure Python, no PySpark/Databricks dependency, fully unit-testable
locally.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone as dt_timezone
from typing import Optional, Tuple

DAILY = "daily"
BACKFILL = "backfill"
REPROCESS = "reprocess"
VALID_EXECUTION_MODES = (DAILY, BACKFILL, REPROCESS)

# Spec 006: "Initial MVP lookback: 3 calendar days."
DEFAULT_ENTSOE_LOOKBACK_DAYS = 3

# Open-Meteo's daily path now uses the Forecast API (operational models,
# refreshed every 1-6 hours) rather than the Historical/Archive API
# (ERA5 reanalysis, which settles over ~2 days) for recent dates — see
# `open_meteo_client.OPEN_METEO_FORECAST_URL`. A rolling lookback, same
# shape as ENTSO-E's, means a value fetched from an early model run gets
# re-fetched and can be replaced by a later, more accurate run before the
# window closes. Kept at the same 3 days as ENTSO-E rather than a
# separate number, by explicit decision.
DEFAULT_OPEN_METEO_LOOKBACK_DAYS = 3


class ProcessingWindowError(ValueError):
    """Raised when execution_mode/start_date/end_date/lookback_days are inconsistent."""


def latest_completed_date(reference_date: Optional[date] = None) -> date:
    """Most recently completed UTC calendar date, relative to `reference_date`."""
    today = reference_date or datetime.now(dt_timezone.utc).date()
    return today - timedelta(days=1)


def _validate_mode(execution_mode: str) -> None:
    if execution_mode not in VALID_EXECUTION_MODES:
        raise ProcessingWindowError(
            f"Unsupported execution_mode {execution_mode!r}; must be one of "
            f"{VALID_EXECUTION_MODES}"
        )


def _require_explicit_range(
    execution_mode: str, start_date: Optional[date], end_date: Optional[date]
) -> Tuple[date, date]:
    if start_date is None or end_date is None:
        raise ProcessingWindowError(
            f"execution_mode={execution_mode!r} requires explicit start_date and end_date"
        )
    if start_date > end_date:
        raise ProcessingWindowError("start_date must not be after end_date")
    return start_date, end_date


def resolve_open_meteo_window(
    execution_mode: str,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    lookback_days: int = DEFAULT_OPEN_METEO_LOOKBACK_DAYS,
    reference_date: Optional[date] = None,
) -> Tuple[date, date]:
    """Resolve the Open-Meteo processing window for one execution_mode.

    `daily`: a rolling recent-date lookback window ending at the latest
    completed date (mirrors `resolve_entsoe_window` — the Forecast API
    backing this path serves a value that can still improve as a later
    model run supersedes an earlier one; see
    `DEFAULT_OPEN_METEO_LOOKBACK_DAYS`). `backfill`/`reprocess`: the
    explicitly supplied start_date/end_date (against the Historical/
    Archive API instead — see the ingestion notebook).
    """
    _validate_mode(execution_mode)
    if execution_mode == DAILY:
        if lookback_days <= 0:
            raise ProcessingWindowError("lookback_days must be a positive integer")
        anchor = latest_completed_date(reference_date)
        return anchor - timedelta(days=lookback_days - 1), anchor
    return _require_explicit_range(execution_mode, start_date, end_date)


def resolve_entsoe_window(
    execution_mode: str,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    lookback_days: int = DEFAULT_ENTSOE_LOOKBACK_DAYS,
    reference_date: Optional[date] = None,
) -> Tuple[date, date]:
    """Resolve the ENTSO-E processing window for one execution_mode.

    `daily`: a rolling recent-date lookback window ending at the latest
    completed date (Spec 006 "Why ENTSO-E Uses a Lookback" — ENTSO-E
    values may be revised, or previously missing intervals may become
    available, after first publication). `backfill`/`reprocess`: the
    explicitly supplied start_date/end_date.
    """
    _validate_mode(execution_mode)
    if execution_mode == DAILY:
        if lookback_days <= 0:
            raise ProcessingWindowError("lookback_days must be a positive integer")
        anchor = latest_completed_date(reference_date)
        return anchor - timedelta(days=lookback_days - 1), anchor
    return _require_explicit_range(execution_mode, start_date, end_date)
