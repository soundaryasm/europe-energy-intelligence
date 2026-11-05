"""Canonical processing-window resolution shared by every task (Spec 006).

Spec 006 requires the workflow to "derive a single canonical processing
window and pass it to downstream tasks" rather than "independently
calculate date ranges inside every task." This module is that single
place.

**One rule, no modes.** `end_date` defaults to the latest completed UTC
date; `start_date` defaults to `lookback_days` back from that end. Every
run is the same code path:

    nightly        (no dates)                  -> last 3 completed days
    repair a day   start=2026-11-01            -> 2026-11-01 .. latest
    bounded repair start=.., end=..            -> exactly that range
    full rebuild   start=2015-01-01            -> everything to latest

There used to be an `execution_mode` of `daily` or `reprocess`, with
`daily` computing a lookback and `reprocess` demanding explicit dates.
It was removed because the two branches resolved identically once the
dates were known, and because the mode leaked into behaviour that had
nothing to do with dates: it also chose which Open-Meteo endpoint to
call, which sent a four-day-old date to the ERA5 archive, hung the task
for an hour and cost a chunk of a bounded compute allowance. Endpoint
choice now derives from the date itself
(`open_meteo_client.select_endpoint`), and a window is just two dates.

Pure Python, no PySpark/Databricks dependency, fully unit-testable
locally.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone as dt_timezone
from typing import Optional, Tuple

# Spec 006: "Initial MVP lookback: 3 calendar days."
#
# One number for every source. ENTSO-E needs it because values are
# revised and missing intervals appear after first publication; the
# Open-Meteo Forecast API needs it because an early model run is
# superseded by a later, more accurate one. They were separate constants
# that both read 3 by explicit decision, which was two things to keep in
# step for no gain.
DEFAULT_LOOKBACK_DAYS = 3

# ENTSO-E is asked for one day more than the window needs, at the start.
#
# A request is built from dates and ENTSO-E reads them as 00:00 UTC, but
# Silver's grain is the country-*local* calendar day. For any UTC+ zone
# the local day begins before 00:00 UTC — local 2026-11-01 in CET starts
# at 2026-10-31 23:00 UTC — so a window asked for in UTC is always
# missing its first local hour. Silver then measures 23 covered hours,
# marks the day `partial`, and Gold withholds the metric.
#
# This is not specific to one run shape: the oldest day of *any* window
# is exposed. The nightly job only escapes it because the previous
# night's window already fetched that hour, and a one-day repair run has
# no previous night to lean on. Padding the fetch here means no caller
# has to remember. The extra day costs one request per country/dataset
# and merges idempotently.
ENTSOE_FETCH_PAD_DAYS = 1


class ProcessingWindowError(ValueError):
    """Raised when start_date/end_date/lookback_days are inconsistent."""


def latest_completed_date(reference_date: Optional[date] = None) -> date:
    """Most recently completed UTC calendar date, relative to `reference_date`."""
    today = reference_date or datetime.now(dt_timezone.utc).date()
    return today - timedelta(days=1)


def resolve_window(
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    reference_date: Optional[date] = None,
) -> Tuple[date, date]:
    """The inclusive (start, end) window to process.

    `end_date` omitted means the latest completed UTC date — so a
    scheduled run needs no dates at all, and a repair run needs only a
    start. `start_date` omitted means `lookback_days` back from the end,
    inclusive of the end day itself.

    `lookback_days` is ignored when `start_date` is given: an explicit
    start is already the answer it would have computed.
    """
    end = end_date if end_date is not None else latest_completed_date(reference_date)

    if start_date is not None:
        start = start_date
    else:
        if lookback_days <= 0:
            raise ProcessingWindowError("lookback_days must be a positive integer")
        start = end - timedelta(days=lookback_days - 1)

    if start > end:
        raise ProcessingWindowError(
            f"start_date {start.isoformat()} must not be after end_date {end.isoformat()}"
        )
    return start, end


def entsoe_fetch_window(start_date: date, end_date: date) -> Tuple[date, date]:
    """The window to *request* from ENTSO-E for a given processing window.

    One day earlier than asked, so the first local hour of the window's
    oldest day is covered. See `ENTSOE_FETCH_PAD_DAYS`. The end is
    unchanged: ENTSO-E reads `end_date` as 00:00 UTC of the day after the
    last one wanted, which already includes the whole final UTC day.
    """
    return start_date - timedelta(days=ENTSOE_FETCH_PAD_DAYS), end_date
