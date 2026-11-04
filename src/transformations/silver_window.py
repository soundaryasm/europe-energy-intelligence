"""Date-window helpers for incremental Silver and serving loads (Spec 006).

Silver and the PostgreSQL publish both used to process every row they
could see: `transform_silver` read the whole Bronze table and
`publish_postgres` collected every Gold row, on every run. That was
affordable only while the history was empty. With 2015-01 onward loaded
it means reprocessing ~41M Bronze rows and pushing ~1M Gold rows to the
serving layer nightly to add a single day — and Spec 005 already
requires that "after the initial load, normal publication should process
only affected dates".

These helpers turn the canonical processing window
(`src/orchestration/processing_window.py`) into the filters those two
tasks need. Pure PySpark-optional Python so the bounds logic is unit
testable without a Spark session.

Why Bronze is read with a padding of whole days either side
-----------------------------------------------------------
Silver's grain is `local_date` — the date in the *country's* timezone —
while Bronze stores `source_timestamp` in UTC. Europe runs from UTC+0 to
UTC+3, so the local day `D` draws on UTC instants that fall outside UTC
day `D`. Reading exactly the window's UTC days would therefore build its
edge days from partial data.

Padding alone would make that worse, not better: the padded input
produces rows for local dates just outside the window, computed from
incomplete data, and `write_silver_table` merges on
`(country_code, local_date)` — so a partial edge day would overwrite a
complete day landed by an earlier run. Hence both halves are needed:
read padded, then restrict the *output* to the window. Edge days are
then computed from complete input, and only whole days are written.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Tuple

# One whole day either side covers every European offset (UTC+0..UTC+3,
# and UTC-1 does not occur), with room to spare rather than arithmetic
# that has to be re-checked whenever a timezone's rules change.
DEFAULT_PAD_DAYS = 1


def bronze_bounds(
    start: date, end: date, *, pad_days: int = DEFAULT_PAD_DAYS,
) -> Tuple[str, str]:
    """Inclusive-lower, exclusive-upper ISO bounds for reading Bronze.

    Returned as strings because `source_timestamp` and
    `observation_date` are stored as ISO-8601 strings, where a
    lexicographic comparison is also a chronological one — so this
    needs no cast on either side of the filter.

    The upper bound is exclusive and already one day past `end`'s
    padding, so `"2026-11-05"` excludes every instant on and after that
    date while admitting all of `2026-11-04`.
    """
    if start > end:
        raise ValueError(f"start {start} must not be after end {end}")
    if pad_days < 0:
        raise ValueError("pad_days must not be negative")
    return (
        (start - timedelta(days=pad_days)).isoformat(),
        (end + timedelta(days=pad_days + 1)).isoformat(),
    )


def date_key_bounds(start: date, end: date) -> Tuple[int, int]:
    """Inclusive `dim_date.date_key` bounds for `start`..`end`.

    `date_key` is `yyyyMMdd` as an integer (see
    `dbt/models/marts/dim_date.sql`), which the Gold fact tables carry,
    so the serving publish filters on it rather than joining back to a
    date column it does not hold.
    """
    if start > end:
        raise ValueError(f"start {start} must not be after end {end}")
    return int(start.strftime("%Y%m%d")), int(end.strftime("%Y%m%d"))


def filter_bronze(df, column: str, start: date, end: date, *, pad_days: int = DEFAULT_PAD_DAYS):
    """Restrict a Bronze DataFrame to the padded window on `column`."""
    low, high = bronze_bounds(start, end, pad_days=pad_days)
    return df.filter((df[column] >= low) & (df[column] < high))


def restrict_to_window(df, start: date, end: date, *, column: str = "local_date"):
    """Keep only the rows whose `column` falls inside the window itself.

    Applied to Silver output, after the padded Bronze read: this is what
    stops a partially-covered edge day being merged over a complete one.

    `column` is a `DateType` (every builder derives it via `to_date`), so
    the bounds are passed as Python `date` objects and compared natively
    — no string cast to get wrong. Inclusive at both ends.
    """
    if start > end:
        raise ValueError(f"start {start} must not be after end {end}")
    return df.filter((df[column] >= start) & (df[column] <= end))
