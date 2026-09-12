"""Historical backfill month selection and checkpoint state (agreed
design, see project memory `project_backfill_architecture_plan`).

The backfill job processes exactly one historical calendar month per
invocation, walking backward from the previous complete month to
`BACKFILL_HISTORICAL_START`. Progress is tracked in an explicit
Delta checkpoint table (one row per source/country/dataset/month) —
never inferred from MIN/MAX dates in Bronze/Silver, which cannot tell
"not attempted yet" apart from "legitimately no data for this period."

Month selection (`next_backfill_month`) is pure Python, testable without
Spark. Reading/writing the checkpoint table needs Spark/Delta and is
lazily imported, same pattern as every other Spark writer in this
codebase.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone as dt_timezone
from typing import Callable, Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

CHECKPOINT_TABLE_NAME = "backfill_checkpoint"

# Spec: backfill covers ENTSO-E/Open-Meteo history back to the
# platform's documented start (see project_country_expansion_plan).
BACKFILL_HISTORICAL_START = date(2015, 1, 1)

# A small buffer either side of the target month so Silver can
# reconstruct complete local-timezone calendar dates at the month's
# edges (ENTSO-E only — Open-Meteo's daily API is already
# timezone-aware per country and needs no buffer). Overlap is fine:
# Bronze MERGE is idempotent, and only the target month's own dates
# count toward the completeness check.
ENTSOE_MONTH_BUFFER_DAYS = 2

# ENTSO-E requests within a backfill month are split into ~weekly
# windows (not the ~90-day default used for reprocess) to avoid
# oversized responses and keep retries cheap — reuses
# `entsoe_client.chunk_date_range`, no new chunking logic.
ENTSOE_BACKFILL_CHUNK_DAYS = 7

STATUS_PENDING = "pending"
STATUS_IN_PROGRESS = "in_progress"
STATUS_SUCCESS = "success"
STATUS_UNAVAILABLE = "unavailable"  # legitimately no data for the whole month
STATUS_FAILED = "failed"
# A combo that has failed (never a clean `unavailable` acknowledgement)
# this many times in a row stops blocking the walker — see
# STATUS_GIVEN_UP below. Deliberately small: this is a safety valve
# against one stubborn combo blocking every older month forever, not a
# tolerance for normal transient failures (the daily/backfill jobs'
# own scheduled re-fires are already the real retry mechanism).
GIVE_UP_AFTER_ATTEMPTS = 3
# A combo that failed GIVE_UP_AFTER_ATTEMPTS times in a row without ever
# producing a clean `unavailable` acknowledgement. Deliberately distinct
# from `unavailable`: that means ENTSO-E/Open-Meteo confirmed there is
# nothing there; this means we don't know and stopped asking for now.
# Counts as done for the walker (never blocks older months forever over
# one stuck combo), but stays visibly distinct in the checkpoint table
# so it can be found and manually re-checked later (e.g. a scoped
# `jobs submit` targeting just that country) — see project memory for
# why this isn't automated: getting the real data, if it ever recovers,
# reaches Gold on the next daily Silver/Gold rebuild regardless of what
# this table says, since Silver reprocesses all of Bronze every run.
STATUS_GIVEN_UP = "given_up"

# Only these statuses let the walker move past a month. `unavailable`
# counts as done (confirmed, not just absent) per the explicit rule:
# only advance past a month once it is fully accounted for, never
# because it merely looks empty. `given_up` counts as done too, once
# GIVE_UP_AFTER_ATTEMPTS is reached, so one stuck combo can't block
# every older month forever.
_DONE_STATUSES = frozenset({STATUS_SUCCESS, STATUS_UNAVAILABLE, STATUS_GIVEN_UP})

SOURCE_ENTSOE = "entsoe"
SOURCE_OPEN_METEO = "open_meteo"

# Open-Meteo has no per-dataset split (one weather call per country);
# this is the `dataset` value its checkpoint rows use, so the schema
# stays uniform across both sources instead of a nullable column.
OPEN_METEO_DATASET = "weather"


@dataclass(frozen=True)
class CheckpointKey:
    source: str
    country_code: str
    dataset: str
    month_start: date


@dataclass(frozen=True)
class CheckpointEntry:
    status: str
    attempt_count: int


_ONE_DAY = timedelta(days=1)


def previous_complete_month(reference_date: Optional[date] = None) -> date:
    """First day of the calendar month before `reference_date`'s month."""
    today = reference_date or datetime.now(dt_timezone.utc).date()
    first_of_this_month = today.replace(day=1)
    last_day_of_prev_month = first_of_this_month - _ONE_DAY
    return last_day_of_prev_month.replace(day=1)


def month_date_range(month_start: date) -> Tuple[date, date]:
    """(first day, last day) of `month_start`'s calendar month."""
    if month_start.day != 1:
        raise ValueError(f"month_start must be the first day of a month, got {month_start}")
    next_month = _month_after(month_start)
    return month_start, next_month - _ONE_DAY


def buffered_entsoe_range(month_start: date, buffer_days: int = ENTSOE_MONTH_BUFFER_DAYS) -> Tuple[date, date]:
    """(start, end) to actually request from ENTSO-E for `month_start`,
    padded by `buffer_days` on each side. Only dates within the plain
    `month_date_range` count toward that month's completeness check.
    """
    start, end = month_date_range(month_start)
    return start - timedelta(days=buffer_days), end + timedelta(days=buffer_days)


def _month_after(month_start: date) -> date:
    if month_start.month == 12:
        return month_start.replace(year=month_start.year + 1, month=1)
    return month_start.replace(month=month_start.month + 1)


def _month_before(month_start: date) -> date:
    if month_start.month == 1:
        return month_start.replace(year=month_start.year - 1, month=12)
    return month_start.replace(month=month_start.month - 1)


def walk_backfill_months(reference_date: Optional[date] = None) -> Iterator[date]:
    """Yield each calendar month, walking backward from the previous
    complete month down to and including `BACKFILL_HISTORICAL_START`.
    """
    month = previous_complete_month(reference_date)
    while month >= BACKFILL_HISTORICAL_START:
        yield month
        month = _month_before(month)


def next_backfill_month(
    is_month_complete: Callable[[date], bool],
    reference_date: Optional[date] = None,
) -> Optional[date]:
    """Return the newest month that is not yet complete, or `None` if
    every month back to `BACKFILL_HISTORICAL_START` is complete —
    the signal the job uses to self-pause its own schedule.

    `is_month_complete(month_start)` is supplied by the caller (backed
    by the checkpoint table): it must return True only when every
    expected (source, country, dataset) combination for that month has
    status `success` or `unavailable`.
    """
    for month in walk_backfill_months(reference_date):
        if not is_month_complete(month):
            return month
    return None


def month_is_complete(
    entries: Dict[Tuple[str, str, str], CheckpointEntry], expected_combos: List[Tuple[str, str, str]]
) -> bool:
    """True iff every `(source, country_code, dataset)` in
    `expected_combos` has a recorded entry in `entries` whose status is
    `success`, `unavailable`, or `given_up`. A missing entry (never
    attempted) or any other status (`pending`/`in_progress`/`failed`)
    means not complete — this is what stops the walker from skipping
    past a month that only partially succeeded.
    """
    return all(
        (entries[combo].status if combo in entries else None) in _DONE_STATUSES
        for combo in expected_combos
    )


# --- Spark/Delta-backed checkpoint persistence -----------------------
# Lazily imported, same pattern as every other Spark writer in this
# codebase (see `entsoe_pipeline._default_spark_writer`), so this
# module stays importable and unit-testable without PySpark installed.


def read_checkpoint_statuses(
    spark, table_name: str = CHECKPOINT_TABLE_NAME
) -> Dict[Tuple[str, str, str, date], CheckpointEntry]:
    """Return `{(source, country_code, dataset, month_start): CheckpointEntry}`
    for every row currently in the checkpoint table (empty dict if the
    table does not exist yet — nothing has ever been attempted).
    """
    if not spark.catalog.tableExists(table_name):
        return {}

    rows = spark.table(table_name).select(
        "source", "country_code", "dataset", "month_start", "status", "attempt_count"
    ).collect()
    return {
        (row.source, row.country_code, row.dataset, date.fromisoformat(row.month_start)):
            CheckpointEntry(row.status, row.attempt_count)
        for row in rows
    }


def write_checkpoint_rows(spark, rows: List[dict], table_name: str = CHECKPOINT_TABLE_NAME) -> int:
    """Upsert `rows` (each a dict matching `backfill_checkpoint_schema()`,
    with `month_start` as an ISO date string) into the checkpoint table.
    """
    from src.ingestion.delta_schema import (
        BACKFILL_CHECKPOINT_KEY_COLS,
        backfill_checkpoint_schema,
        write_with_deterministic_schema,
    )
    from src.transformations.dedupe import dedupe_latest

    if not rows:
        return 0

    df = dedupe_latest(
        spark.createDataFrame(rows, schema=backfill_checkpoint_schema()),
        key_cols=BACKFILL_CHECKPOINT_KEY_COLS,
        order_col="updated_at",
    )
    return write_with_deterministic_schema(
        spark, df, table_name, backfill_checkpoint_schema(), BACKFILL_CHECKPOINT_KEY_COLS,
    )


def build_checkpoint_row(
    key: CheckpointKey,
    status: str,
    *,
    attempt_count: int,
    started_at: Optional[str] = None,
    completed_at: Optional[str] = None,
    last_error: Optional[str] = None,
) -> dict:
    return {
        "source": key.source,
        "country_code": key.country_code,
        "dataset": key.dataset,
        "month_start": key.month_start.isoformat(),
        "status": status,
        "attempt_count": attempt_count,
        "started_at": started_at,
        "completed_at": completed_at,
        "last_error": last_error,
        "updated_at": datetime.now(dt_timezone.utc).isoformat(),
    }
