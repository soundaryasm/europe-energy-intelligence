"""Orchestrates one backfill-job invocation (Databricks-only entry
point, wired up by `notebooks/orchestration/backfill_month.py`).

Reuses the existing ENTSO-E/Open-Meteo ingestion pipelines unchanged —
this module only adds: picking which historical month to process next,
verifying *whole-month* coverage per (source, country, dataset) after
ingesting it, and updating the checkpoint table accordingly. See
`backfill_checkpoint.py` / `backfill_completeness.py` for why "a record
exists" is not treated as "the month is done".

Every Spark-touching dependency is injectable (same pattern as
`fetch_fn`/`spark_writer` throughout `entsoe_pipeline.py`/
`open_meteo_pipeline.py`), so the orchestration logic here is
unit-testable with plain mocks — no PySpark required.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timezone as dt_timezone
from typing import Callable, Dict, FrozenSet, List, Optional, Tuple

from src.config.countries import CountryConfig, load_countries
from src.ingestion.entsoe_datasets import ALL_DATASETS
from src.ingestion.entsoe_pipeline import BRONZE_TABLE_NAME as ENTSOE_BRONZE_TABLE
from src.ingestion.entsoe_pipeline import IngestionResult as EntsoeIngestionResult
from src.ingestion.open_meteo_client import OPEN_METEO_ARCHIVE_URL, SOURCE_ENDPOINT
from src.ingestion.open_meteo_pipeline import BRONZE_TABLE_NAME as OPEN_METEO_BRONZE_TABLE
from src.ingestion.open_meteo_pipeline import IngestionResult as OpenMeteoIngestionResult
from src.ingestion.worldbank_pipeline import BRONZE_TABLE_NAME as WORLDBANK_BRONZE_TABLE
from src.orchestration.backfill_checkpoint import (
    CHECKPOINT_TABLE_NAME,
    CheckpointEntry,
    CheckpointKey,
    ENTSOE_BACKFILL_CHUNK_DAYS,
    GIVE_UP_AFTER_ATTEMPTS,
    OPEN_METEO_DATASET,
    SOURCE_ENTSOE,
    SOURCE_OPEN_METEO,
    STATUS_FAILED,
    STATUS_GIVEN_UP,
    build_checkpoint_row,
    buffered_entsoe_range,
    month_date_range,
    month_is_complete,
    next_backfill_month,
    read_checkpoint_statuses,
    write_checkpoint_rows,
)
from src.orchestration.backfill_completeness import classify_month_result, evaluate_month_coverage

logger = logging.getLogger(__name__)


@dataclass
class BackfillRunResult:
    target_month: Optional[date]
    self_pause: bool = False
    combo_statuses: Dict[Tuple[str, str, str], str] = field(default_factory=dict)
    entsoe_records_written: int = 0
    open_meteo_records_written: int = 0
    worldbank_records_written: int = 0
    worldbank_error: Optional[str] = None
    checkpoint_rows_written: int = 0


def expected_combos(countries: List[CountryConfig]) -> List[Tuple[str, str, str]]:
    """Every `(source, country_code, dataset)` the backfill is responsible for."""
    combos: List[Tuple[str, str, str]] = []
    for country in countries:
        for dataset in ALL_DATASETS:
            combos.append((SOURCE_ENTSOE, country.country_code, dataset.name))
        combos.append((SOURCE_OPEN_METEO, country.country_code, OPEN_METEO_DATASET))
    return combos


def determine_target_month(
    spark,
    countries: List[CountryConfig],
    checkpoint_table: str = CHECKPOINT_TABLE_NAME,
    reference_date: Optional[date] = None,
    checkpoint_reader: Callable[..., Dict[Tuple[str, str, str, date], CheckpointEntry]] = read_checkpoint_statuses,
) -> Optional[date]:
    """The newest historical month not yet fully complete, or `None` if
    the backfill has reached `BACKFILL_HISTORICAL_START` — the signal to
    self-pause the job's schedule.
    """
    entries = checkpoint_reader(spark, checkpoint_table)
    combos = expected_combos(countries)

    def is_complete(month: date) -> bool:
        month_entries = {
            (src, cc, ds): entry
            for (src, cc, ds, m), entry in entries.items()
            if m == month
        }
        return month_is_complete(month_entries, combos)

    return next_backfill_month(is_complete, reference_date=reference_date)


def _entsoe_covered_dates(spark, country_code: str, dataset_name: str, table_name: str) -> FrozenSet[date]:
    """Distinct calendar dates already in Bronze for one country/dataset.

    `source_timestamp` is stored as `"%Y-%m-%dT%H:%M:%SZ"` (see
    `entsoe_bronze.build_bronze_records`) — parsed explicitly rather than
    relying on Spark's default timestamp inference.
    """
    from pyspark.sql import functions as F

    if not spark.catalog.tableExists(table_name):
        return frozenset()

    rows = (
        spark.table(table_name)
        .filter((F.col("country_code") == country_code) & (F.col("dataset_type") == dataset_name))
        .select(F.to_date(F.col("source_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("d"))
        .distinct()
        .collect()
    )
    return frozenset(row.d for row in rows if row.d is not None)


def _open_meteo_covered_dates(spark, country_code: str, table_name: str) -> FrozenSet[date]:
    """Distinct calendar dates already in Bronze for one country.

    `observation_date` is already a plain `YYYY-MM-DD` string, as
    returned directly by Open-Meteo's `daily.time` array.
    """
    from pyspark.sql import functions as F

    if not spark.catalog.tableExists(table_name):
        return frozenset()

    rows = (
        spark.table(table_name)
        .filter(F.col("country_code") == country_code)
        .select(F.to_date(F.col("observation_date")).alias("d"))
        .distinct()
        .collect()
    )
    return frozenset(row.d for row in rows if row.d is not None)


def _default_entsoe_ingestion_fn(*args, **kwargs) -> EntsoeIngestionResult:
    from src.ingestion.entsoe_pipeline import run_ingestion
    return run_ingestion(*args, **kwargs)


def _default_open_meteo_ingestion_fn(*args, **kwargs) -> OpenMeteoIngestionResult:
    from src.ingestion.open_meteo_pipeline import run_ingestion
    return run_ingestion(*args, **kwargs)


def _default_worldbank_ingestion_fn(*args, **kwargs):
    from src.ingestion.worldbank_pipeline import run_ingestion
    return run_ingestion(*args, **kwargs)


def _next_status_and_attempt_count(
    status: str,
    key: Tuple[str, str, str, date],
    existing_entries: Dict[Tuple[str, str, str, date], CheckpointEntry],
) -> Tuple[str, int]:
    """Bump this combo's attempt count from whatever it was last time,
    and escalate a repeatedly-`failed` combo to `given_up` once it hits
    `GIVE_UP_AFTER_ATTEMPTS` — see `backfill_checkpoint.STATUS_GIVEN_UP`
    for why this exists (a single stuck combo must not block every
    older month forever) and why it's distinct from `unavailable` (a
    confirmed absence, not a give-up).
    """
    previous = existing_entries.get(key)
    attempt_count = (previous.attempt_count if previous else 0) + 1
    if status == STATUS_FAILED and attempt_count >= GIVE_UP_AFTER_ATTEMPTS:
        return STATUS_GIVEN_UP, attempt_count
    return status, attempt_count


def run_backfill_month(
    spark,
    *,
    token: str,
    countries: Optional[List[CountryConfig]] = None,
    checkpoint_table: str = CHECKPOINT_TABLE_NAME,
    entsoe_table: str = ENTSOE_BRONZE_TABLE,
    open_meteo_table: str = OPEN_METEO_BRONZE_TABLE,
    reference_date: Optional[date] = None,
    checkpoint_reader: Callable[..., Dict[Tuple[str, str, str, date], CheckpointEntry]] = read_checkpoint_statuses,
    checkpoint_writer: Callable[..., int] = write_checkpoint_rows,
    entsoe_covered_dates_fn: Callable[..., FrozenSet[date]] = _entsoe_covered_dates,
    open_meteo_covered_dates_fn: Callable[..., FrozenSet[date]] = _open_meteo_covered_dates,
    entsoe_ingestion_fn: Callable[..., EntsoeIngestionResult] = _default_entsoe_ingestion_fn,
    open_meteo_ingestion_fn: Callable[..., OpenMeteoIngestionResult] = _default_open_meteo_ingestion_fn,
    worldbank_table: str = WORLDBANK_BRONZE_TABLE,
    worldbank_ingestion_fn: Callable[..., object] = _default_worldbank_ingestion_fn,
) -> BackfillRunResult:
    """Process exactly one historical month: ingest it from ENTSO-E and
    Open-Meteo, verify whole-month coverage per (source, country,
    dataset), and update the checkpoint. Also best-effort fetches World
    Bank data for the target month's year (annual, not month-gated —
    see the World Bank call site below); a World Bank failure never
    affects the checkpoint or fails this function. Returns
    `self_pause=True` (target_month `None`) once every month back to
    `BACKFILL_HISTORICAL_START` is done — the caller (the notebook) is
    responsible for actually pausing the job's own schedule when it
    sees that.
    """
    resolved_countries = countries if countries is not None else load_countries()
    target_month = determine_target_month(
        spark, resolved_countries, checkpoint_table, reference_date, checkpoint_reader=checkpoint_reader,
    )

    if target_month is None:
        logger.info("Backfill complete: every month back to the historical start is accounted for.")
        return BackfillRunResult(target_month=None, self_pause=True)

    logger.info("Backfill target month: %s", target_month)
    now_iso = datetime.now(dt_timezone.utc).isoformat()
    # Re-read rather than reuse determine_target_month's own read: that
    # call doesn't return its entries, and this is a cheap Delta read —
    # simpler than threading the result through, matching this
    # project's existing "cheap redundant reads are fine" calls
    # elsewhere (e.g. World Bank's always-refetch design).
    existing_entries = checkpoint_reader(spark, checkpoint_table)

    entsoe_start, entsoe_end = buffered_entsoe_range(target_month)
    entsoe_result = entsoe_ingestion_fn(
        entsoe_start, entsoe_end, token=token, spark=spark,
        countries=resolved_countries, table_name=entsoe_table,
        chunk_days=ENTSOE_BACKFILL_CHUNK_DAYS,
    )

    weather_start, weather_end = month_date_range(target_month)
    open_meteo_result = open_meteo_ingestion_fn(
        weather_start, weather_end, spark=spark,
        countries=resolved_countries, table_name=open_meteo_table,
        endpoint_url=OPEN_METEO_ARCHIVE_URL, source_endpoint_label=SOURCE_ENDPOINT,
    )

    # World Bank is annual, not monthly, and deliberately outside the
    # month-completeness/checkpoint model entirely (see
    # backfill_checkpoint.py) — it just piggybacks on whichever year the
    # target month happens to fall in, per the agreed "dumb" design:
    # backfilling any month of 2025 re-fetches all of 2025's indicators,
    # redundant but cheap and idempotent. Best-effort: a World Bank
    # failure must never fail the ENTSO-E/Open-Meteo month being
    # processed, so it's caught here rather than left to propagate.
    worldbank_records_written = 0
    worldbank_error: Optional[str] = None
    try:
        worldbank_result = worldbank_ingestion_fn(
            target_month.year, spark=spark, countries=resolved_countries, table_name=worldbank_table,
        )
        worldbank_records_written = worldbank_result.records_written
        if not worldbank_result.succeeded:
            worldbank_error = str(worldbank_result.errors)
    except Exception as exc:  # noqa: BLE001 - must not abort the ENTSO-E/Open-Meteo month
        logger.error("World Bank backfill fetch failed for year %s: %s", target_month.year, exc)
        worldbank_error = str(exc)

    checkpoint_rows: List[dict] = []
    combo_statuses: Dict[Tuple[str, str, str], str] = {}

    for country in resolved_countries:
        for dataset in ALL_DATASETS:
            attempt_key = f"{country.country_code}:{dataset.name}"
            covered = entsoe_covered_dates_fn(spark, country.country_code, dataset.name, entsoe_table)
            coverage = evaluate_month_coverage(target_month, covered)
            status = classify_month_result(
                coverage, ingestion_reported_no_data=attempt_key in entsoe_result.unavailable,
            )
            combo_key = (SOURCE_ENTSOE, country.country_code, dataset.name)
            entry_key = (*combo_key, target_month)
            status, attempt_count = _next_status_and_attempt_count(status, entry_key, existing_entries)
            combo_statuses[combo_key] = status
            checkpoint_rows.append(build_checkpoint_row(
                CheckpointKey(SOURCE_ENTSOE, country.country_code, dataset.name, target_month),
                status, attempt_count=attempt_count, started_at=now_iso,
                completed_at=datetime.now(dt_timezone.utc).isoformat(),
                last_error=entsoe_result.errors.get(attempt_key),
            ))

        covered = open_meteo_covered_dates_fn(spark, country.country_code, open_meteo_table)
        coverage = evaluate_month_coverage(target_month, covered)
        # Open-Meteo's pipeline has no distinct "confirmed no data"
        # signal (unlike ENTSO-E's Acknowledgement_MarketDocument) — an
        # empty result there is always classified `failed`, never
        # `unavailable`; a full-month weather gap is not an expected
        # legitimate condition the way an ENTSO-E source gap can be.
        status = classify_month_result(coverage, ingestion_reported_no_data=False)
        combo_key = (SOURCE_OPEN_METEO, country.country_code, OPEN_METEO_DATASET)
        entry_key = (*combo_key, target_month)
        status, attempt_count = _next_status_and_attempt_count(status, entry_key, existing_entries)
        combo_statuses[combo_key] = status
        checkpoint_rows.append(build_checkpoint_row(
            CheckpointKey(SOURCE_OPEN_METEO, country.country_code, OPEN_METEO_DATASET, target_month),
            status, attempt_count=attempt_count, started_at=now_iso,
            completed_at=datetime.now(dt_timezone.utc).isoformat(),
            last_error=open_meteo_result.errors.get(country.country_code),
        ))

    rows_written = checkpoint_writer(spark, checkpoint_rows, checkpoint_table)

    return BackfillRunResult(
        target_month=target_month,
        self_pause=False,
        combo_statuses=combo_statuses,
        entsoe_records_written=entsoe_result.records_written,
        open_meteo_records_written=open_meteo_result.records_written,
        worldbank_records_written=worldbank_records_written,
        worldbank_error=worldbank_error,
        checkpoint_rows_written=rows_written,
    )
