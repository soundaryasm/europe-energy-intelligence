"""Open-Meteo Bronze ingestion orchestration for the MVP countries (Spec 001).

This module MUST only be executed on Databricks. The PySpark/Delta write
path (`_default_spark_writer`) imports PySpark lazily, inside the function
body, so that the orchestration and business logic in this module stay
importable and unit-testable without a PySpark installation and without
touching any external system. Callers on Databricks pass the active
`spark` session; tests inject a mock `spark_writer` instead.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone as dt_timezone
from typing import Callable, Dict, List, Optional, Tuple

from src.config.countries import CountryConfig, load_countries
from src.ingestion.open_meteo_bronze import build_bronze_records
from src.ingestion.open_meteo_client import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
    OPEN_METEO_ARCHIVE_URL,
    OpenMeteoAPIError,
    OpenMeteoRequest,
    SOURCE_ENDPOINT,
    fetch_weather,
)
from src.orchestration.processing_window import latest_completed_date

logger = logging.getLogger(__name__)

BRONZE_TABLE_NAME = "bronze_open_meteo_weather"
DEFAULT_BACKFILL_MONTHS = 24
_DAYS_PER_MONTH_APPROX = 30


@dataclass
class IngestionResult:
    """Observability summary for one ingestion execution."""

    started_at: datetime
    ended_at: Optional[datetime] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    countries_attempted: List[str] = field(default_factory=list)
    countries_succeeded: List[str] = field(default_factory=list)
    countries_failed: List[str] = field(default_factory=list)
    records_written: int = 0
    errors: Dict[str, str] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return not self.countries_failed


def daily_processing_date(reference_date: Optional[date] = None) -> date:
    """Return the most recently completed calendar date to ingest.

    Delegates to the canonical `processing_window.latest_completed_date`
    (Spec 006 "Canonical Processing Window") so this single "yesterday"
    rule has one implementation, reused by both the backfill default
    below and the daily/backfill/reprocess notebook entry points.
    """
    return latest_completed_date(reference_date)


def backfill_date_range(
    months: int = DEFAULT_BACKFILL_MONTHS,
    end_date: Optional[date] = None,
) -> Tuple[date, date]:
    """Return a configurable (start_date, end_date) historical backfill window.

    `months` and `end_date` are parameters rather than constants so the
    same ingestion code serves both historical backfill and daily
    incremental execution, per Spec 001.
    """
    if months <= 0:
        raise ValueError("months must be a positive integer")
    resolved_end = end_date or daily_processing_date()
    start = resolved_end - timedelta(days=months * _DAYS_PER_MONTH_APPROX)
    return start, resolved_end


def _default_spark_writer(spark, records: List[dict], table_name: str) -> int:
    """Write Bronze records to a Delta table, upserting on the business key.

    Only ever runs on Databricks: PySpark/Delta are imported here, inside
    the function body, rather than at module import time.

    The source DataFrame is deduplicated on the merge key (keeping the
    row with the latest `ingestion_timestamp`) before the MERGE. Delta's
    MERGE fails with DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
    if two source rows match the same target row — this can happen if a
    retry or overlapping reprocess run hands the writer the same logical
    observation twice in one call. Deduplicating here (the same
    keep-latest-by-ingestion_timestamp rule used everywhere else in this
    codebase — see `src/transformations/dedupe.py`) makes the write
    robust to that regardless of cause, per Delta's own guidance to
    "preprocess the source table to eliminate the possibility of
    multiple matches."

    Uses an explicit, application-owned schema and validates it against
    the existing table rather than relying on Delta's automatic MERGE
    schema evolution (unsupported on Databricks serverless Standard
    environment v5 — see `delta_schema` module docstring).
    """
    from src.ingestion.delta_schema import (
        OPEN_METEO_BRONZE_KEY_COLS,
        open_meteo_bronze_schema,
        write_with_deterministic_schema,
    )
    from src.transformations.dedupe import dedupe_latest

    if not records:
        return 0

    df = dedupe_latest(
        spark.createDataFrame(records, schema=open_meteo_bronze_schema()),
        key_cols=OPEN_METEO_BRONZE_KEY_COLS,
    )

    return write_with_deterministic_schema(
        spark, df, table_name, open_meteo_bronze_schema(), OPEN_METEO_BRONZE_KEY_COLS,
    )


def run_ingestion(
    start_date: date,
    end_date: date,
    *,
    spark=None,
    countries: Optional[List[CountryConfig]] = None,
    table_name: str = BRONZE_TABLE_NAME,
    endpoint_url: str = OPEN_METEO_ARCHIVE_URL,
    source_endpoint_label: str = SOURCE_ENDPOINT,
    fetch_fn: Callable[..., dict] = fetch_weather,
    spark_writer: Callable[[object, List[dict], str], int] = _default_spark_writer,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> IngestionResult:
    """Ingest Open-Meteo Bronze weather data for every configured country.

    `endpoint_url`/`source_endpoint_label` default to the Historical/
    Archive API (settled data, right for backfill/reprocess of older
    dates). The daily ingestion notebook instead passes
    `OPEN_METEO_FORECAST_URL`/`SOURCE_ENDPOINT_FORECAST` for recent dates
    — see `open_meteo_client` for why the two APIs aren't interchangeable
    for recent dates.

    Countries are processed independently: one country's failure is
    recorded in the result and logged, but does not stop the others from
    being ingested. Failures are never silently ignored (Spec 001
    "Observability" / "API Behaviour").
    """
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    result = IngestionResult(
        started_at=datetime.now(dt_timezone.utc),
        start_date=start_date,
        end_date=end_date,
    )

    resolved_countries = countries if countries is not None else load_countries()
    all_records: List[dict] = []

    for country in resolved_countries:
        result.countries_attempted.append(country.country_code)
        try:
            payload = fetch_fn(
                OpenMeteoRequest(
                    country_code=country.country_code,
                    latitude=country.latitude,
                    longitude=country.longitude,
                    timezone=country.timezone,
                    start_date=start_date,
                    end_date=end_date,
                ),
                endpoint_url=endpoint_url,
                timeout=timeout,
                max_retries=max_retries,
            )
            records = build_bronze_records(payload, country, source_endpoint=source_endpoint_label)
            if not records:
                raise OpenMeteoAPIError(
                    f"Open-Meteo returned no records for {country.country_code} "
                    f"between {start_date} and {end_date}."
                )
            all_records.extend(records)
            result.countries_succeeded.append(country.country_code)
        except Exception as exc:  # noqa: BLE001 - a per-country failure must stay visible
            logger.error("Open-Meteo ingestion failed for %s: %s", country.country_code, exc)
            result.countries_failed.append(country.country_code)
            result.errors[country.country_code] = str(exc)

    if all_records:
        result.records_written = spark_writer(spark, all_records, table_name)

    result.ended_at = datetime.now(dt_timezone.utc)
    return result
