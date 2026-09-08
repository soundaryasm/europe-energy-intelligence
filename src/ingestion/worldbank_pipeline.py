"""World Bank Bronze ingestion orchestration for the MVP countries.

Deliberately dumb/simple, by explicit design decision: this pipeline has
no execution_mode, no lookback window, and no backfill date-range concept
of its own — it always fetches exactly one calendar year, for every
configured country, across a fixed small set of indicators. One call per
indicator (World Bank's semicolon-separated country list covers every
country in that single call), so 4 indicators = 4 HTTP calls total,
regardless of country count.

Idempotent by the same Bronze MERGE mechanism as the other two sources,
so re-fetching the same year on every daily run (and again on every
backfill run that happens to target a month within that year) is
expected and cheap, not wasteful — no rate limit is documented for the
World Bank API, and even a full year's worth of country data across all
four indicators is a handful of small requests.

This module MUST only be executed on Databricks for its Spark write path
(`_default_spark_writer`), imported lazily inside the function body so
the orchestration and business logic here stay importable and
unit-testable without a PySpark installation.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone as dt_timezone
from typing import Callable, Dict, List, Optional

from src.config.countries import CountryConfig, load_countries
from src.ingestion.worldbank_bronze import build_bronze_records
from src.ingestion.worldbank_client import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
    WorldBankAPIError,
    WorldBankRequest,
    fetch_indicator,
)

logger = logging.getLogger(__name__)

BRONZE_TABLE_NAME = "bronze_worldbank_indicators"

# Deliberately just these four, per the agreed minimal scope. Add more
# here later if needed — nothing else in this module needs to change.
INDICATOR_CODES = (
    "SP.POP.TOTL",          # Population, total
    "NY.GDP.MKTP.KD",       # GDP, constant 2015 US$
    "NY.GDP.PCAP.KD",       # GDP per capita, constant 2015 US$
    "SP.URB.TOTL.IN.ZS",    # Urban population (% of total)
)


@dataclass
class IngestionResult:
    """Observability summary for one World Bank ingestion execution, at indicator granularity."""

    started_at: datetime
    ended_at: Optional[datetime] = None
    year: Optional[int] = None
    indicators_attempted: List[str] = field(default_factory=list)
    indicators_succeeded: List[str] = field(default_factory=list)
    indicators_failed: List[str] = field(default_factory=list)
    records_written: int = 0
    errors: Dict[str, str] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return not self.indicators_failed


def _default_spark_writer(spark, records: List[dict], table_name: str) -> int:
    """Write Bronze records to a Delta table, upserting on the business key.

    Only ever runs on Databricks: PySpark/Delta are imported here, inside
    the function body, rather than at module import time. Uses the same
    explicit-schema, no-auto-schema-evolution writer as ENTSO-E/Open-Meteo
    (see `delta_schema` module docstring for why).
    """
    from src.ingestion.delta_schema import (
        WORLDBANK_BRONZE_KEY_COLS,
        worldbank_bronze_schema,
        write_with_deterministic_schema,
    )
    from src.transformations.dedupe import dedupe_latest

    if not records:
        return 0

    df = dedupe_latest(
        spark.createDataFrame(records, schema=worldbank_bronze_schema()),
        key_cols=WORLDBANK_BRONZE_KEY_COLS,
    )

    return write_with_deterministic_schema(
        spark, df, table_name, worldbank_bronze_schema(), WORLDBANK_BRONZE_KEY_COLS,
    )


def run_ingestion(
    year: int,
    *,
    spark=None,
    countries: Optional[List[CountryConfig]] = None,
    table_name: str = BRONZE_TABLE_NAME,
    indicator_codes=INDICATOR_CODES,
    fetch_fn: Callable[..., List[dict]] = fetch_indicator,
    spark_writer: Callable[[object, List[dict], str], int] = _default_spark_writer,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> IngestionResult:
    """Ingest World Bank Bronze indicator data for every configured country, for one year.

    Each indicator is processed independently: one indicator's failure is
    recorded and logged but does not stop the others. A genuinely empty
    result (no data for that indicator/year) is not a failure — it just
    contributes zero rows.
    """
    result = IngestionResult(
        started_at=datetime.now(dt_timezone.utc),
        year=year,
    )

    resolved_countries = countries if countries is not None else load_countries()
    country_codes = [c.country_code for c in resolved_countries]
    all_records: List[dict] = []

    for indicator_code in indicator_codes:
        result.indicators_attempted.append(indicator_code)
        try:
            wb_records = fetch_fn(
                WorldBankRequest(
                    indicator_code=indicator_code,
                    country_codes=country_codes,
                    year=year,
                ),
                timeout=timeout,
                max_retries=max_retries,
            )
            all_records.extend(build_bronze_records(wb_records))
            result.indicators_succeeded.append(indicator_code)
        except Exception as exc:  # noqa: BLE001 - a per-indicator failure must stay visible
            logger.error("World Bank ingestion failed for %s: %s", indicator_code, exc)
            result.indicators_failed.append(indicator_code)
            result.errors[indicator_code] = str(exc)

    if all_records:
        result.records_written = spark_writer(spark, all_records, table_name)

    result.ended_at = datetime.now(dt_timezone.utc)
    return result
