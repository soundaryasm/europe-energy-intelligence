"""Silver day-ahead price transformation (Spec 003).

Databricks-only: aggregates Bronze ENTSO-E price observations (schema
produced by `src/ingestion/entsoe_bronze.build_bronze_records` for
dataset_type="price") into daily EUR/MWh statistics per country. PySpark
is imported lazily inside function bodies so this module stays importable
without a PySpark installation; correctness against real Spark DataFrames
is covered by `tests/test_silver_energy_spark.py` (Databricks-runtime
tests, skipped locally).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict

from src.transformations.dedupe import dedupe_latest
from src.transformations.entsoe_silver_common import (
    interval_hours_udf,
    with_completeness_status,
    with_local_date,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

SILVER_PRICE_COLUMNS = (
    "country_code",
    "local_date",
    "avg_day_ahead_price_eur_mwh",
    "min_day_ahead_price_eur_mwh",
    "max_day_ahead_price_eur_mwh",
    "source_interval_count",
    "covered_duration_hours",
    "completeness_status",
    "source_system",
)


def build_silver_energy_price_daily(bronze_df: "DataFrame", country_timezones: Dict[str, str]) -> "DataFrame":
    """Aggregate Bronze ENTSO-E price observations into `silver_energy_price_daily`.

    The daily average is interval-duration-weighted (Spec 003), so mixed
    15/30/60-minute resolutions within one day are not double- or
    under-counted. Negative prices are valid and are never filtered out.
    `completeness_status` follows the same expected-timeline rule as
    demand (Spec 003 "Price Completeness") — Gold must not treat a
    `partial` day's average as a trusted complete daily metric.
    """
    from pyspark.sql import functions as F

    price_only = bronze_df.filter(F.col("dataset_type") == "price")
    deduped = dedupe_latest(price_only, key_cols=["country_code", "source_timestamp"])

    enriched = with_local_date(deduped, country_timezones).withColumn(
        "interval_hours", interval_hours_udf()(F.col("source_resolution"))
    ).withColumn("weighted_price", F.col("value") * F.col("interval_hours"))

    aggregated = (
        enriched.groupBy("country_code", "local_date")
        .agg(
            (F.sum("weighted_price") / F.sum("interval_hours")).alias("avg_day_ahead_price_eur_mwh"),
            F.min("value").alias("min_day_ahead_price_eur_mwh"),
            F.max("value").alias("max_day_ahead_price_eur_mwh"),
            F.count(F.lit(1)).alias("source_interval_count"),
            F.sum("interval_hours").alias("covered_duration_hours"),
        )
        .withColumn("source_system", F.lit("entsoe"))
    )

    result = with_completeness_status(aggregated, country_timezones)

    return result.select(*SILVER_PRICE_COLUMNS)
