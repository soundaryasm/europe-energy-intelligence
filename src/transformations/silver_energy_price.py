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

from src.ingestion.entsoe_xml import parse_iso8601_duration_minutes
from src.transformations.dedupe import dedupe_latest

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

SILVER_PRICE_COLUMNS = (
    "country_code",
    "local_date",
    "avg_day_ahead_price_eur_mwh",
    "min_day_ahead_price_eur_mwh",
    "max_day_ahead_price_eur_mwh",
    "source_interval_count",
    "source_system",
)


def _interval_hours_udf():
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    def _hours(resolution: str) -> float:
        return parse_iso8601_duration_minutes(resolution) / 60.0

    return F.udf(_hours, DoubleType())


def _with_local_date(df: "DataFrame", country_timezones: Dict[str, str]):
    from pyspark.sql import functions as F

    timezone_map = F.create_map([F.lit(x) for pair in country_timezones.items() for x in pair])
    return df.withColumn("_tz", timezone_map[F.col("country_code")]).withColumn(
        "local_date",
        F.to_date(
            F.from_utc_timestamp(
                F.to_timestamp(F.col("source_timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
                F.col("_tz"),
            )
        ),
    )


def build_silver_energy_price_daily(bronze_df: "DataFrame", country_timezones: Dict[str, str]) -> "DataFrame":
    """Aggregate Bronze ENTSO-E price observations into `silver_energy_price_daily`.

    The daily average is interval-duration-weighted (Spec 003), so mixed
    15/30/60-minute resolutions within one day are not double- or
    under-counted. Negative prices are valid and are never filtered out.
    """
    from pyspark.sql import functions as F

    price_only = bronze_df.filter(F.col("dataset_type") == "price")
    deduped = dedupe_latest(price_only, key_cols=["country_code", "source_timestamp"])

    enriched = _with_local_date(deduped, country_timezones).withColumn(
        "interval_hours", _interval_hours_udf()(F.col("source_resolution"))
    ).withColumn("weighted_price", F.col("value") * F.col("interval_hours"))

    result = (
        enriched.groupBy("country_code", "local_date")
        .agg(
            (F.sum("weighted_price") / F.sum("interval_hours")).alias("avg_day_ahead_price_eur_mwh"),
            F.min("value").alias("min_day_ahead_price_eur_mwh"),
            F.max("value").alias("max_day_ahead_price_eur_mwh"),
            F.count(F.lit(1)).alias("source_interval_count"),
        )
        .withColumn("source_system", F.lit("entsoe"))
    )

    return result.select(*SILVER_PRICE_COLUMNS)
