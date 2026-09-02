"""Silver electricity demand transformation (Spec 003).

Databricks-only: aggregates Bronze ENTSO-E load observations (schema
produced by `src/ingestion/entsoe_bronze.build_bronze_records` for
dataset_type="load") into daily demand (MWh) per country. PySpark is
imported lazily inside function bodies so this module stays importable
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

SILVER_DEMAND_COLUMNS = (
    "country_code",
    "local_date",
    "daily_demand_mwh",
    "source_interval_count",
    "source_system",
)


def _interval_hours_udf():
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    def _hours(resolution: str) -> float:
        # Reuses the exact same tested parser used by ENTSO-E XML parsing
        # (Spec 002), so the interval-to-hours rule has one implementation.
        return parse_iso8601_duration_minutes(resolution) / 60.0

    return F.udf(_hours, DoubleType())


def _with_local_date(df: "DataFrame", country_timezones: Dict[str, str]):
    """Bucket UTC source_timestamp into each country's local calendar date."""
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


def build_silver_energy_demand_daily(bronze_df: "DataFrame", country_timezones: Dict[str, str]) -> "DataFrame":
    """Aggregate Bronze ENTSO-E load observations into `silver_energy_demand_daily`.

    `energy_mwh = load_mw x interval_duration_hours` per source interval
    (Spec 003), summed per country/local_date. Negative load readings are
    excluded (Spec 003/007: "demand is not negative") rather than
    silently coerced to zero.
    """
    from pyspark.sql import functions as F

    load_only = bronze_df.filter(F.col("dataset_type") == "load")
    deduped = dedupe_latest(load_only, key_cols=["country_code", "source_timestamp"])
    valid = deduped.filter(F.col("value") >= 0)

    enriched = _with_local_date(valid, country_timezones).withColumn(
        "interval_hours", _interval_hours_udf()(F.col("source_resolution"))
    ).withColumn("energy_mwh", F.col("value") * F.col("interval_hours"))

    result = (
        enriched.groupBy("country_code", "local_date")
        .agg(
            F.sum("energy_mwh").alias("daily_demand_mwh"),
            F.count(F.lit(1)).alias("source_interval_count"),
        )
        .withColumn("source_system", F.lit("entsoe"))
    )

    return result.select(*SILVER_DEMAND_COLUMNS)
