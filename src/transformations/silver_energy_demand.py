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

from src.transformations.dedupe import dedupe_latest
from src.transformations.entsoe_silver_common import (
    interval_hours_udf,
    with_completeness_status,
    with_local_date,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

SILVER_DEMAND_COLUMNS = (
    "country_code",
    "local_date",
    "daily_demand_mwh",
    "source_interval_count",
    "covered_duration_hours",
    "completeness_status",
    "source_system",
)


def build_silver_energy_demand_daily(bronze_df: "DataFrame", country_timezones: Dict[str, str]) -> "DataFrame":
    """Aggregate Bronze ENTSO-E load observations into `silver_energy_demand_daily`.

    `energy_mwh = load_mw x interval_duration_hours` per source interval
    (Spec 003), summed per country/local_date. Negative load readings are
    excluded (Spec 003/007: "demand is not negative") rather than
    silently coerced to zero. `completeness_status` is `complete` only
    when the summed interval coverage reaches the expected duration of
    that local calendar day (Spec 003 "Demand Completeness") — a partial
    day's `daily_demand_mwh` is still computed here (for diagnosis) but
    Gold must not treat it as trusted (Spec 004 "Trusted Silver Inputs").
    """
    from pyspark.sql import functions as F

    load_only = bronze_df.filter(F.col("dataset_type") == "load")
    deduped = dedupe_latest(load_only, key_cols=["country_code", "source_timestamp"])
    valid = deduped.filter(F.col("value") >= 0)

    enriched = with_local_date(valid, country_timezones).withColumn(
        "interval_hours", interval_hours_udf()(F.col("source_resolution"))
    ).withColumn("energy_mwh", F.col("value") * F.col("interval_hours"))

    aggregated = (
        enriched.groupBy("country_code", "local_date")
        .agg(
            F.sum("energy_mwh").alias("daily_demand_mwh"),
            F.count(F.lit(1)).alias("source_interval_count"),
            F.sum("interval_hours").alias("covered_duration_hours"),
        )
        .withColumn("source_system", F.lit("entsoe"))
    )

    result = with_completeness_status(aggregated, country_timezones)

    return result.select(*SILVER_DEMAND_COLUMNS)
