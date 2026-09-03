"""Silver generation mix transformation (Spec 003).

Databricks-only: aggregates Bronze ENTSO-E generation-by-production-type
observations (schema produced by
`src/ingestion/entsoe_bronze.build_bronze_records` for
dataset_type="generation") into daily generation (MWh) per
country/local_date/normalized_production_type, using the centralized
mapping in `src/transformations/production_types.py`. PySpark is imported
lazily inside function bodies so this module stays importable without a
PySpark installation; correctness against real Spark DataFrames is
covered by `tests/test_silver_generation_mix_spark.py` (Databricks-runtime
tests, skipped locally).

Grain: Spec 003 states the generation-mix grain as
`country + local_date + normalized_production_type` (matching the Gold
`fact_generation_mix_daily` grain in Spec 004/005). Where multiple raw
ENTSO-E psrType codes map to the same normalized category (e.g. two coal
subtypes), their generation is summed into one row, and every
contributing raw code is retained in `production_type_raw_codes` for
traceability rather than picking one arbitrarily.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict

from src.transformations.dedupe import dedupe_latest
from src.transformations.entsoe_silver_common import (
    interval_hours_udf,
    with_completeness_status,
    with_local_date,
)
from src.transformations.production_types import get_production_type_info

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

SILVER_GENERATION_MIX_COLUMNS = (
    "country_code",
    "local_date",
    "normalized_production_type",
    "renewable_flag",
    "generation_mwh",
    "production_type_raw_codes",
    "covered_duration_hours",
    "completeness_status",
    "source_system",
)


def _normalize_production_type_udf():
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("normalized_category", StringType(), False),
            StructField("renewable", BooleanType(), False),
        ]
    )

    def _normalize(raw_code):
        info = get_production_type_info(raw_code)
        return (info.normalized_category, info.renewable)

    return F.udf(_normalize, schema)


def build_silver_generation_mix_daily(bronze_df: "DataFrame", country_timezones: Dict[str, str]) -> "DataFrame":
    """Aggregate Bronze ENTSO-E generation observations into `silver_generation_mix_daily`.

    `completeness_status` is evaluated per production-type series (Spec
    003 "Generation Completeness": "Completeness must be evaluated using
    timeline coverage for each relevant production-type series"), using
    the same expected-local-day-duration rule as demand/price.
    """
    from pyspark.sql import functions as F

    generation_only = bronze_df.filter(F.col("dataset_type") == "generation")
    deduped = dedupe_latest(
        generation_only, key_cols=["country_code", "source_timestamp", "production_type_raw"]
    )
    valid = deduped.filter(F.col("value") >= 0)

    normalize_udf = _normalize_production_type_udf()

    enriched = (
        with_local_date(valid, country_timezones)
        .withColumn("interval_hours", interval_hours_udf()(F.col("source_resolution")))
        .withColumn("generation_mwh_interval", F.col("value") * F.col("interval_hours"))
        .withColumn("_normalized", normalize_udf(F.col("production_type_raw")))
        .withColumn("normalized_production_type", F.col("_normalized.normalized_category"))
        .withColumn("renewable_flag", F.col("_normalized.renewable"))
    )

    aggregated = (
        enriched.groupBy("country_code", "local_date", "normalized_production_type")
        .agg(
            F.sum("generation_mwh_interval").alias("generation_mwh"),
            F.first("renewable_flag").alias("renewable_flag"),
            F.sort_array(F.collect_set("production_type_raw")).alias("production_type_raw_codes"),
            F.sum("interval_hours").alias("covered_duration_hours"),
        )
        .withColumn("source_system", F.lit("entsoe"))
    )

    result = with_completeness_status(aggregated, country_timezones)

    return result.select(*SILVER_GENERATION_MIX_COLUMNS)
