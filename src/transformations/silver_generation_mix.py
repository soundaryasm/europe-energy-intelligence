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

from src.ingestion.entsoe_xml import parse_iso8601_duration_minutes
from src.transformations.dedupe import dedupe_latest
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
    "source_system",
)


def _interval_hours_udf():
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    def _hours(resolution: str) -> float:
        return parse_iso8601_duration_minutes(resolution) / 60.0

    return F.udf(_hours, DoubleType())


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


def build_silver_generation_mix_daily(bronze_df: "DataFrame", country_timezones: Dict[str, str]) -> "DataFrame":
    """Aggregate Bronze ENTSO-E generation observations into `silver_generation_mix_daily`."""
    from pyspark.sql import functions as F

    generation_only = bronze_df.filter(F.col("dataset_type") == "generation")
    deduped = dedupe_latest(
        generation_only, key_cols=["country_code", "source_timestamp", "production_type_raw"]
    )
    valid = deduped.filter(F.col("value") >= 0)

    normalize_udf = _normalize_production_type_udf()

    enriched = (
        _with_local_date(valid, country_timezones)
        .withColumn("interval_hours", _interval_hours_udf()(F.col("source_resolution")))
        .withColumn("generation_mwh_interval", F.col("value") * F.col("interval_hours"))
        .withColumn("_normalized", normalize_udf(F.col("production_type_raw")))
        .withColumn("normalized_production_type", F.col("_normalized.normalized_category"))
        .withColumn("renewable_flag", F.col("_normalized.renewable"))
    )

    result = (
        enriched.groupBy("country_code", "local_date", "normalized_production_type")
        .agg(
            F.sum("generation_mwh_interval").alias("generation_mwh"),
            F.first("renewable_flag").alias("renewable_flag"),
            F.sort_array(F.collect_set("production_type_raw")).alias("production_type_raw_codes"),
        )
        .withColumn("source_system", F.lit("entsoe"))
    )

    return result.select(*SILVER_GENERATION_MIX_COLUMNS)
