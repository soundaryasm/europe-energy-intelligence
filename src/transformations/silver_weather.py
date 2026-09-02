"""Silver weather transformation (Spec 003).

Databricks-only: aggregates hourly/daily Bronze Open-Meteo observations
(the schema produced by `src/ingestion/open_meteo_bronze.build_bronze_records`)
into one row per country/local_date. PySpark is imported lazily inside
the function body so this module stays importable without a PySpark
installation; it must actually run on Databricks against a real Bronze
DataFrame, and its correctness is covered by
`tests/test_silver_weather_spark.py` (Databricks-runtime tests, skipped
locally — see that file's header).

Open-Meteo Bronze timestamps are already expressed in each country's
local time (the ingestion client requests data with an explicit
`timezone` parameter per Spec 001), so deriving the local calendar date
here is a plain date-of-string operation — no UTC/local conversion is
needed or performed for weather.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from src.transformations.dedupe import dedupe_latest

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

TEMPERATURE_VARIABLE = "temperature_2m"
WIND_VARIABLE = "wind_speed_10m"
SOLAR_VARIABLE = "shortwave_radiation_sum"

SILVER_WEATHER_COLUMNS = (
    "country_code",
    "local_date",
    "avg_temperature_c",
    "avg_wind_speed_kmh",
    "solar_radiation_mj_m2",
    "temperature_observation_count",
    "wind_observation_count",
    "reference_location",
    "source_system",
)


def build_silver_weather_daily(bronze_df: "DataFrame") -> "DataFrame":
    """Aggregate Bronze Open-Meteo observations into `silver_weather_daily`."""
    from pyspark.sql import functions as F

    deduped = dedupe_latest(
        bronze_df, key_cols=["country_code", "source_variable", "observation_timestamp"]
    )

    # Both hourly ("2024-01-01T00:00") and daily ("2024-01-01") Open-Meteo
    # timestamps start with the ISO date, so a plain substring is robust
    # to both shapes without needing two different format strings.
    with_local_date = deduped.withColumn(
        "local_date", F.to_date(F.substring(F.col("observation_timestamp"), 1, 10), "yyyy-MM-dd")
    )

    temperature = (
        with_local_date.filter(F.col("source_variable") == TEMPERATURE_VARIABLE)
        .groupBy("country_code", "local_date")
        .agg(
            F.avg("source_value").alias("avg_temperature_c"),
            F.count("source_value").alias("temperature_observation_count"),
        )
    )

    wind = (
        with_local_date.filter(F.col("source_variable") == WIND_VARIABLE)
        .groupBy("country_code", "local_date")
        .agg(
            F.avg("source_value").alias("avg_wind_speed_kmh"),
            F.count("source_value").alias("wind_observation_count"),
        )
    )

    solar = (
        with_local_date.filter(F.col("source_variable") == SOLAR_VARIABLE)
        .groupBy("country_code", "local_date")
        .agg(F.avg("source_value").alias("solar_radiation_mj_m2"))
    )

    reference = with_local_date.select(
        "country_code", "reference_location", "source_system"
    ).dropDuplicates(["country_code"])

    result = (
        temperature.join(wind, ["country_code", "local_date"], "outer")
        .join(solar, ["country_code", "local_date"], "outer")
        .join(reference, ["country_code"], "left")
    )

    return result.select(*SILVER_WEATHER_COLUMNS)
