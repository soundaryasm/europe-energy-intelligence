"""Silver weather transformation (Spec 003).

Databricks-only: normalizes daily Bronze Open-Meteo observations (the
schema produced by
`src/ingestion/open_meteo_bronze.build_bronze_records`) into one row per
country/local_date. PySpark is imported lazily inside the function body
so this module stays importable without a PySpark installation; it must
actually run on Databricks against a real Bronze DataFrame, and its
correctness is covered by `tests/test_silver_weather_spark.py`
(Databricks-runtime tests, skipped locally — see that file's header).

Bronze already carries exactly one row per (country, local_date,
variable) — Open-Meteo's daily API returns `temperature_2m_mean`,
`wind_speed_10m_mean`, and `shortwave_radiation_sum` directly, so there
is no hourly series to average here. This is a normalize/pivot from tall
to wide, not an aggregation.

Open-Meteo Bronze dates are already expressed in each country's local
time (the ingestion client requests data with an explicit `timezone`
parameter per Spec 001), so deriving the local calendar date here is a
plain string-to-date parse — no UTC/local conversion is needed or
performed for weather.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from src.transformations.dedupe import dedupe_latest

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

TEMPERATURE_VARIABLE = "temperature_2m_mean"
WIND_VARIABLE = "wind_speed_10m_mean"
SOLAR_VARIABLE = "shortwave_radiation_sum"

SILVER_WEATHER_COLUMNS = (
    "country_code",
    "local_date",
    "avg_temperature_c",
    "avg_wind_speed_kmh",
    "solar_radiation_mj_m2",
    "reference_location",
    "source_system",
)


def build_silver_weather_daily(bronze_df: "DataFrame") -> "DataFrame":
    """Normalize Bronze Open-Meteo daily observations into `silver_weather_daily`."""
    from pyspark.sql import functions as F

    deduped = dedupe_latest(
        bronze_df, key_cols=["country_code", "source_variable", "observation_date"]
    )

    with_local_date = deduped.withColumn(
        "local_date", F.to_date(F.col("observation_date"), "yyyy-MM-dd")
    )

    def _metric(variable: str, output_col: str):
        return with_local_date.filter(F.col("source_variable") == variable).select(
            "country_code", "local_date", F.col("source_value").alias(output_col)
        )

    temperature = _metric(TEMPERATURE_VARIABLE, "avg_temperature_c")
    wind = _metric(WIND_VARIABLE, "avg_wind_speed_kmh")
    solar = _metric(SOLAR_VARIABLE, "solar_radiation_mj_m2")

    reference = with_local_date.select(
        "country_code", "reference_location", "source_system"
    ).dropDuplicates(["country_code"])

    result = (
        temperature.join(wind, ["country_code", "local_date"], "outer")
        .join(solar, ["country_code", "local_date"], "outer")
        .join(reference, ["country_code"], "left")
    )

    return result.select(*SILVER_WEATHER_COLUMNS)
