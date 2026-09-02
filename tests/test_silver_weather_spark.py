"""Databricks-runtime tests for the Silver weather transformation (Spec 003).

These exercise the REAL PySpark transformation logic in
`src/transformations/silver_weather.py` against small literal Spark
DataFrames. They are NOT executed locally: this sandbox has no JVM/PySpark
installation, and per project policy production/Spark workloads only run
on Databricks. `pytest.importorskip("pyspark")` below causes pytest to
automatically SKIP this entire module wherever PySpark is unavailable
(including here) and to run it for real wherever it is available (a
Databricks cluster, or a CI runner with PySpark installed). None of the
transformation logic itself is mocked.
"""
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

from src.config.countries import CountryConfig
from src.ingestion.open_meteo_bronze import build_bronze_records
from src.transformations.silver_weather import build_silver_weather_daily

pytestmark = pytest.mark.databricks

IRELAND = CountryConfig("IE", "Ireland", "Dublin", 53.3498, -6.2603, "Europe/Dublin")

PAYLOAD = {
    "hourly": {
        "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
        "temperature_2m": [4.1, 4.0],
        "wind_speed_10m": [12.0, 11.5],
    },
    "daily": {
        "time": ["2024-01-01"],
        "shortwave_radiation_sum": [3.2],
    },
}


def test_build_silver_weather_daily_aggregates_one_row_per_country_date(spark_session):
    rows = build_bronze_records(PAYLOAD, IRELAND)
    bronze_df = spark_session.createDataFrame(rows)

    result = build_silver_weather_daily(bronze_df).collect()

    assert len(result) == 1
    row = result[0].asDict()
    assert row["country_code"] == "IE"
    assert str(row["local_date"]) == "2024-01-01"
    assert row["avg_temperature_c"] == pytest.approx(4.05)
    assert row["avg_wind_speed_kmh"] == pytest.approx(11.75)
    assert row["solar_radiation_mj_m2"] == pytest.approx(3.2)
    assert row["temperature_observation_count"] == 2
    assert row["wind_observation_count"] == 2
    assert row["reference_location"] == "Dublin"
    assert row["source_system"] == "open-meteo"


def test_build_silver_weather_daily_keeps_countries_separate(spark_session):
    germany = CountryConfig("DE", "Germany", "Berlin", 52.5200, 13.4050, "Europe/Berlin")
    rows = build_bronze_records(PAYLOAD, IRELAND) + build_bronze_records(PAYLOAD, germany)
    bronze_df = spark_session.createDataFrame(rows)

    result = build_silver_weather_daily(bronze_df).collect()

    assert {row.country_code for row in result} == {"IE", "DE"}


def test_build_silver_weather_daily_dedupes_reruns_to_latest_ingestion(spark_session):
    # Two "runs" of the exact same source observation with different
    # values (simulating a rerun that fetched revised data) — only the
    # most recently ingested value should count, not an average of both.
    schema_rows = [
        {
            "country_code": "IE",
            "country_name": "Ireland",
            "reference_location": "Dublin",
            "latitude": 53.3498,
            "longitude": -6.2603,
            "timezone": "Europe/Dublin",
            "observation_timestamp": "2024-01-01T00:00",
            "source_variable": "temperature_2m",
            "source_value": 999.0,  # stale value from an earlier run
            "source_resolution": "hourly",
            "source_system": "open-meteo",
            "ingestion_timestamp": datetime(2024, 1, 5, tzinfo=timezone.utc).isoformat(),
        },
        {
            "country_code": "IE",
            "country_name": "Ireland",
            "reference_location": "Dublin",
            "latitude": 53.3498,
            "longitude": -6.2603,
            "timezone": "Europe/Dublin",
            "observation_timestamp": "2024-01-01T00:00",
            "source_variable": "temperature_2m",
            "source_value": 4.1,  # latest, correct value
            "source_resolution": "hourly",
            "source_system": "open-meteo",
            "ingestion_timestamp": datetime(2024, 1, 6, tzinfo=timezone.utc).isoformat(),
        },
    ]
    bronze_df = spark_session.createDataFrame(schema_rows)

    result = build_silver_weather_daily(bronze_df).collect()

    assert len(result) == 1
    assert result[0].avg_temperature_c == pytest.approx(4.1)
    assert result[0].temperature_observation_count == 1  # not 2 — the stale row was dropped
