"""Tests for Open-Meteo Bronze record construction (Spec 001)."""
from datetime import datetime, timezone

import pytest

from src.config.countries import CountryConfig
from src.ingestion.open_meteo_bronze import SOURCE_SYSTEM, build_bronze_records, business_key

IRELAND = CountryConfig(
    country_code="IE",
    country_name="Ireland",
    reference_location="Dublin",
    latitude=53.3498,
    longitude=-6.2603,
    timezone="Europe/Dublin",
)

PAYLOAD = {
    "latitude": 53.391914,
    "longitude": -6.171417,
    "timezone": "Europe/Dublin",
    "utc_offset_seconds": 3600,
    "daily_units": {
        "time": "iso8601",
        "temperature_2m_mean": "°C",
        "wind_speed_10m_mean": "km/h",
        "shortwave_radiation_sum": "MJ/m²",
    },
    "daily": {
        "time": ["2024-01-01"],
        "temperature_2m_mean": [4.1],
        "wind_speed_10m_mean": [12.0],
        "shortwave_radiation_sum": [3.2],
    },
}


def test_build_bronze_records_produces_one_row_per_daily_variable():
    records = build_bronze_records(PAYLOAD, IRELAND)
    # 3 daily variables x 1 date = 3 logical Bronze observations.
    assert len(records) == 3


def test_build_bronze_records_preserve_configured_and_returned_metadata():
    records = build_bronze_records(PAYLOAD, IRELAND)
    for row in records:
        assert row["country_code"] == "IE"
        assert row["reference_location"] == "Dublin"
        # configured reference coordinates/timezone
        assert row["latitude"] == IRELAND.latitude
        assert row["longitude"] == IRELAND.longitude
        assert row["timezone"] == "Europe/Dublin"
        # returned Open-Meteo grid coordinates/timezone/UTC offset
        assert row["returned_latitude"] == PAYLOAD["latitude"]
        assert row["returned_longitude"] == PAYLOAD["longitude"]
        assert row["returned_timezone"] == "Europe/Dublin"
        assert row["utc_offset_seconds"] == 3600
        assert row["source_endpoint"] == "archive_daily"
        assert row["source_system"] == SOURCE_SYSTEM
        assert row["ingestion_timestamp"]


def test_build_bronze_records_keeps_source_unit_per_variable():
    records = build_bronze_records(PAYLOAD, IRELAND)
    units = {row["source_variable"]: row["source_unit"] for row in records}
    assert units == {
        "temperature_2m_mean": "°C",
        "wind_speed_10m_mean": "km/h",
        "shortwave_radiation_sum": "MJ/m²",
    }


def test_build_bronze_records_keeps_all_three_daily_variables_distinct():
    records = build_bronze_records(PAYLOAD, IRELAND)
    variables = {row["source_variable"] for row in records}
    assert variables == {"temperature_2m_mean", "wind_speed_10m_mean", "shortwave_radiation_sum"}


def test_business_key_is_stable_across_reruns_with_different_ingestion_times():
    first_run = build_bronze_records(
        PAYLOAD, IRELAND, ingestion_timestamp=datetime(2024, 1, 5, tzinfo=timezone.utc)
    )
    second_run = build_bronze_records(
        PAYLOAD, IRELAND, ingestion_timestamp=datetime(2024, 1, 6, tzinfo=timezone.utc)
    )

    first_keys = sorted(business_key(r) for r in first_run)
    second_keys = sorted(business_key(r) for r in second_run)

    # Same logical observations despite different ingestion timestamps,
    # which is what allows a rerun to MERGE instead of duplicate.
    assert first_keys == second_keys


def test_business_key_is_unique_per_variable_and_date():
    records = build_bronze_records(PAYLOAD, IRELAND)
    keys = [business_key(r) for r in records]
    assert len(keys) == len(set(keys))


def test_build_bronze_records_raises_on_mismatched_series_length():
    broken_payload = {
        "daily": {
            "time": ["2024-01-01", "2024-01-02"],
            "temperature_2m_mean": [4.1],  # missing one value vs. 'time'
            "wind_speed_10m_mean": [12.0, 11.5],
            "shortwave_radiation_sum": [3.2, 3.0],
        },
    }

    with pytest.raises(ValueError):
        build_bronze_records(broken_payload, IRELAND)
