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


def test_build_bronze_records_produces_one_row_per_variable_observation():
    records = build_bronze_records(PAYLOAD, IRELAND)
    # 2 hourly variables x 2 timestamps + 1 daily variable x 1 timestamp
    assert len(records) == 5


def test_build_bronze_records_preserve_country_and_source_metadata():
    records = build_bronze_records(PAYLOAD, IRELAND)
    for row in records:
        assert row["country_code"] == "IE"
        assert row["reference_location"] == "Dublin"
        assert row["latitude"] == IRELAND.latitude
        assert row["longitude"] == IRELAND.longitude
        assert row["timezone"] == "Europe/Dublin"
        assert row["source_system"] == SOURCE_SYSTEM
        assert row["ingestion_timestamp"]


def test_build_bronze_records_keeps_hourly_and_daily_variables_distinct():
    records = build_bronze_records(PAYLOAD, IRELAND)
    variables = {row["source_variable"] for row in records}
    assert variables == {"temperature_2m", "wind_speed_10m", "shortwave_radiation_sum"}


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


def test_business_key_is_unique_per_variable_and_timestamp():
    records = build_bronze_records(PAYLOAD, IRELAND)
    keys = [business_key(r) for r in records]
    assert len(keys) == len(set(keys))


def test_build_bronze_records_raises_on_mismatched_series_length():
    broken_payload = {
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
            "temperature_2m": [4.1],  # missing one value vs. 'time'
            "wind_speed_10m": [12.0, 11.5],
        },
        "daily": {
            "time": ["2024-01-01"],
            "shortwave_radiation_sum": [3.2],
        },
    }

    with pytest.raises(ValueError):
        build_bronze_records(broken_payload, IRELAND)
