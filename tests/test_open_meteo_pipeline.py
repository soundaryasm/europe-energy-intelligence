"""Tests for Open-Meteo ingestion orchestration (Spec 001).

The Databricks/PySpark write path and the HTTP fetch are both external
systems, so they are replaced with `unittest.mock` doubles here. These
tests never touch the network, Spark, or Databricks.
"""
import threading
import time
from datetime import date
from unittest.mock import MagicMock

import pytest

from src.config.countries import CountryConfig
from src.ingestion.open_meteo_client import OpenMeteoAPIError
from src.ingestion.open_meteo_pipeline import (
    backfill_date_range,
    daily_processing_date,
    run_ingestion,
)

IRELAND = CountryConfig("IE", "Ireland", "Dublin", 53.3498, -6.2603, "Europe/Dublin")
GERMANY = CountryConfig("DE", "Germany", "Berlin", 52.5200, 13.4050, "Europe/Berlin")


def _payload_for(country_code):
    return {
        "latitude": 53.39,
        "longitude": -6.17,
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
            "temperature_2m_mean": [5.0],
            "wind_speed_10m_mean": [10.0],
            "shortwave_radiation_sum": [2.0],
        },
    }


def test_daily_processing_date_is_the_most_recently_completed_day():
    assert daily_processing_date(date(2024, 3, 15)) == date(2024, 3, 14)


def test_backfill_date_range_is_configurable_not_hardcoded():
    start, end = backfill_date_range(months=24, end_date=date(2024, 3, 15))

    assert end == date(2024, 3, 15)
    assert start < end
    months_span = (end.year - start.year) * 12 + (end.month - start.month)
    assert months_span in (23, 24)


def test_backfill_date_range_defaults_to_yesterday_when_no_end_date_given():
    _, end = backfill_date_range(months=1)
    assert end == daily_processing_date()


def test_backfill_date_range_rejects_non_positive_months():
    with pytest.raises(ValueError):
        backfill_date_range(months=0)


def test_run_ingestion_rejects_inverted_date_range():
    with pytest.raises(ValueError):
        run_ingestion(date(2024, 1, 5), date(2024, 1, 1), countries=[IRELAND])


def test_run_ingestion_writes_records_for_all_successful_countries():
    fetch_fn = MagicMock(side_effect=lambda request, **_: _payload_for(request.country_code))
    spark_writer = MagicMock(return_value=99)

    result = run_ingestion(
        date(2024, 1, 1),
        date(2024, 1, 1),
        spark=MagicMock(),
        countries=[IRELAND, GERMANY],
        fetch_fn=fetch_fn,
        spark_writer=spark_writer,
    )

    assert result.countries_attempted == ["IE", "DE"]
    assert result.countries_succeeded == ["IE", "DE"]
    assert result.countries_failed == []
    assert result.succeeded is True
    assert result.records_written == 99
    assert result.ended_at is not None and result.ended_at >= result.started_at

    spark_writer.assert_called_once()
    written_records = spark_writer.call_args[0][1]
    assert len(written_records) == 6  # 3 variables x 1 timestamp x 2 countries


def test_run_ingestion_records_country_failure_without_stopping_others():
    def fetch_side_effect(request, **_):
        if request.country_code == "IE":
            raise OpenMeteoAPIError("boom")
        return _payload_for(request.country_code)

    fetch_fn = MagicMock(side_effect=fetch_side_effect)
    spark_writer = MagicMock(return_value=3)

    result = run_ingestion(
        date(2024, 1, 1),
        date(2024, 1, 1),
        spark=MagicMock(),
        countries=[IRELAND, GERMANY],
        fetch_fn=fetch_fn,
        spark_writer=spark_writer,
    )

    assert result.countries_failed == ["IE"]
    assert result.countries_succeeded == ["DE"]
    assert result.succeeded is False
    assert "IE" in result.errors  # failure must be visible, not silently ignored

    spark_writer.assert_called_once()
    written_records = spark_writer.call_args[0][1]
    assert all(r["country_code"] == "DE" for r in written_records)


def test_run_ingestion_does_not_write_when_every_country_fails():
    fetch_fn = MagicMock(side_effect=OpenMeteoAPIError("all down"))
    spark_writer = MagicMock()

    result = run_ingestion(
        date(2024, 1, 1),
        date(2024, 1, 1),
        spark=MagicMock(),
        countries=[IRELAND],
        fetch_fn=fetch_fn,
        spark_writer=spark_writer,
    )

    assert result.succeeded is False
    assert result.records_written == 0
    spark_writer.assert_not_called()  # no partial/empty write on total failure


def test_run_ingestion_treats_empty_response_as_a_failure_not_a_silent_success():
    empty_payload = {
        "daily": {
            "time": [],
            "temperature_2m_mean": [],
            "wind_speed_10m_mean": [],
            "shortwave_radiation_sum": [],
        },
    }
    fetch_fn = MagicMock(return_value=empty_payload)
    spark_writer = MagicMock()

    result = run_ingestion(
        date(2024, 1, 1),
        date(2024, 1, 1),
        spark=MagicMock(),
        countries=[IRELAND],
        fetch_fn=fetch_fn,
        spark_writer=spark_writer,
    )

    assert result.countries_failed == ["IE"]
    spark_writer.assert_not_called()


def test_run_ingestion_is_idempotent_across_reruns():
    fetch_fn = MagicMock(side_effect=lambda request, **_: _payload_for(request.country_code))
    spark_writer = MagicMock(return_value=3)

    first_run = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        spark=MagicMock(), countries=[IRELAND], fetch_fn=fetch_fn, spark_writer=spark_writer,
    )
    second_run = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        spark=MagicMock(), countries=[IRELAND], fetch_fn=fetch_fn, spark_writer=spark_writer,
    )

    first_records = spark_writer.call_args_list[0][0][1]
    second_records = spark_writer.call_args_list[1][0][1]

    from src.ingestion.open_meteo_bronze import business_key

    first_keys = sorted(business_key(r) for r in first_records)
    second_keys = sorted(business_key(r) for r in second_records)
    assert first_keys == second_keys  # reruns produce the same logical records


def test_run_ingestion_stops_new_requests_once_breaker_trips():
    countries = [
        CountryConfig("F1", "Fail1", "X", 0.0, 0.0, "UTC"),
        CountryConfig("F2", "Fail2", "X", 0.0, 0.0, "UTC"),
        CountryConfig("F3", "Fail3", "X", 0.0, 0.0, "UTC"),
        CountryConfig("S4", "Skip4", "X", 0.0, 0.0, "UTC"),
        CountryConfig("S5", "Skip5", "X", 0.0, 0.0, "UTC"),
    ]

    def _fetch(request, **_):
        if request.country_code in ("F1", "F2", "F3"):
            raise OpenMeteoAPIError("down")
        return _payload_for(request.country_code)  # would succeed if actually attempted

    fetch_fn = MagicMock(side_effect=_fetch)

    result = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        spark=MagicMock(), countries=countries, fetch_fn=fetch_fn,
        spark_writer=MagicMock(return_value=0), max_workers=1, circuit_breaker_threshold=3,
    )

    assert fetch_fn.call_count == 3  # F1, F2, F3 only — S4/S5 skipped, breaker already open
    assert set(result.countries_failed) == {"F1", "F2", "F3", "S4", "S5"}
    assert "circuit breaker" in result.errors["S4"].lower()


def test_run_ingestion_does_not_trip_breaker_on_a_single_countrys_failure():
    def _fetch(request, **_):
        if request.country_code == "IE":
            raise OpenMeteoAPIError("IE-specific issue")
        return _payload_for(request.country_code)

    result = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        spark=MagicMock(), countries=[IRELAND, GERMANY], fetch_fn=_fetch,
        spark_writer=MagicMock(return_value=0), circuit_breaker_threshold=3,
    )

    assert result.countries_failed == ["IE"]
    assert result.countries_succeeded == ["DE"]
    assert "circuit breaker" not in result.errors["IE"].lower()


def test_run_ingestion_processes_countries_concurrently():
    lock = threading.Lock()
    state = {"current": 0, "max_seen": 0}

    def fetch_fn(request, **_):
        with lock:
            state["current"] += 1
            state["max_seen"] = max(state["max_seen"], state["current"])
        time.sleep(0.05)
        with lock:
            state["current"] -= 1
        return _payload_for(request.country_code)

    countries = [CountryConfig(f"C{i}", f"Country{i}", "X", 0.0, 0.0, "UTC") for i in range(5)]

    run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        spark=MagicMock(), countries=countries, fetch_fn=fetch_fn,
        spark_writer=MagicMock(return_value=0), max_workers=5,
    )

    assert 1 < state["max_seen"] <= 5
