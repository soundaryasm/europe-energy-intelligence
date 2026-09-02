"""Tests for Open-Meteo ingestion orchestration (Spec 001).

The Databricks/PySpark write path and the HTTP fetch are both external
systems, so they are replaced with `unittest.mock` doubles here. These
tests never touch the network, Spark, or Databricks.
"""
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
        "hourly": {
            "time": ["2024-01-01T00:00"],
            "temperature_2m": [5.0],
            "wind_speed_10m": [10.0],
        },
        "daily": {
            "time": ["2024-01-01"],
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
        "hourly": {"time": [], "temperature_2m": [], "wind_speed_10m": []},
        "daily": {"time": [], "shortwave_radiation_sum": []},
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
