"""Tests for the Open-Meteo HTTP client (Spec 001).

The real Open-Meteo API is an external system, so every test here mocks
the HTTP session with `unittest.mock` — no network calls are made.
"""
from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest
import requests

from src.ingestion import open_meteo_client
from src.ingestion.open_meteo_client import (
    OpenMeteoAPIError,
    OpenMeteoRequest,
    _build_params,
    fetch_weather,
)

SAMPLE_REQUEST = OpenMeteoRequest(
    country_code="IE",
    latitude=53.3498,
    longitude=-6.2603,
    timezone="Europe/Dublin",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 2),
)


def _valid_payload():
    return {
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
            "time": ["2024-01-01", "2024-01-02"],
            "temperature_2m_mean": [4.1, 4.0],
            "wind_speed_10m_mean": [12.0, 11.5],
            "shortwave_radiation_sum": [3.2, 3.0],
        },
    }


def _mock_response(status_code=200, json_body=None, text=""):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_body if json_body is not None else {}
    response.text = text
    return response


def test_build_params_includes_required_daily_variables_and_range():
    params = _build_params(SAMPLE_REQUEST)

    assert params["daily"] == "temperature_2m_mean,wind_speed_10m_mean,shortwave_radiation_sum"
    assert "hourly" not in params  # Spec 001: no hourly ingestion for the MVP
    assert params["start_date"] == "2024-01-01"
    assert params["end_date"] == "2024-01-02"
    assert params["timezone"] == "Europe/Dublin"
    assert params["latitude"] == SAMPLE_REQUEST.latitude
    assert params["longitude"] == SAMPLE_REQUEST.longitude


def test_fetch_weather_uses_explicit_timeout():
    session = MagicMock()
    session.get.return_value = _mock_response(200, _valid_payload())

    fetch_weather(SAMPLE_REQUEST, session=session, timeout=17)

    _, kwargs = session.get.call_args
    assert kwargs["timeout"] == 17


def test_fetch_weather_returns_payload_on_success():
    session = MagicMock()
    payload = _valid_payload()
    session.get.return_value = _mock_response(200, payload)

    result = fetch_weather(SAMPLE_REQUEST, session=session)

    assert result == payload
    session.get.assert_called_once()


def test_fetch_weather_raises_after_exhausting_bounded_retries_on_5xx():
    # Retry mechanics themselves (backoff, Retry-After, attempt count) are
    # covered generically in tests/test_http.py — this just confirms
    # max_retries/sleep_fn are actually forwarded to it.
    session = MagicMock()
    session.get.return_value = _mock_response(500, {}, text="internal error")

    with pytest.raises(OpenMeteoAPIError):
        fetch_weather(SAMPLE_REQUEST, session=session, max_retries=2, sleep_fn=lambda _: None)

    assert session.get.call_count == 2  # max_retries is now the total attempt count


def test_fetch_weather_retries_are_bounded_on_connection_errors():
    session = MagicMock()
    session.get.side_effect = requests.exceptions.ConnectionError("boom")

    with pytest.raises(OpenMeteoAPIError):
        fetch_weather(SAMPLE_REQUEST, session=session, max_retries=2, sleep_fn=lambda _: None)

    assert session.get.call_count == 2  # max_retries is now the total attempt count


def test_fetch_weather_recovers_after_one_transient_failure():
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(503, {}, text="temporarily unavailable"),
        _mock_response(200, _valid_payload()),
    ]

    result = fetch_weather(SAMPLE_REQUEST, session=session, max_retries=2, sleep_fn=lambda _: None)

    assert result["daily"]["time"] == _valid_payload()["daily"]["time"]
    assert session.get.call_count == 2


def test_fetch_weather_fails_fast_on_non_retryable_client_error():
    session = MagicMock()
    session.get.return_value = _mock_response(400, {}, text="bad request")

    with pytest.raises(OpenMeteoAPIError):
        fetch_weather(SAMPLE_REQUEST, session=session, max_retries=3, sleep_fn=lambda _: None)

    session.get.assert_called_once()  # a 400 must not be retried


def test_fetch_weather_raises_on_api_error_document():
    session = MagicMock()
    session.get.return_value = _mock_response(
        200, {"error": True, "reason": "Latitude must be in range of -90 to 90 degrees"}
    )

    with pytest.raises(OpenMeteoAPIError):
        fetch_weather(SAMPLE_REQUEST, session=session)


def test_fetch_weather_raises_on_missing_expected_variable():
    session = MagicMock()
    payload = _valid_payload()
    del payload["daily"]["wind_speed_10m_mean"]
    session.get.return_value = _mock_response(200, payload)

    with pytest.raises(OpenMeteoAPIError):
        fetch_weather(SAMPLE_REQUEST, session=session)


class TestSelectEndpoint:
    """Endpoint choice is a function of date, never of execution_mode.

    The original rule branched on execution_mode — `daily` took
    Forecast, anything else took Archive. It was wrong because mode says
    nothing about whether a date has settled: a `reprocess` run over a
    date four days old was sent to Archive, where ERA5 had not settled
    it, and the task ran for an hour before its timeout killed it
    without writing a row. These tests pin the replacement so that
    cannot come back.
    """

    REFERENCE = date(2026, 11, 5)

    def test_a_recent_date_takes_the_forecast_api(self):
        url, label = open_meteo_client.select_endpoint(
            date(2026, 11, 1), reference_date=self.REFERENCE
        )
        assert url == open_meteo_client.OPEN_METEO_FORECAST_URL
        assert label == open_meteo_client.SOURCE_ENDPOINT_FORECAST

    def test_a_settled_date_takes_the_archive_api(self):
        url, label = open_meteo_client.select_endpoint(
            date(2015, 1, 1), reference_date=self.REFERENCE
        )
        assert url == open_meteo_client.OPEN_METEO_ARCHIVE_URL
        assert label == open_meteo_client.SOURCE_ENDPOINT

    def test_the_boundary_day_is_still_forecast(self):
        # FORECAST_MAX_AGE_DAYS is inclusive: at exactly the limit the
        # Forecast API still returns real values.
        boundary = self.REFERENCE - timedelta(
            days=open_meteo_client.FORECAST_MAX_AGE_DAYS
        )
        url, _ = open_meteo_client.select_endpoint(boundary, reference_date=self.REFERENCE)
        assert url == open_meteo_client.OPEN_METEO_FORECAST_URL

    def test_one_day_past_the_boundary_switches_to_archive(self):
        past = self.REFERENCE - timedelta(
            days=open_meteo_client.FORECAST_MAX_AGE_DAYS + 1
        )
        url, _ = open_meteo_client.select_endpoint(past, reference_date=self.REFERENCE)
        assert url == open_meteo_client.OPEN_METEO_ARCHIVE_URL

    def test_the_limit_stays_inside_the_measured_null_cliff(self):
        # Measured against the live API on 2026-11-05: real values at
        # 30/45/52 days back, every field null at 60/75/90. A limit at
        # or past the cliff would write nulls into Bronze, which
        # `build_bronze_records` does not filter.
        assert open_meteo_client.FORECAST_MAX_AGE_DAYS < 52

    def test_a_window_straddling_the_cliff_takes_archive(self):
        # select_endpoint is given the window's OLDEST date, so Forecast
        # is chosen only when the whole window is inside retention.
        # Archive's recent values are provisional but real; Forecast's
        # older ones would be null.
        url, _ = open_meteo_client.select_endpoint(
            date(2026, 8, 1), reference_date=self.REFERENCE
        )
        assert url == open_meteo_client.OPEN_METEO_ARCHIVE_URL
