"""Tests for the Open-Meteo HTTP client (Spec 001).

The real Open-Meteo API is an external system, so every test here mocks
the HTTP session with `unittest.mock` — no network calls are made.
"""
from datetime import date
from unittest.mock import MagicMock

import pytest
import requests

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


def _mock_response(status_code=200, json_body=None, text=""):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_body if json_body is not None else {}
    response.text = text
    return response


def test_build_params_includes_required_variables_and_range():
    params = _build_params(SAMPLE_REQUEST)

    assert params["hourly"] == "temperature_2m,wind_speed_10m"
    assert params["daily"] == "shortwave_radiation_sum"
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
    session = MagicMock()
    session.get.return_value = _mock_response(500, {}, text="internal error")

    with pytest.raises(OpenMeteoAPIError):
        fetch_weather(SAMPLE_REQUEST, session=session, max_retries=1, sleep_fn=lambda _: None)

    assert session.get.call_count == 2  # 1 initial attempt + 1 retry, never unbounded


def test_fetch_weather_retries_are_bounded_on_connection_errors():
    session = MagicMock()
    session.get.side_effect = requests.exceptions.ConnectionError("boom")

    with pytest.raises(OpenMeteoAPIError):
        fetch_weather(SAMPLE_REQUEST, session=session, max_retries=2, sleep_fn=lambda _: None)

    assert session.get.call_count == 3  # 1 initial attempt + 2 retries


def test_fetch_weather_recovers_after_one_transient_failure():
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(503, {}, text="temporarily unavailable"),
        _mock_response(200, _valid_payload()),
    ]

    result = fetch_weather(SAMPLE_REQUEST, session=session, max_retries=2, sleep_fn=lambda _: None)

    assert result["hourly"]["time"] == _valid_payload()["hourly"]["time"]
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
    del payload["hourly"]["wind_speed_10m"]
    session.get.return_value = _mock_response(200, payload)

    with pytest.raises(OpenMeteoAPIError):
        fetch_weather(SAMPLE_REQUEST, session=session)
