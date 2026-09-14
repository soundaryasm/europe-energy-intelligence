"""Tests for the World Bank HTTP client.

The real World Bank API is an external system, so every test here mocks
the HTTP session with `unittest.mock` — no network calls are made. Mock
payload shapes below are copied from real curl responses captured during
investigation, not guessed.
"""
from unittest.mock import MagicMock

import pytest
import requests

from src.ingestion.worldbank_client import (
    WorldBankAPIError,
    WorldBankRequest,
    _build_params,
    _build_url,
    fetch_indicator,
)

SAMPLE_REQUEST = WorldBankRequest(
    indicator_code="SP.POP.TOTL",
    country_codes=["IE", "DE"],
    year=2023,
)


def _success_payload():
    return [
        {"page": 1, "pages": 1, "per_page": 1000, "total": 2, "sourceid": "2", "lastupdated": "2026-07-13"},
        [
            {
                "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                "country": {"id": "DE", "value": "Germany"},
                "countryiso3code": "DEU",
                "date": "2023",
                "value": 83287273,
                "unit": "", "obs_status": "", "decimal": 0,
            },
            {
                "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                "country": {"id": "IE", "value": "Ireland"},
                "countryiso3code": "IRL",
                "date": "2023",
                "value": 5311538,
                "unit": "", "obs_status": "", "decimal": 0,
            },
        ],
    ]


def _error_payload():
    return [{"message": [{"id": "120", "key": "Invalid value", "value": "The provided parameter value is not valid"}]}]


def _no_data_payload():
    return [{"page": 0, "pages": 0, "per_page": 0, "total": 0, "sourceid": None, "lastupdated": None}, None]


def _mock_response(status_code=200, json_body=None, text=""):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_body if json_body is not None else {}
    response.text = text
    return response


def test_build_url_semicolon_joins_countries():
    url = _build_url(SAMPLE_REQUEST)
    assert url == "https://api.worldbank.org/v2/country/IE;DE/indicator/SP.POP.TOTL"


def test_build_params_uses_year_as_exact_date():
    params = _build_params(SAMPLE_REQUEST)
    assert params["date"] == "2023"
    assert params["format"] == "json"


def test_fetch_indicator_returns_records_on_success():
    session = MagicMock()
    session.get.return_value = _mock_response(200, _success_payload())

    records = fetch_indicator(SAMPLE_REQUEST, session=session)

    assert len(records) == 2
    assert {r["country"]["id"] for r in records} == {"DE", "IE"}


def test_fetch_indicator_returns_empty_list_when_genuinely_no_data():
    # Real case: World Bank returns HTTP 200 with a null data element for
    # a valid country/indicator/year combination with nothing recorded —
    # this must not be treated as an error.
    session = MagicMock()
    session.get.return_value = _mock_response(200, _no_data_payload())

    records = fetch_indicator(SAMPLE_REQUEST, session=session)

    assert records == []


def test_fetch_indicator_raises_on_worldbank_logical_error():
    # Real case: World Bank returns HTTP 200 even for its own "invalid
    # parameter" error — must be detected from the response body.
    session = MagicMock()
    session.get.return_value = _mock_response(200, _error_payload())

    with pytest.raises(WorldBankAPIError):
        fetch_indicator(SAMPLE_REQUEST, session=session)


def test_fetch_indicator_uses_explicit_timeout():
    session = MagicMock()
    session.get.return_value = _mock_response(200, _success_payload())

    fetch_indicator(SAMPLE_REQUEST, session=session, timeout=17)

    _, kwargs = session.get.call_args
    assert kwargs["timeout"] == 17


def test_fetch_indicator_raises_after_exhausting_bounded_retries_on_5xx():
    # Retry mechanics themselves (backoff, Retry-After, attempt count) are
    # covered generically in tests/test_http.py — this just confirms
    # max_retries/sleep_fn are actually forwarded to it.
    session = MagicMock()
    session.get.return_value = _mock_response(500, {}, text="internal error")

    with pytest.raises(WorldBankAPIError):
        fetch_indicator(SAMPLE_REQUEST, session=session, max_retries=2, sleep_fn=lambda _: None)

    assert session.get.call_count == 2  # max_retries is now the total attempt count


def test_fetch_indicator_retries_are_bounded_on_connection_errors():
    session = MagicMock()
    session.get.side_effect = requests.exceptions.ConnectionError("boom")

    with pytest.raises(WorldBankAPIError):
        fetch_indicator(SAMPLE_REQUEST, session=session, max_retries=2, sleep_fn=lambda _: None)

    assert session.get.call_count == 2  # max_retries is now the total attempt count


def test_fetch_indicator_recovers_after_one_transient_failure():
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(503, {}, text="temporarily unavailable"),
        _mock_response(200, _success_payload()),
    ]

    records = fetch_indicator(SAMPLE_REQUEST, session=session, max_retries=2, sleep_fn=lambda _: None)

    assert len(records) == 2
    assert session.get.call_count == 2


def test_fetch_indicator_fails_fast_on_non_retryable_client_error():
    # Real case: an oversized per_page gets a hard HTTP 400 (HTML body,
    # not JSON) — must not be retried.
    session = MagicMock()
    session.get.return_value = _mock_response(400, {}, text="<html>Request Error</html>")

    with pytest.raises(WorldBankAPIError):
        fetch_indicator(SAMPLE_REQUEST, session=session, max_retries=3, sleep_fn=lambda _: None)

    session.get.assert_called_once()  # a 400 must not be retried
