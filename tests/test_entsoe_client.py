"""Tests for the ENTSO-E HTTP client (Spec 002).

The real ENTSO-E API is an external system, so every test here mocks the
HTTP session with `unittest.mock` — no network calls are made.
"""
from datetime import date
from unittest.mock import MagicMock

import pytest
import requests

from src.ingestion.entsoe_client import (
    ENTSOE_TOKEN_ENV_VAR,
    EntsoeAPIError,
    EntsoeRequest,
    MissingCredentialsError,
    _build_params,
    chunk_date_range,
    fetch_entsoe_document,
    get_security_token_from_env,
)
from src.ingestion.entsoe_datasets import GENERATION, LOAD, PRICE
from tests.fixtures_entsoe_xml import ACKNOWLEDGEMENT_ERROR_XML, LOAD_XML, PRICE_XML

DOMAIN = "10Y1001A1001A59C"


def _request(dataset, start=date(2024, 1, 1), end=date(2024, 1, 2)):
    return EntsoeRequest(
        country_code="IE", domain=DOMAIN, dataset=dataset, period_start=start, period_end=end
    )


def _mock_response(status_code=200, text=""):
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    return response


# --- date chunking -----------------------------------------------------

def test_chunk_date_range_splits_large_ranges_by_chunk_days():
    windows = chunk_date_range(date(2024, 1, 1), date(2024, 3, 31), chunk_days=31)
    assert windows[0] == (date(2024, 1, 1), date(2024, 1, 31))
    assert windows[-1][1] == date(2024, 3, 31)
    # windows must be contiguous with no gaps or overlaps
    for (_, prev_end), (next_start, _) in zip(windows, windows[1:]):
        assert next_start == date.fromordinal(prev_end.toordinal() + 1)


def test_chunk_date_range_single_window_when_range_fits():
    windows = chunk_date_range(date(2024, 1, 1), date(2024, 1, 5), chunk_days=90)
    assert windows == [(date(2024, 1, 1), date(2024, 1, 5))]


def test_chunk_date_range_rejects_non_positive_chunk_days():
    with pytest.raises(ValueError):
        chunk_date_range(date(2024, 1, 1), date(2024, 1, 5), chunk_days=0)


def test_chunk_date_range_rejects_inverted_range():
    with pytest.raises(ValueError):
        chunk_date_range(date(2024, 1, 5), date(2024, 1, 1))


# --- param building ------------------------------------------------------

def test_build_params_load_uses_out_bidding_zone_domain():
    params = _build_params(_request(LOAD))
    assert params["documentType"] == "A65"
    assert params["processType"] == "A16"
    assert params["outBiddingZone_Domain"] == DOMAIN
    assert "in_Domain" not in params


def test_build_params_generation_uses_in_domain():
    params = _build_params(_request(GENERATION))
    assert params["documentType"] == "A75"
    assert params["in_Domain"] == DOMAIN
    assert "outBiddingZone_Domain" not in params


def test_build_params_price_uses_in_and_out_domain_no_process_type():
    params = _build_params(_request(PRICE))
    assert params["documentType"] == "A44"
    assert params["in_Domain"] == DOMAIN
    assert params["out_Domain"] == DOMAIN
    assert "processType" not in params


def test_build_params_period_end_is_exclusive_next_day():
    params = _build_params(_request(LOAD, start=date(2024, 1, 1), end=date(2024, 1, 1)))
    assert params["periodStart"] == "202401010000"
    assert params["periodEnd"] == "202401020000"


# --- security token ------------------------------------------------------

def test_get_security_token_from_env_reads_env_var(monkeypatch):
    monkeypatch.setenv(ENTSOE_TOKEN_ENV_VAR, "dummy-token")
    assert get_security_token_from_env() == "dummy-token"


def test_get_security_token_from_env_raises_when_missing(monkeypatch):
    monkeypatch.delenv(ENTSOE_TOKEN_ENV_VAR, raising=False)
    with pytest.raises(MissingCredentialsError):
        get_security_token_from_env()


# --- fetch_entsoe_document -------------------------------------------------

def test_fetch_entsoe_document_returns_xml_on_success():
    session = MagicMock()
    session.get.return_value = _mock_response(200, LOAD_XML)

    xml_text = fetch_entsoe_document(_request(LOAD), token="tok", session=session)

    assert xml_text == LOAD_XML
    session.get.assert_called_once()


def test_fetch_entsoe_document_uses_explicit_timeout():
    session = MagicMock()
    session.get.return_value = _mock_response(200, PRICE_XML)

    fetch_entsoe_document(_request(PRICE), token="tok", session=session, timeout=12)

    _, kwargs = session.get.call_args
    assert kwargs["timeout"] == 12


def test_fetch_entsoe_document_includes_token_in_request_params():
    session = MagicMock()
    session.get.return_value = _mock_response(200, LOAD_XML)

    fetch_entsoe_document(_request(LOAD), token="super-secret", session=session)

    _, kwargs = session.get.call_args
    assert kwargs["params"]["securityToken"] == "super-secret"


def test_fetch_entsoe_document_raises_on_acknowledgement_error_document():
    session = MagicMock()
    session.get.return_value = _mock_response(200, ACKNOWLEDGEMENT_ERROR_XML)

    with pytest.raises(EntsoeAPIError, match="No matching data"):
        fetch_entsoe_document(_request(LOAD), token="tok", session=session)


def test_fetch_entsoe_document_raises_on_unexpected_root_element():
    session = MagicMock()
    session.get.return_value = _mock_response(200, PRICE_XML)  # wrong doc type for LOAD

    with pytest.raises(EntsoeAPIError):
        fetch_entsoe_document(_request(LOAD), token="tok", session=session)


def test_fetch_entsoe_document_retries_are_bounded_on_5xx():
    session = MagicMock()
    session.get.return_value = _mock_response(500, "server error")

    with pytest.raises(EntsoeAPIError):
        fetch_entsoe_document(
            _request(LOAD), token="tok", session=session, max_retries=2, sleep_fn=lambda _: None
        )

    assert session.get.call_count == 3


def test_fetch_entsoe_document_recovers_after_transient_failure():
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(503, "temporarily unavailable"),
        _mock_response(200, LOAD_XML),
    ]

    xml_text = fetch_entsoe_document(
        _request(LOAD), token="tok", session=session, max_retries=2, sleep_fn=lambda _: None
    )

    assert xml_text == LOAD_XML
    assert session.get.call_count == 2


def test_fetch_entsoe_document_fails_fast_on_non_retryable_status():
    session = MagicMock()
    session.get.return_value = _mock_response(400, "bad request")

    with pytest.raises(EntsoeAPIError):
        fetch_entsoe_document(
            _request(LOAD), token="tok", session=session, max_retries=3, sleep_fn=lambda _: None
        )

    session.get.assert_called_once()


def test_fetch_entsoe_document_retries_are_bounded_on_connection_errors():
    session = MagicMock()
    session.get.side_effect = requests.exceptions.ConnectionError("boom")

    with pytest.raises(EntsoeAPIError):
        fetch_entsoe_document(
            _request(LOAD), token="tok", session=session, max_retries=1, sleep_fn=lambda _: None
        )

    assert session.get.call_count == 2
