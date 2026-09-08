"""Tests for the shared HTTP retry interface used by every ingestion client.

No network calls are made — an injected `session` mock stands in for
`requests`, and an injected `sleep_fn` avoids real sleeps.
"""
from unittest.mock import MagicMock

import pytest
import requests

from src.ingestion.http import HttpRequestError, request


def _response(status_code=200, headers=None):
    return MagicMock(status_code=status_code, headers=headers or {})


def test_request_returns_response_on_first_try_success():
    session = MagicMock()
    ok = _response(200)
    session.get.return_value = ok

    result = request("http://x", session=session, sleep_fn=lambda _: None)

    assert result is ok
    assert session.get.call_count == 1


def test_request_returns_non_retryable_status_immediately_without_retry():
    # A 404 (or any status outside RETRYABLE_STATUS_CODES) is returned
    # as-is on the first attempt — the caller decides what it means.
    session = MagicMock()
    not_found = _response(404)
    session.get.return_value = not_found

    result = request("http://x", session=session, max_attempts=3, sleep_fn=lambda _: None)

    assert result is not_found
    session.get.assert_called_once()


def test_request_returns_last_response_as_is_after_exhausting_retryable_status():
    # A retryable status (502) that never recovers is NOT raised — it is
    # handed back untouched so each client keeps its own interpretation.
    session = MagicMock()
    bad_gateway = _response(502)
    session.get.return_value = bad_gateway

    result = request("http://x", session=session, max_attempts=3, sleep_fn=lambda _: None)

    assert result is bad_gateway
    assert session.get.call_count == 3


def test_request_recovers_after_one_transient_retryable_status():
    session = MagicMock()
    ok = _response(200)
    session.get.side_effect = [_response(503), ok]

    result = request("http://x", session=session, max_attempts=3, sleep_fn=lambda _: None)

    assert result is ok
    assert session.get.call_count == 2


def test_request_raises_after_exhausting_retries_on_connection_error():
    session = MagicMock()
    session.get.side_effect = requests.exceptions.ConnectionError("boom")

    with pytest.raises(HttpRequestError):
        request("http://x", session=session, max_attempts=3, sleep_fn=lambda _: None)

    assert session.get.call_count == 3


def test_request_recovers_after_one_transient_connection_error():
    session = MagicMock()
    ok = _response(200)
    session.get.side_effect = [requests.exceptions.ConnectionError("boom"), ok]

    result = request("http://x", session=session, max_attempts=3, sleep_fn=lambda _: None)

    assert result is ok
    assert session.get.call_count == 2


def test_request_honors_retry_after_seconds_header_over_exponential_backoff():
    session = MagicMock()
    ok = _response(200)
    session.get.side_effect = [_response(429, headers={"Retry-After": "7"}), ok]
    slept = []

    request("http://x", session=session, max_attempts=3, sleep_fn=lambda s: slept.append(s))

    assert slept == [7.0]


def test_request_falls_back_to_exponential_backoff_when_no_retry_after_header():
    session = MagicMock()
    ok = _response(200)
    session.get.side_effect = [_response(503), ok]
    slept = []

    request("http://x", session=session, max_attempts=3, sleep_fn=lambda s: slept.append(s))

    assert slept == [pytest.approx(2.0)]  # DEFAULT_WAIT_MIN_SECONDS on the first retry


def test_request_ignores_unparseable_retry_after_header():
    session = MagicMock()
    ok = _response(200)
    session.get.side_effect = [_response(429, headers={"Retry-After": "not-a-number"}), ok]
    slept = []

    request("http://x", session=session, max_attempts=3, sleep_fn=lambda s: slept.append(s))

    assert slept == [pytest.approx(2.0)]  # falls back to exponential backoff


def test_request_passes_params_headers_and_timeout_through():
    session = MagicMock()
    session.get.return_value = _response(200)

    request(
        "http://x",
        params={"a": 1},
        headers={"X-Test": "1"},
        timeout=17,
        session=session,
        sleep_fn=lambda _: None,
    )

    _, kwargs = session.get.call_args
    assert kwargs["params"] == {"a": 1}
    assert kwargs["headers"] == {"X-Test": "1"}
    assert kwargs["timeout"] == 17


def test_request_logs_each_attempt(caplog):
    import logging

    session = MagicMock()
    session.get.side_effect = [_response(502), _response(200)]

    with caplog.at_level(logging.WARNING, logger="src.ingestion.http"):
        request("http://x", session=session, max_attempts=3, sleep_fn=lambda _: None)

    assert any("Retrying" in record.message for record in caplog.records)
