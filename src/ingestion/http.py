"""Shared HTTP interface: a tenacity-driven retry wrapper around `requests`.

Every ingestion client (ENTSO-E, Open-Meteo, World Bank) talks to the
network only through `request()` here. This module owns retry mechanics —
attempt count, exponential backoff, honoring a `Retry-After` response
header, and logging each attempt — and nothing else. It hands back the
raw `requests.Response` untouched; status-code interpretation and body
parsing (what counts as a logical error, what "no data" looks like, etc.)
stay with each caller, since that is specific to each API.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Mapping, Optional

import requests
from tenacity import Retrying, before_sleep_log, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 30

# "Retry 3 times total" (1 initial attempt + 2 retries), not 3 retries on
# top of the first attempt.
DEFAULT_MAX_ATTEMPTS = 3

DEFAULT_WAIT_MIN_SECONDS = 2.0
DEFAULT_WAIT_MAX_SECONDS = 30.0

# Transient failures are worth a bounded retry; client errors (bad
# request, auth, not found, a source's own logical error) are not, and
# must reach the caller immediately as a normal response instead.
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class HttpRequestError(RuntimeError):
    """Raised only when every attempt failed at the transport level (no
    response was ever received) — connection refused, DNS failure, a
    timeout. A response that came back with a retryable status code and
    never recovered is NOT raised here: it is returned as-is, so each
    caller keeps deciding what a given status means for its own API.
    """


class _RetryableResponse(Exception):
    """Internal signal carrying a response with a retryable status code
    through tenacity's retry/wait machinery. Never escapes this module.
    """

    def __init__(self, response: requests.Response):
        self.response = response
        super().__init__(f"retryable status {response.status_code}")


def _retry_after_seconds(response: requests.Response) -> Optional[float]:
    """Parse a `Retry-After` header (delay-seconds or HTTP-date form)."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    raw = raw.strip()
    if raw.isdigit():
        return float(raw)
    try:
        target = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if target is None:
        return None
    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)
    return max((target - datetime.now(timezone.utc)).total_seconds(), 0.0)


def _wait(exponential: Callable) -> Callable:
    def _wait_honoring_retry_after(retry_state) -> float:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, _RetryableResponse):
            retry_after = _retry_after_seconds(exc.response)
            if retry_after is not None:
                return retry_after
        return exponential(retry_state)

    return _wait_honoring_retry_after


def request(
    url: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, Any]] = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    session: Optional[Any] = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> requests.Response:
    """GET `url` and return the response, retrying transient failures.

    Retries, bounded to `max_attempts` total, on connection/timeout
    errors and on responses whose status is in `RETRYABLE_STATUS_CODES`.
    A `Retry-After` header on a retryable response overrides the
    exponential backoff for that one wait. Every attempt is logged.

    Any response that isn't a transport failure — a success, a
    non-retryable error, or a retryable status that never recovered — is
    returned to the caller untouched. `HttpRequestError` is raised only
    when every attempt failed at the transport level.
    """
    http = session or requests

    def _attempt() -> requests.Response:
        response = http.get(url, params=params, headers=headers, timeout=timeout)
        if response.status_code in RETRYABLE_STATUS_CODES:
            raise _RetryableResponse(response)
        return response

    retryer = Retrying(
        stop=stop_after_attempt(max_attempts),
        wait=_wait(wait_exponential(min=DEFAULT_WAIT_MIN_SECONDS, max=DEFAULT_WAIT_MAX_SECONDS)),
        retry=retry_if_exception_type((requests.exceptions.RequestException, _RetryableResponse)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        sleep=sleep_fn,
        reraise=True,
    )

    try:
        return retryer(_attempt)
    except _RetryableResponse as exc:
        return exc.response  # retries exhausted; hand the last response back as-is
    except requests.exceptions.RequestException as exc:
        raise HttpRequestError(f"Request to {url} failed after {max_attempts} attempts: {exc}") from exc
