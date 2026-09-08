"""HTTP client for the World Bank Indicators API (v2).

This module only handles network I/O, request construction, and response
validation. It has no PySpark/Delta dependency so it can be exercised
entirely with plain Python and `unittest.mock` in tests, on or off
Databricks.

No API key is required (confirmed against the official World Bank Data
Help Desk docs: "API keys and other authentication methods are no longer
necessary to access the API."). No official rate limit is documented
anywhere on that site — this client still retries bounded on transient
HTTP failures as a defensive default, not because a documented limit
requires it. Retry mechanics (attempt count, backoff, Retry-After) live
in `src.ingestion.http`, shared with every other ingestion client.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Sequence

from src.ingestion import http as http_client

logger = logging.getLogger(__name__)

WORLDBANK_API_URL = "https://api.worldbank.org/v2"

DEFAULT_TIMEOUT_SECONDS = http_client.DEFAULT_TIMEOUT_SECONDS
DEFAULT_MAX_RETRIES = http_client.DEFAULT_MAX_ATTEMPTS

# 1000 comfortably covers our whole country set for one year/indicator in
# a single page (confirmed: 15 countries x full 2015-2026 range = 165
# records fit in one page at per_page=500). Confirmed empirically that
# per_page=100000 gets a hard HTTP 400 from the server; per_page=20000
# works. 1000 stays nowhere near that boundary.
DEFAULT_PER_PAGE = 1000


class WorldBankAPIError(RuntimeError):
    """Raised when the World Bank API cannot be used to produce trustworthy data."""


@dataclass(frozen=True)
class WorldBankRequest:
    indicator_code: str
    country_codes: Sequence[str]
    year: int


def _build_url(request: WorldBankRequest) -> str:
    countries = ";".join(request.country_codes)
    return f"{WORLDBANK_API_URL}/country/{countries}/indicator/{request.indicator_code}"


def _build_params(request: WorldBankRequest) -> dict:
    return {
        "format": "json",
        "date": str(request.year),
        "per_page": DEFAULT_PER_PAGE,
    }


def _extract_records(payload: Any, request: WorldBankRequest) -> List[dict]:
    """Validate and unwrap a World Bank `[metadata, records]` response.

    Confirmed against real responses — three distinct shapes:
    - error: `[{"message": [...]}]`, a 1-element list (still HTTP 200).
    - genuinely no data for this country/indicator/year: `[metadata, null]`.
    - success: `[metadata, [...records...]]`.
    """
    if not isinstance(payload, list) or len(payload) == 0:
        raise WorldBankAPIError(
            f"World Bank response for indicator {request.indicator_code!r} "
            f"was not the expected [metadata, records] list: {payload!r}"
        )

    metadata = payload[0]
    if isinstance(metadata, dict) and "message" in metadata:
        raise WorldBankAPIError(
            f"World Bank API reported an error for indicator "
            f"{request.indicator_code!r} (countries={list(request.country_codes)}, "
            f"year={request.year}): {metadata['message']}"
        )

    if len(payload) < 2 or payload[1] is None:
        return []  # genuinely no data — not an error, just nothing to persist

    return payload[1]


def fetch_indicator(
    request: WorldBankRequest,
    *,
    session: Optional[Any] = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> List[dict]:
    """Fetch one indicator for a set of countries in one calendar year.

    One call already covers every requested country for the year (World
    Bank's semicolon-separated country-list syntax, confirmed against a
    real response for all 15 MVP countries at once) — there is no need to
    call this once per country.

    `max_retries` is the total attempt count (see `src.ingestion.http`),
    which handles retrying transient network errors and 5xx/429/408
    responses. A World-Bank-reported logical error, or an HTTP error that
    never recovered, raises `WorldBankAPIError` immediately, so callers
    never silently persist incomplete data. A genuinely empty result (no
    data for this indicator/year) returns `[]`, not an error.
    """
    url = _build_url(request)
    params = _build_params(request)

    logger.info(
        "Requesting World Bank indicator=%s countries=%s year=%s",
        request.indicator_code, list(request.country_codes), request.year,
    )

    try:
        response = http_client.request(
            url, params=params, timeout=timeout, session=session,
            max_attempts=max_retries, sleep_fn=sleep_fn,
        )
    except http_client.HttpRequestError as exc:
        raise WorldBankAPIError(
            f"World Bank request for indicator {request.indicator_code!r} failed: {exc}"
        ) from exc

    if response.status_code == 200:
        return _extract_records(response.json(), request)

    raise WorldBankAPIError(
        f"World Bank request for indicator {request.indicator_code!r} failed with "
        f"status {response.status_code}: {response.text[:500]}"
    )
