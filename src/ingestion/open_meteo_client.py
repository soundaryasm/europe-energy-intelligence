"""HTTP client for the Open-Meteo Historical Weather API (Spec 001).

This module only handles network I/O, request construction, and response
validation. It has no PySpark/Delta dependency so it can be exercised
entirely with plain Python and `unittest.mock` in tests, on or off
Databricks. Retry mechanics (attempt count, backoff, Retry-After) live
in `src.ingestion.http`, shared with every other ingestion client.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Mapping, Optional

from src.ingestion import http as http_client

logger = logging.getLogger(__name__)

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
SOURCE_ENDPOINT = "archive_daily"

# Forecast API (api.open-meteo.com), not the Historical/Archive API, for
# recent dates. The Historical API's ERA5-reanalysis data has a real
# settlement lag (confirmed empirically: the same date's value differs
# between the two endpoints, and the Historical API's own value for a
# very recent date is provisional and can still change on a later
# fetch) — the Forecast API's operational models (also supporting the
# same `start_date`/`end_date` params, confirmed against real responses)
# refresh every 1-6 hours instead, which fits a daily 02:00 pipeline far
# better. Backfill/reprocess of settled historical dates still use the
# Archive API above, where this lag is a non-issue.
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
SOURCE_ENDPOINT_FORECAST = "forecast_daily"

# Spec 001: "Use Open-Meteo daily variables where available rather than
# retrieving hourly observations solely to calculate daily averages...
# The MVP does not require hourly weather ingestion." The Historical
# Weather API exposes all three approved metrics directly as daily
# variables, so no hourly section is requested at all.
DAILY_VARIABLES = ("temperature_2m_mean", "wind_speed_10m_mean", "shortwave_radiation_sum")

DEFAULT_TIMEOUT_SECONDS = http_client.DEFAULT_TIMEOUT_SECONDS
DEFAULT_MAX_RETRIES = http_client.DEFAULT_MAX_ATTEMPTS


class OpenMeteoAPIError(RuntimeError):
    """Raised when the Open-Meteo API cannot be used to produce trustworthy data."""


@dataclass(frozen=True)
class OpenMeteoRequest:
    country_code: str
    latitude: float
    longitude: float
    timezone: str
    start_date: date
    end_date: date


def _build_params(request: OpenMeteoRequest) -> dict:
    return {
        "latitude": request.latitude,
        "longitude": request.longitude,
        "start_date": request.start_date.isoformat(),
        "end_date": request.end_date.isoformat(),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": request.timezone,
    }


def _validate_response_payload(payload: Any, request: OpenMeteoRequest) -> None:
    if not isinstance(payload, Mapping):
        raise OpenMeteoAPIError(
            f"Open-Meteo response for {request.country_code} was not a JSON object."
        )

    if payload.get("error"):
        reason = payload.get("reason", "unknown error")
        raise OpenMeteoAPIError(
            f"Open-Meteo API reported an error for {request.country_code}: {reason}"
        )

    daily_payload = payload.get("daily")
    if not isinstance(daily_payload, Mapping) or "time" not in daily_payload:
        raise OpenMeteoAPIError(
            f"Open-Meteo response for {request.country_code} is missing 'daily.time'."
        )
    for variable in DAILY_VARIABLES:
        if variable not in daily_payload:
            raise OpenMeteoAPIError(
                f"Open-Meteo response for {request.country_code} is missing "
                f"'daily.{variable}'."
            )


def fetch_weather(
    request: OpenMeteoRequest,
    *,
    endpoint_url: str = OPEN_METEO_ARCHIVE_URL,
    session: Optional[Any] = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict:
    """Fetch raw daily weather data for one country/date range.

    `endpoint_url` defaults to the Historical/Archive API; pass
    `OPEN_METEO_FORECAST_URL` for recent dates instead (see that
    constant's docstring for why). Both endpoints accept the identical
    `start_date`/`end_date`/`daily`/`timezone` parameters this function
    builds, so no other request-shape change is needed between them.

    `max_retries` is the total attempt count (see `src.ingestion.http`),
    which handles retrying transient network errors and 5xx/429/408
    responses. Non-retryable HTTP errors and structurally invalid
    responses raise `OpenMeteoAPIError` immediately, so callers never
    silently persist incomplete data.
    """
    params = _build_params(request)

    logger.info(
        "Requesting Open-Meteo weather: country=%s start=%s end=%s endpoint=%s",
        request.country_code,
        request.start_date,
        request.end_date,
        endpoint_url,
    )

    try:
        response = http_client.request(
            endpoint_url, params=params, timeout=timeout, session=session,
            max_attempts=max_retries, sleep_fn=sleep_fn,
        )
    except http_client.HttpRequestError as exc:
        raise OpenMeteoAPIError(
            f"Open-Meteo request for {request.country_code} failed: {exc}"
        ) from exc

    if response.status_code == 200:
        payload = response.json()
        _validate_response_payload(payload, request)
        return payload

    raise OpenMeteoAPIError(
        f"Open-Meteo request for {request.country_code} failed with status "
        f"{response.status_code}: {response.text[:500]}"
    )
