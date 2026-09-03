"""HTTP client for the ENTSO-E Transparency Platform Web API (Spec 002).

Only handles network I/O, request construction, and response validation
at the HTTP/XML-envelope level. Detailed XML parsing lives in
`entsoe_xml.py`. No PySpark/Delta dependency lives here, so it can be
exercised entirely with plain Python and `unittest.mock` in tests.

IMPORTANT: the exact ENTSO-E request parameter names and response
envelope implemented here follow ENTSO-E's publicly documented API guide.
They have NOT been verified against a live ENTSO-E account/response in
this environment (no API token was available) and must be validated on
first real execution with real credentials.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone as dt_timezone
from typing import Any, Callable, List, Optional, Tuple
from xml.etree import ElementTree as ET

import requests

from src.ingestion.entsoe_datasets import EntsoeDataset

logger = logging.getLogger(__name__)

ENTSOE_API_URL = "https://web-api.tp.entsoe.eu/api"
ENTSOE_TOKEN_ENV_VAR = "ENTSOE_API_TOKEN"

DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 2.0
DEFAULT_CHUNK_DAYS = 90

_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

# ENTSO-E signals an application-level error with this document, even on
# HTTP 200 (and also uses it for HTTP 4xx bodies).
_ACKNOWLEDGEMENT_ROOT_TAG = "Acknowledgement_MarketDocument"

# ENTSO-E reuses the same Acknowledgement_MarketDocument shape both for
# "the request was understood but no data exists for this period" and
# for genuine errors (bad parameters, auth failures, etc). The reason
# CODE alone is not a reliable signal — ENTSO-E documents code 999 as a
# generic/reused code covering multiple different conditions — so
# classification inspects the reason TEXT for known "no data" phrasing
# instead (Spec 007 "Source Availability": distinguish source
# legitimately unavailable from an actual request failure).
_NO_DATA_TEXT_MARKERS = ("no matching data found",)


class EntsoeAPIError(RuntimeError):
    """Raised when the ENTSO-E API cannot be used to produce trustworthy data."""


class EntsoeNoDataError(EntsoeAPIError):
    """Raised when ENTSO-E acknowledges the request but reports no matching
    data for the requested country/dataset/period.

    This is a distinct condition from a technical `EntsoeAPIError`: the
    source is legitimately unavailable for this request, not a broken
    ingestion run (Spec 006 "Partial Source Data" / Spec 007 "Source
    Availability"). Callers must classify this separately — e.g. as
    `unavailable` rather than `failed` — instead of treating every
    Acknowledgement_MarketDocument as a hard failure.
    """


class MissingCredentialsError(RuntimeError):
    """Raised when the ENTSO-E security token cannot be found."""


@dataclass(frozen=True)
class EntsoeRequest:
    country_code: str
    domain: str
    dataset: EntsoeDataset
    period_start: date
    period_end: date


def get_security_token_from_env() -> str:
    """Read the ENTSO-E security token from the environment (local/dev use).

    On Databricks, the token must instead be retrieved via
    `dbutils.secrets.get(scope=..., key=...)` in the notebook entry point
    and passed explicitly into the pipeline — never hard-coded and never
    logged.
    """
    token = os.environ.get(ENTSOE_TOKEN_ENV_VAR)
    if not token:
        raise MissingCredentialsError(
            f"{ENTSOE_TOKEN_ENV_VAR} is not set. On Databricks, retrieve the token via "
            "Databricks-managed secrets (dbutils.secrets.get) and pass it explicitly; "
            "do not hard-code it."
        )
    return token


def chunk_date_range(
    start_date: date, end_date: date, chunk_days: int = DEFAULT_CHUNK_DAYS
) -> List[Tuple[date, date]]:
    """Split a date range into bounded windows for large historical requests.

    The chunk size is a parameter (not hard-coded elsewhere) per Spec 002's
    "configurable chunking strategy" requirement.
    """
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    if chunk_days <= 0:
        raise ValueError("chunk_days must be a positive integer")

    windows: List[Tuple[date, date]] = []
    window_start = start_date
    while window_start <= end_date:
        window_end = min(window_start + timedelta(days=chunk_days - 1), end_date)
        windows.append((window_start, window_end))
        window_start = window_end + timedelta(days=1)
    return windows


def _to_entsoe_timestamp(value: date) -> str:
    """ENTSO-E expects UTC timestamps formatted as yyyyMMddHHmm."""
    as_datetime = datetime(value.year, value.month, value.day, tzinfo=dt_timezone.utc)
    return as_datetime.strftime("%Y%m%d%H%M")


def _build_params(request: EntsoeRequest) -> dict:
    dataset = request.dataset
    params = {
        "securityToken": None,  # populated by fetch_entsoe_document just before sending
        "documentType": dataset.document_type,
        "periodStart": _to_entsoe_timestamp(request.period_start),
        # ENTSO-E's periodEnd is exclusive; request through the start of the day after
        # period_end so the requested end date's data is fully included.
        "periodEnd": _to_entsoe_timestamp(request.period_end + timedelta(days=1)),
    }
    if dataset.process_type:
        params["processType"] = dataset.process_type

    if dataset.name == "load":
        params["outBiddingZone_Domain"] = request.domain
    elif dataset.name == "generation":
        params["in_Domain"] = request.domain
    elif dataset.name == "price":
        params["in_Domain"] = request.domain
        params["out_Domain"] = request.domain
    else:
        raise ValueError(f"Unsupported ENTSO-E dataset: {dataset.name}")

    return params


def _local_tag(element: ET.Element) -> str:
    tag = element.tag
    return tag.split("}", 1)[1] if "}" in tag else tag


def _acknowledgement_reason(root: ET.Element) -> Tuple[Optional[str], str]:
    """Extract (code, text) from an Acknowledgement_MarketDocument's Reason."""
    code: Optional[str] = None
    text = "unknown reason"
    for element in root.iter():
        local = _local_tag(element)
        if local == "code" and element.text:
            code = element.text.strip()
        elif local == "text" and element.text:
            text = element.text.strip()
    return code, text


def _is_no_data_reason(reason_text: str) -> bool:
    normalized = reason_text.strip().lower()
    return any(marker in normalized for marker in _NO_DATA_TEXT_MARKERS)


def _validate_response_xml(xml_text: str, request: EntsoeRequest) -> ET.Element:
    if not xml_text or not xml_text.strip():
        raise EntsoeAPIError(
            f"ENTSO-E response for {request.country_code}/{request.dataset.name} was empty."
        )

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise EntsoeAPIError(
            f"ENTSO-E response for {request.country_code}/{request.dataset.name} "
            f"could not be parsed as XML: {exc}"
        ) from exc

    root_tag = _local_tag(root)

    if root_tag == _ACKNOWLEDGEMENT_ROOT_TAG:
        code, reason_text = _acknowledgement_reason(root)
        if _is_no_data_reason(reason_text):
            raise EntsoeNoDataError(
                f"ENTSO-E reported no matching data for {request.country_code}/"
                f"{request.dataset.name} (reason code {code}): {reason_text}"
            )
        raise EntsoeAPIError(
            f"ENTSO-E API reported an error for {request.country_code}/"
            f"{request.dataset.name} (reason code {code}): {reason_text}"
        )

    if root_tag not in request.dataset.expected_root_tags:
        raise EntsoeAPIError(
            f"ENTSO-E response for {request.country_code}/{request.dataset.name} had "
            f"unexpected root element '{root_tag}'."
        )

    return root


def fetch_entsoe_document(
    request: EntsoeRequest,
    *,
    token: str,
    session: Optional[Any] = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> str:
    """Fetch one raw ENTSO-E XML document for a country/dataset/date window.

    Bounded retries apply to transient network/HTTP failures. Non-retryable
    HTTP errors, genuine ENTSO-E error-acknowledgement documents, and
    structurally unexpected responses raise `EntsoeAPIError` immediately.
    An acknowledgement whose reason is "no matching data found" raises
    the more specific `EntsoeNoDataError` (a subclass of `EntsoeAPIError`)
    instead — callers that need to tell a legitimately unavailable
    source apart from a real failure should catch that first.
    """
    http = session or requests
    params = _build_params(request)
    params["securityToken"] = token
    total_attempts = max_retries + 1

    logger.info(
        "Requesting ENTSO-E %s: country=%s domain=%s start=%s end=%s",
        request.dataset.name, request.country_code, request.domain,
        request.period_start, request.period_end,
    )

    last_error: Optional[Exception] = None
    for attempt in range(1, total_attempts + 1):
        try:
            response = http.get(ENTSOE_API_URL, params=params, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            last_error = exc
            logger.warning(
                "ENTSO-E request failed for %s/%s (attempt %s/%s): %s",
                request.country_code, request.dataset.name, attempt, total_attempts, exc,
            )
            if attempt < total_attempts:
                sleep_fn(retry_backoff_seconds)
                continue
            raise EntsoeAPIError(
                f"ENTSO-E request for {request.country_code}/{request.dataset.name} failed "
                f"after {total_attempts} attempts: {exc}"
            ) from exc

        if response.status_code == 200:
            _validate_response_xml(response.text, request)
            return response.text

        if response.status_code in _RETRYABLE_STATUS_CODES and attempt < total_attempts:
            logger.warning(
                "ENTSO-E request returned status %s for %s/%s (attempt %s/%s); retrying.",
                response.status_code, request.country_code, request.dataset.name,
                attempt, total_attempts,
            )
            sleep_fn(retry_backoff_seconds)
            continue

        # ENTSO-E also returns 400 with an Acknowledgement_MarketDocument body
        # describing the actual problem — surface that reason when present.
        try:
            _validate_response_xml(response.text, request)
        except EntsoeAPIError:
            raise
        raise EntsoeAPIError(
            f"ENTSO-E request for {request.country_code}/{request.dataset.name} failed with "
            f"status {response.status_code}: {response.text[:500]}"
        )

    raise EntsoeAPIError(
        f"ENTSO-E request for {request.country_code}/{request.dataset.name} failed after "
        f"{total_attempts} attempts: {last_error}"
    )
