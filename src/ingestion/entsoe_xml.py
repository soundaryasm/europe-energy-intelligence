"""Parse ENTSO-E XML TimeSeries documents into flat observation records (Spec 002).

Pure Python / stdlib `xml.etree.ElementTree` only — no PySpark dependency,
so this is fully unit-testable without Databricks.

IMPORTANT: the exact ENTSO-E XML element names implemented here follow
ENTSO-E's publicly documented schema (GL_MarketDocument for load/generation,
Publication_MarketDocument for day-ahead prices). This has NOT been
verified against a real ENTSO-E response in this environment (no API
token was available) and must be validated against a real payload before
this pipeline is trusted in production.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import List, Optional
from xml.etree import ElementTree as ET

from src.ingestion.entsoe_datasets import EntsoeDataset

_DURATION_PATTERN = re.compile(r"^P(?:(?P<days>\d+)D)?T?(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?$")


class EntsoeXmlError(ValueError):
    """Raised when an ENTSO-E XML document cannot be parsed as expected."""


def _local_tag(element: ET.Element) -> str:
    tag = element.tag
    return tag.split("}", 1)[1] if "}" in tag else tag


def _find_local(element: ET.Element, tag_name: str) -> Optional[ET.Element]:
    for child in element.iter():
        if _local_tag(child) == tag_name:
            return child
    return None


def _find_all_direct(element: ET.Element, tag_name: str) -> List[ET.Element]:
    return [child for child in element if _local_tag(child) == tag_name]


def parse_iso8601_duration_minutes(duration: str) -> int:
    """Parse a subset of ISO 8601 durations (e.g. 'PT60M', 'PT30M', 'PT15M') to minutes."""
    match = _DURATION_PATTERN.match(duration.strip()) if duration else None
    if not match:
        raise EntsoeXmlError(f"Unsupported ENTSO-E resolution duration: {duration!r}")

    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    total_minutes = days * 24 * 60 + hours * 60 + minutes
    if total_minutes <= 0:
        raise EntsoeXmlError(f"ENTSO-E resolution duration must be positive: {duration!r}")
    return total_minutes


def _parse_period_start(period: ET.Element) -> datetime:
    time_interval = _find_local(period, "timeInterval")
    if time_interval is None:
        raise EntsoeXmlError("TimeSeries <Period> is missing <timeInterval>.")
    start_element = _find_local(time_interval, "start")
    if start_element is None or not start_element.text:
        raise EntsoeXmlError("TimeSeries <timeInterval> is missing <start>.")

    raw = start_element.text.strip()
    # ENTSO-E timestamps are UTC, formatted like "2024-01-01T00:00Z".
    normalized = raw[:-1] if raw.endswith("Z") else raw
    try:
        return datetime.fromisoformat(normalized).replace(tzinfo=dt_timezone.utc)
    except ValueError as exc:
        raise EntsoeXmlError(f"Could not parse ENTSO-E period start timestamp: {raw!r}") from exc


def parse_time_series(xml_text: str, dataset: EntsoeDataset) -> List[dict]:
    """Flatten every <Point> across every <TimeSeries> into observation records.

    Each record has: resolution, source_timestamp (UTC datetime), value,
    unit, currency, production_type_raw, source_document_mrid.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise EntsoeXmlError(f"Could not parse ENTSO-E XML document: {exc}") from exc

    mrid_element = _find_local(root, "mRID")
    document_mrid = mrid_element.text.strip() if mrid_element is not None and mrid_element.text else None

    records: List[dict] = []

    for time_series in _find_all_direct(root, "TimeSeries"):
        production_type_raw = None
        psr_type_element = _find_local(time_series, "psrType")
        if psr_type_element is not None and psr_type_element.text:
            production_type_raw = psr_type_element.text.strip()

        unit_element = _find_local(time_series, "quantity_Measure_Unit.name")
        unit = unit_element.text.strip() if unit_element is not None and unit_element.text else None

        currency_element = _find_local(time_series, "currency_Unit.name")
        currency = (
            currency_element.text.strip() if currency_element is not None and currency_element.text else None
        )

        for period in _find_all_direct(time_series, "Period"):
            resolution_element = _find_local(period, "resolution")
            if resolution_element is None or not resolution_element.text:
                raise EntsoeXmlError("TimeSeries <Period> is missing <resolution>.")
            resolution = resolution_element.text.strip()
            resolution_minutes = parse_iso8601_duration_minutes(resolution)
            period_start = _parse_period_start(period)

            for point in _find_all_direct(period, "Point"):
                position_element = _find_local(point, "position")
                value_element = _find_local(point, dataset.value_tag)
                if position_element is None or position_element.text is None:
                    raise EntsoeXmlError("ENTSO-E <Point> is missing <position>.")
                if value_element is None or value_element.text is None:
                    raise EntsoeXmlError(
                        f"ENTSO-E <Point> is missing expected value element <{dataset.value_tag}>."
                    )

                position = int(position_element.text.strip())
                value = float(value_element.text.strip())
                observation_timestamp = period_start + timedelta(minutes=(position - 1) * resolution_minutes)

                records.append(
                    {
                        "resolution": resolution,
                        "resolution_minutes": resolution_minutes,
                        "source_timestamp": observation_timestamp,
                        "value": value,
                        "unit": unit,
                        "currency": currency,
                        "production_type_raw": production_type_raw,
                        "source_document_mrid": document_mrid,
                    }
                )

    return records
