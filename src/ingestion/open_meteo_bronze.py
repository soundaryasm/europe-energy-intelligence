"""Transform raw Open-Meteo responses into Bronze-layer records (Spec 001).

Bronze rows are intentionally "tall": one row per (variable, timestamp),
preserving source-level detail with minimal transformation, per Spec 001's
Bronze Storage requirements. Daily aggregation into a single row per
country/date happens later in Silver (Spec 003), not here.
"""
from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
from typing import Any, Iterable, List, Mapping, Optional, Tuple

from src.config.countries import CountryConfig
from src.ingestion.open_meteo_client import DAILY_VARIABLES, HOURLY_VARIABLES

SOURCE_SYSTEM = "open-meteo"


def _rows_for_section(
    payload: Mapping[str, Any],
    section: str,
    variables: Iterable[str],
    country: CountryConfig,
    ingestion_timestamp: datetime,
) -> List[dict]:
    section_payload = payload[section]
    timestamps = section_payload["time"]
    rows: List[dict] = []

    for variable in variables:
        values = section_payload[variable]
        if len(values) != len(timestamps):
            raise ValueError(
                f"Open-Meteo '{section}.{variable}' length ({len(values)}) does not match "
                f"'{section}.time' length ({len(timestamps)}) for country {country.country_code}."
            )
        for observation_timestamp, source_value in zip(timestamps, values):
            rows.append(
                {
                    "country_code": country.country_code,
                    "country_name": country.country_name,
                    "reference_location": country.reference_location,
                    "latitude": country.latitude,
                    "longitude": country.longitude,
                    "timezone": country.timezone,
                    "observation_timestamp": observation_timestamp,
                    "source_variable": variable,
                    "source_value": source_value,
                    "source_resolution": section,
                    "source_system": SOURCE_SYSTEM,
                    "ingestion_timestamp": ingestion_timestamp.isoformat(),
                }
            )
    return rows


def build_bronze_records(
    payload: Mapping[str, Any],
    country: CountryConfig,
    *,
    ingestion_timestamp: Optional[datetime] = None,
) -> List[dict]:
    """Flatten one Open-Meteo response into deterministic Bronze rows.

    The business key returned by `business_key()` is stable across reruns
    of the same country/date range, which is what allows a downstream
    Delta MERGE write to stay idempotent.
    """
    ts = ingestion_timestamp or datetime.now(dt_timezone.utc)

    rows = _rows_for_section(payload, "hourly", HOURLY_VARIABLES, country, ts)
    rows += _rows_for_section(payload, "daily", DAILY_VARIABLES, country, ts)
    return rows


def business_key(row: Mapping[str, Any]) -> Tuple[str, str, str]:
    """Deterministic identity for one logical Bronze observation."""
    return (row["country_code"], row["source_variable"], row["observation_timestamp"])
