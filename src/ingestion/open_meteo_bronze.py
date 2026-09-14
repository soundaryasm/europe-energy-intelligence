"""Transform raw Open-Meteo daily responses into Bronze-layer records (Spec 001).

Bronze rows are intentionally "tall": one row per (variable, date),
preserving source-level detail with minimal transformation, per Spec
001's Bronze Storage requirements. For one country/date this produces
exactly `len(DAILY_VARIABLES)` rows (3 in the MVP) — daily aggregation
across countries/dates happens later in Silver (Spec 003), not here.
"""
from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
from typing import Any, List, Mapping, Optional, Tuple

from src.config.countries import CountryConfig
from src.ingestion.open_meteo_client import DAILY_VARIABLES, SOURCE_ENDPOINT

SOURCE_SYSTEM = "open-meteo"


def build_bronze_records(
    payload: Mapping[str, Any],
    country: CountryConfig,
    *,
    source_endpoint: str = SOURCE_ENDPOINT,
    ingestion_timestamp: Optional[datetime] = None,
) -> List[dict]:
    """Flatten one Open-Meteo daily response into deterministic Bronze rows.

    Retains both configured reference coordinates/timezone and the
    coordinates/timezone/UTC offset actually returned by Open-Meteo
    (which may resolve to a nearby grid cell — Spec 001 "Requested vs
    Returned Coordinates"), plus the source unit for each variable, so
    none of that is lost by flattening. The business key returned by
    `business_key()` is stable across reruns of the same country/date
    range, which is what allows a downstream Delta MERGE write to stay
    idempotent.

    `source_endpoint` records which Open-Meteo API actually produced this
    row (`SOURCE_ENDPOINT` for the Historical/Archive API used for
    backfill, `SOURCE_ENDPOINT_FORECAST` for the Forecast API used for
    recent/daily dates) — the two can return a slightly different value
    for the same date, since Forecast's operational-model estimate for a
    recent day is provisional in a different sense than Archive's
    ERA5-reanalysis value is. Recording provenance lets that be traced
    later rather than silently blended.
    """
    ts = ingestion_timestamp or datetime.now(dt_timezone.utc)

    daily_payload = payload["daily"]
    timestamps = daily_payload["time"]
    daily_units = payload.get("daily_units", {})

    returned_latitude = payload.get("latitude")
    returned_longitude = payload.get("longitude")
    returned_timezone = payload.get("timezone")
    utc_offset_seconds = payload.get("utc_offset_seconds")

    rows: List[dict] = []

    for variable in DAILY_VARIABLES:
        values = daily_payload[variable]
        if len(values) != len(timestamps):
            raise ValueError(
                f"Open-Meteo 'daily.{variable}' length ({len(values)}) does not match "
                f"'daily.time' length ({len(timestamps)}) for country {country.country_code}."
            )
        for observation_date, source_value in zip(timestamps, values):
            rows.append(
                {
                    "country_code": country.country_code,
                    "country_name": country.country_name,
                    "reference_location": country.reference_location,
                    "latitude": country.latitude,
                    "longitude": country.longitude,
                    "timezone": country.timezone,
                    "returned_latitude": returned_latitude,
                    "returned_longitude": returned_longitude,
                    "returned_timezone": returned_timezone,
                    "utc_offset_seconds": utc_offset_seconds,
                    "observation_date": observation_date,
                    "source_variable": variable,
                    "source_value": source_value,
                    "source_unit": daily_units.get(variable),
                    "source_endpoint": source_endpoint,
                    "source_system": SOURCE_SYSTEM,
                    "ingestion_timestamp": ts.isoformat(),
                }
            )
    return rows


def business_key(row: Mapping[str, Any]) -> Tuple[str, str, str]:
    """Deterministic identity for one logical Bronze observation."""
    return (row["country_code"], row["source_variable"], row["observation_date"])
