"""Transform parsed ENTSO-E TimeSeries records into Bronze-layer rows (Spec 002).

Bronze rows are "tall": one row per source observation, preserving
source-level detail (resolution, unit, currency, production type, source
document id) with minimal transformation, per Spec 002's Bronze Storage
requirements.
"""
from __future__ import annotations

from datetime import date, datetime, timezone as dt_timezone
from typing import List, Optional, Tuple

from src.config.entsoe import EntsoeCountryDomain
from src.ingestion.entsoe_datasets import EntsoeDataset

SOURCE_SYSTEM = "entsoe"


def build_bronze_records(
    parsed_points: List[dict],
    country_domain: EntsoeCountryDomain,
    dataset: EntsoeDataset,
    requested_start_date: date,
    requested_end_date: date,
    *,
    ingestion_timestamp: Optional[datetime] = None,
) -> List[dict]:
    """Flatten parsed ENTSO-E points into deterministic Bronze rows.

    The business key returned by `business_key()` is stable across reruns
    of the same country/dataset/date window, which is what allows a
    downstream Delta MERGE write to stay idempotent even when ENTSO-E
    later revises a previously published value.
    """
    ts = ingestion_timestamp or datetime.now(dt_timezone.utc)
    rows: List[dict] = []

    for point in parsed_points:
        rows.append(
            {
                "country_code": country_domain.country_code,
                "domain": country_domain.domain,
                "dataset_type": dataset.name,
                # Always UTC (ENTSO-E's API is UTC-only). Stored without an
                # offset suffix so downstream Spark parsing is unambiguous.
                "source_timestamp": point["source_timestamp"].strftime("%Y-%m-%dT%H:%M:%S"),
                "source_resolution": point["resolution"],
                "value": point["value"],
                "unit": point.get("unit"),
                "production_type_raw": point.get("production_type_raw"),
                "currency": point.get("currency"),
                "source_document_mrid": point.get("source_document_mrid"),
                "requested_start_date": requested_start_date.isoformat(),
                "requested_end_date": requested_end_date.isoformat(),
                "source_system": SOURCE_SYSTEM,
                "ingestion_timestamp": ts.isoformat(),
            }
        )

    return rows


def business_key(row: dict) -> Tuple[str, str, str, Optional[str]]:
    """Deterministic identity for one logical Bronze observation.

    Production type is part of the key so generation records for the same
    country/timestamp but different production types don't collide
    (Spec 007's documented ENTSO-E Generation business key).
    """
    return (
        row["country_code"],
        row["dataset_type"],
        row["source_timestamp"],
        row.get("production_type_raw"),
    )
