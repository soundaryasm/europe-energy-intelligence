"""Transform parsed ENTSO-E TimeSeries records into Bronze-layer rows (Spec 002).

Bronze rows are "tall": one row per source observation, preserving
source-level detail (resolution, unit, currency, production type, source
document id) with minimal transformation, per Spec 002's Bronze Storage
requirements.
"""
from __future__ import annotations

from datetime import date, datetime, timezone as dt_timezone
from typing import List, Optional, Tuple

# ENTSO-E's <businessType> code for consumption. Real for pumped-storage
# hydro reported inside the generation-per-type (A75) document: the same
# psrType (e.g. B10/B11/B12) can carry BOTH a Production (A01) and a
# Consumption (A04) TimeSeries for the same timestamps. Silver excludes
# this from generation totals (Spec 003 "Actual Generation by Production
# Type" is about generation, not consumption).
CONSUMPTION_BUSINESS_TYPE = "A04"

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
                "business_type": point.get("business_type"),
                "currency": point.get("currency"),
                "source_document_mrid": point.get("source_document_mrid"),
                "requested_start_date": requested_start_date.isoformat(),
                "requested_end_date": requested_end_date.isoformat(),
                "source_system": SOURCE_SYSTEM,
                "ingestion_timestamp": ts.isoformat(),
            }
        )

    return rows


def business_key(row: dict) -> Tuple[str, str, str, Optional[str], Optional[str]]:
    """Deterministic identity for one logical Bronze observation.

    Production type is part of the key so generation records for the same
    country/timestamp but different production types don't collide
    (Spec 007's documented ENTSO-E Generation business key). Business
    type is also part of the key so a Production (A01) and a Consumption
    (A04) observation for the same psrType/timestamp — real for
    pumped-storage hydro in the A75 document — remain two distinct
    records instead of colliding into one.
    """
    return (
        row["country_code"],
        row["dataset_type"],
        row["source_timestamp"],
        row.get("production_type_raw"),
        row.get("business_type"),
    )
