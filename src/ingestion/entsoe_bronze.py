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
from src.ingestion.entsoe_xml import EntsoeXmlError

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

    Every ENTSO-E dataset this pipeline ingests (load, generation, price)
    has been confirmed — against real responses, not just documentation —
    to always carry a `<businessType>` code (see `tests/fixtures_entsoe_xml.py`
    and `tmp/entsoe.md`). A missing one is therefore treated as a genuine
    ingestion defect and rejected here, rather than silently persisted as
    a `NULL` that later collides with a real value under the same merge
    key (see `docs/migrations/001_entsoe_bronze_add_business_type.sql` for
    the incident this guards against).
    """
    ts = ingestion_timestamp or datetime.now(dt_timezone.utc)
    rows: List[dict] = []

    for point in parsed_points:
        business_type = point.get("business_type")
        if business_type is None:
            raise EntsoeXmlError(
                f"ENTSO-E point for {country_domain.country_code}/{dataset.name} at "
                f"{point['source_timestamp']} has no businessType. Refusing to persist "
                "a Bronze row with a missing business key component."
            )

        rows.append(
            {
                "country_code": country_domain.country_code,
                "domain": country_domain.domain,
                "dataset_type": dataset.name,
                # Always UTC (ENTSO-E's API is UTC-only). The trailing "Z"
                # is kept (not stripped) so Spark's timestamp parser reads
                # this as an absolute instant regardless of the session's
                # `spark.sql.session.timeZone` — a naive (no-zone) string
                # would instead be silently interpreted as being in
                # whatever that session default happens to be.
                "source_timestamp": point["source_timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source_resolution": point["resolution"],
                "value": point["value"],
                "unit": point.get("unit"),
                "production_type_raw": point.get("production_type_raw"),
                "business_type": business_type,
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
