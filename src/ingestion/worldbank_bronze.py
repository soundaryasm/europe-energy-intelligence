"""Transform raw World Bank indicator records into Bronze-layer rows.

Bronze rows are one row per (country, indicator, year) — already the
exact grain World Bank's own response records come in, so this is a
straight field-rename/flatten, no aggregation.
"""
from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
from typing import Any, List, Mapping, Optional, Tuple

SOURCE_SYSTEM = "worldbank"


def build_bronze_records(
    wb_records: List[Mapping[str, Any]],
    *,
    ingestion_timestamp: Optional[datetime] = None,
) -> List[dict]:
    """Flatten raw World Bank API records into deterministic Bronze rows.

    `value` is kept nullable — World Bank can list a record for a
    country/indicator/year with `value: null` (distinct from the record
    being entirely absent, which `worldbank_client.fetch_indicator`
    already reduces to an empty list). The business key returned by
    `business_key()` is stable across reruns of the same
    country/indicator/year, which is what allows a downstream Delta
    MERGE write to stay idempotent — fetching the same year repeatedly
    (by design, see the pipeline module) is expected, not wasteful.
    """
    ts = ingestion_timestamp or datetime.now(dt_timezone.utc)

    rows: List[dict] = []
    for record in wb_records:
        rows.append(
            {
                "country_code": record["country"]["id"],
                "country_name": record["country"]["value"],
                "indicator_code": record["indicator"]["id"],
                "indicator_name": record["indicator"]["value"],
                "year": int(record["date"]),
                "value": record.get("value"),
                "source_system": SOURCE_SYSTEM,
                "ingestion_timestamp": ts.isoformat(),
            }
        )
    return rows


def business_key(row: Mapping[str, Any]) -> Tuple[str, str, int]:
    """Deterministic identity for one logical Bronze observation."""
    return (row["country_code"], row["indicator_code"], row["year"])
