"""ENTSO-E Bronze ingestion orchestration for the MVP countries (Spec 002).

This module MUST only be executed on Databricks. The PySpark/Delta write
path is imported lazily, inside the function body, exactly like the
Open-Meteo pipeline (`open_meteo_pipeline.py`), so orchestration and
business logic stay importable and unit-testable without PySpark.

XML parsing (`entsoe_xml.parse_time_series`) and Bronze row construction
(`entsoe_bronze.build_bronze_records`) are real business logic and are
exercised for real in this module and in its tests — only the HTTP layer
and the Spark write are treated as external systems to mock in tests.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Callable, Dict, List, Optional, Sequence

from src.config.countries import CountryConfig, load_countries
from src.config.entsoe import EntsoeCountryDomain, EntsoeConfigError, load_entsoe_domains
from src.ingestion.entsoe_bronze import build_bronze_records
from src.ingestion.entsoe_client import (
    DEFAULT_CHUNK_DAYS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
    EntsoeAPIError,
    EntsoeNoDataError,
    EntsoeRequest,
    chunk_date_range,
    fetch_entsoe_document,
)
from src.ingestion.entsoe_datasets import ALL_DATASETS, EntsoeDataset
from src.ingestion.entsoe_xml import parse_time_series

logger = logging.getLogger(__name__)

BRONZE_TABLE_NAME = "bronze_entsoe_energy"


@dataclass
class IngestionResult:
    """Observability summary for one ENTSO-E ingestion execution, at
    country+dataset granularity (Spec 002 "Observability").

    `unavailable` and `failed` are deliberately distinct (Spec 006
    "Partial Source Data" / Spec 007 "Source Availability"):
    `unavailable` means ENTSO-E acknowledged the request but reported no
    matching data for that country/dataset/period — a legitimate source
    condition, not a broken ingestion run. `failed` means a genuine
    technical/invalid-response failure. Only `failed` affects
    `all_succeeded`, so a run with legitimately unavailable sources does
    not get treated as a failed run.
    """

    started_at: str
    ended_at: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    countries_attempted: List[str] = field(default_factory=list)
    datasets_attempted: List[str] = field(default_factory=list)
    succeeded: List[str] = field(default_factory=list)     # "COUNTRY:dataset"
    unavailable: List[str] = field(default_factory=list)   # "COUNTRY:dataset"
    failed: List[str] = field(default_factory=list)        # "COUNTRY:dataset"
    records_written: int = 0
    errors: Dict[str, str] = field(default_factory=dict)

    @property
    def all_succeeded(self) -> bool:
        return not self.failed


def resolve_country_domains(
    countries: Sequence[CountryConfig], domains: Dict[str, EntsoeCountryDomain]
) -> List[EntsoeCountryDomain]:
    """Match configured MVP countries to their ENTSO-E domain entries.

    Raises `EntsoeConfigError` if any configured country is missing a
    domain entry — this is a configuration defect, not a runtime API
    failure, so it must not be silently skipped.
    """
    missing = [c.country_code for c in countries if c.country_code not in domains]
    if missing:
        raise EntsoeConfigError(f"No ENTSO-E domain configured for countries: {missing}")

    unvalidated = [code for code, entry in domains.items() if not entry.validated]
    if unvalidated:
        logger.warning(
            "ENTSO-E domain codes for %s are NOT validated against a live ENTSO-E "
            "account. Verify before relying on this data.",
            unvalidated,
        )

    return [domains[c.country_code] for c in countries]


def _default_spark_writer(spark, records: List[dict], table_name: str) -> int:
    """Write Bronze records to a Delta table, upserting on the business key.

    Only ever runs on Databricks: PySpark/Delta are imported here, inside
    the function body, rather than at module import time.
    """
    from delta.tables import DeltaTable

    if not records:
        return 0

    df = spark.createDataFrame(records)

    if spark.catalog.tableExists(table_name):
        target = DeltaTable.forName(spark, table_name)
        (
            target.alias("t")
            .merge(
                df.alias("s"),
                "t.country_code = s.country_code AND "
                "t.dataset_type = s.dataset_type AND "
                "t.source_timestamp = s.source_timestamp AND "
                "coalesce(t.production_type_raw, '') = coalesce(s.production_type_raw, '')",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    return df.count()


def run_ingestion(
    start_date: date,
    end_date: date,
    *,
    token: str,
    spark=None,
    countries: Optional[List[CountryConfig]] = None,
    domains: Optional[Dict[str, EntsoeCountryDomain]] = None,
    datasets: Sequence[EntsoeDataset] = ALL_DATASETS,
    table_name: str = BRONZE_TABLE_NAME,
    fetch_fn: Callable[..., str] = fetch_entsoe_document,
    spark_writer: Callable[[object, List[dict], str], int] = _default_spark_writer,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
) -> IngestionResult:
    """Ingest ENTSO-E Bronze data for every configured country and dataset.

    Each (country, dataset) pair is processed independently: one failure
    is recorded and logged but does not stop the others (Spec 002
    "A failed country/dataset request must not be silently ignored.").
    Large date ranges are split into bounded chunks per `chunk_days`.

    A per-window `EntsoeNoDataError` (ENTSO-E acknowledging "no matching
    data" for that window) does not abort the (country, dataset) attempt
    — later windows are still requested. If every window comes back with
    no data, the pair is classified `unavailable`, not `failed`, and no
    rows are written for it (never a synthesized zero — Spec 002
    "Do not manufacture observations for missing intervals"). Any other
    exception aborts the attempt and is classified `failed`.
    """
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    from datetime import datetime, timezone as dt_timezone

    result = IngestionResult(
        started_at=datetime.now(dt_timezone.utc).isoformat(),
        start_date=start_date,
        end_date=end_date,
    )

    resolved_countries = countries if countries is not None else load_countries()
    resolved_domains = domains if domains is not None else load_entsoe_domains()
    country_domains = resolve_country_domains(resolved_countries, resolved_domains)
    windows = chunk_date_range(start_date, end_date, chunk_days=chunk_days)

    all_records: List[dict] = []

    for country_domain in country_domains:
        result.countries_attempted.append(country_domain.country_code)

        for dataset in datasets:
            if dataset.name not in result.datasets_attempted:
                result.datasets_attempted.append(dataset.name)

            attempt_key = f"{country_domain.country_code}:{dataset.name}"
            try:
                dataset_records: List[dict] = []
                saw_no_data = False
                for window_start, window_end in windows:
                    try:
                        xml_text = fetch_fn(
                            EntsoeRequest(
                                country_code=country_domain.country_code,
                                domain=country_domain.domain,
                                dataset=dataset,
                                period_start=window_start,
                                period_end=window_end,
                            ),
                            token=token,
                            timeout=timeout,
                            max_retries=max_retries,
                        )
                    except EntsoeNoDataError as exc:
                        saw_no_data = True
                        logger.info(
                            "ENTSO-E reported no data for %s (%s to %s): %s",
                            attempt_key, window_start, window_end, exc,
                        )
                        continue

                    parsed_points = parse_time_series(xml_text, dataset)
                    dataset_records.extend(
                        build_bronze_records(
                            parsed_points, country_domain, dataset, window_start, window_end
                        )
                    )

                if dataset_records:
                    all_records.extend(dataset_records)
                    result.succeeded.append(attempt_key)
                elif saw_no_data:
                    result.unavailable.append(attempt_key)
                else:
                    # No window produced records, and none explicitly said
                    # "no data" either — still a failure, not a silent
                    # empty success (Spec 002 "API Behaviour").
                    raise EntsoeAPIError(
                        f"ENTSO-E returned no records for {country_domain.country_code}/"
                        f"{dataset.name} between {start_date} and {end_date}."
                    )
            except Exception as exc:  # noqa: BLE001 - a per-pair failure must stay visible
                logger.error("ENTSO-E ingestion failed for %s: %s", attempt_key, exc)
                result.failed.append(attempt_key)
                result.errors[attempt_key] = str(exc)

    if all_records:
        result.records_written = spark_writer(spark, all_records, table_name)

    result.ended_at = datetime.now(dt_timezone.utc).isoformat()
    return result
