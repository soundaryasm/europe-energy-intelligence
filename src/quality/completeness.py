"""Source completeness classification (Spec 007).

Turns raw Bronze ingestion results (the `IngestionResult` objects
returned by `src/ingestion/open_meteo_pipeline.run_ingestion` and
`src/ingestion/entsoe_pipeline.run_ingestion`) into an explicit
per-country classification, so a country whose ingestion silently
returned nothing is never conflated with one that actually succeeded.

Spec 007: "Instead distinguish: complete, partially available,
unavailable, failed ingestion. A missing country must be visible in
execution results."

Note: this module is about *visibility*, not pipeline gating — a failed
critical dbt test already blocks `publish_postgres` structurally, via
the Databricks job's task dependencies (Spec 006's
resources/daily_pipeline.yml), so no separate Python gating mechanism is
implemented for that.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable


class CompletenessStatus(str, Enum):
    COMPLETE = "complete"
    PARTIALLY_AVAILABLE = "partially_available"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


@dataclass(frozen=True)
class CountryCompleteness:
    country_code: str
    status: CompletenessStatus
    details: str = ""


def classify_open_meteo_completeness(result) -> Dict[str, CountryCompleteness]:
    """Classify each attempted country from an Open-Meteo `IngestionResult`."""
    classification: Dict[str, CountryCompleteness] = {}
    for country in result.countries_attempted:
        if country in result.countries_succeeded:
            classification[country] = CountryCompleteness(country, CompletenessStatus.COMPLETE)
        else:
            classification[country] = CountryCompleteness(
                country, CompletenessStatus.FAILED, result.errors.get(country, "")
            )
    return classification


def classify_entsoe_completeness(
    result, required_datasets: Iterable[str]
) -> Dict[str, CountryCompleteness]:
    """Classify each attempted country from an ENTSO-E `IngestionResult`.

    COMPLETE only if every dataset in `required_datasets` succeeded for
    that country. PARTIALLY_AVAILABLE if some (not all) succeeded,
    regardless of whether the rest are `failed` or `unavailable` — some
    real data exists either way. If none succeeded: FAILED if at least
    one required dataset genuinely failed (a technical failure is more
    severe than mere unavailability); otherwise UNAVAILABLE (every
    missing dataset was a legitimate ENTSO-E "no matching data" response,
    not a broken request — Spec 006 "Partial Source Data" / Spec 007
    "Source Availability").
    """
    required = set(required_datasets)
    classification: Dict[str, CountryCompleteness] = {}

    def _datasets(attempt_keys, country: str) -> set:
        return {
            key.split(":", 1)[1] for key in attempt_keys if key.startswith(f"{country}:")
        } & required

    for country in result.countries_attempted:
        succeeded_datasets = _datasets(result.succeeded, country)
        unavailable_datasets = _datasets(result.unavailable, country)
        failed_datasets = _datasets(result.failed, country)

        if succeeded_datasets == required:
            classification[country] = CountryCompleteness(country, CompletenessStatus.COMPLETE)
        elif succeeded_datasets:
            missing = sorted(failed_datasets | unavailable_datasets)
            classification[country] = CountryCompleteness(
                country, CompletenessStatus.PARTIALLY_AVAILABLE, f"missing datasets: {missing}"
            )
        elif failed_datasets:
            missing = sorted(failed_datasets | unavailable_datasets)
            classification[country] = CountryCompleteness(
                country, CompletenessStatus.FAILED, f"missing datasets: {missing}"
            )
        else:
            classification[country] = CountryCompleteness(
                country,
                CompletenessStatus.UNAVAILABLE,
                f"unavailable datasets: {sorted(unavailable_datasets)}",
            )

    return classification


def summarize(classification: Dict[str, CountryCompleteness]) -> Dict[str, list]:
    """Group country codes by status, for compact logging/observability."""
    summary: Dict[str, list] = {status.value: [] for status in CompletenessStatus}
    for country_code, entry in classification.items():
        summary[entry.status.value].append(country_code)
    return summary
