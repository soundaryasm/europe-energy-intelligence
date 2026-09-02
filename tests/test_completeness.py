"""Tests for source completeness classification (Spec 007)."""
from src.ingestion.entsoe_pipeline import IngestionResult as EntsoeIngestionResult
from src.ingestion.open_meteo_pipeline import IngestionResult as OpenMeteoIngestionResult
from src.quality.completeness import (
    CompletenessStatus,
    classify_entsoe_completeness,
    classify_open_meteo_completeness,
    summarize,
)


def test_open_meteo_completeness_marks_succeeded_countries_complete():
    result = OpenMeteoIngestionResult(
        started_at="t0",
        countries_attempted=["IE", "DE"],
        countries_succeeded=["IE", "DE"],
    )

    classification = classify_open_meteo_completeness(result)

    assert classification["IE"].status == CompletenessStatus.COMPLETE
    assert classification["DE"].status == CompletenessStatus.COMPLETE


def test_open_meteo_completeness_marks_failed_countries_failed_with_details():
    result = OpenMeteoIngestionResult(
        started_at="t0",
        countries_attempted=["IE", "DE"],
        countries_succeeded=["DE"],
        countries_failed=["IE"],
        errors={"IE": "timeout"},
    )

    classification = classify_open_meteo_completeness(result)

    assert classification["IE"].status == CompletenessStatus.FAILED
    assert classification["IE"].details == "timeout"
    assert classification["DE"].status == CompletenessStatus.COMPLETE


def test_entsoe_completeness_complete_only_when_every_required_dataset_succeeds():
    result = EntsoeIngestionResult(
        started_at="t0",
        countries_attempted=["IE"],
        succeeded=["IE:load", "IE:generation", "IE:price"],
    )

    classification = classify_entsoe_completeness(result, required_datasets=["load", "generation", "price"])

    assert classification["IE"].status == CompletenessStatus.COMPLETE


def test_entsoe_completeness_partially_available_when_some_datasets_fail():
    result = EntsoeIngestionResult(
        started_at="t0",
        countries_attempted=["IE"],
        succeeded=["IE:load", "IE:generation"],
        failed=["IE:price"],
    )

    classification = classify_entsoe_completeness(result, required_datasets=["load", "generation", "price"])

    assert classification["IE"].status == CompletenessStatus.PARTIALLY_AVAILABLE
    assert "price" in classification["IE"].details


def test_entsoe_completeness_failed_when_no_required_dataset_succeeds():
    result = EntsoeIngestionResult(
        started_at="t0",
        countries_attempted=["IE"],
        failed=["IE:load", "IE:generation", "IE:price"],
    )

    classification = classify_entsoe_completeness(result, required_datasets=["load", "generation", "price"])

    assert classification["IE"].status == CompletenessStatus.FAILED


def test_summarize_groups_country_codes_by_status():
    result = OpenMeteoIngestionResult(
        started_at="t0",
        countries_attempted=["IE", "DE"],
        countries_succeeded=["IE"],
        countries_failed=["DE"],
    )
    classification = classify_open_meteo_completeness(result)

    summary = summarize(classification)

    assert summary["complete"] == ["IE"]
    assert summary["failed"] == ["DE"]
    assert summary["partially_available"] == []
    assert summary["unavailable"] == []
