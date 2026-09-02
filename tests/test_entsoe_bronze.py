"""Tests for ENTSO-E Bronze record construction (Spec 002)."""
from datetime import datetime, timezone
from datetime import date

from src.config.entsoe import EntsoeCountryDomain
from src.ingestion.entsoe_bronze import SOURCE_SYSTEM, build_bronze_records, business_key
from src.ingestion.entsoe_datasets import GENERATION, LOAD
from src.ingestion.entsoe_xml import parse_time_series
from tests.fixtures_entsoe_xml import GENERATION_XML, LOAD_XML

IRELAND_DOMAIN = EntsoeCountryDomain(country_code="IE", domain="10Y1001A1001A59C", validated=False)


def test_build_bronze_records_preserves_source_metadata():
    parsed = parse_time_series(LOAD_XML, LOAD)
    rows = build_bronze_records(
        parsed, IRELAND_DOMAIN, LOAD, date(2024, 1, 1), date(2024, 1, 1),
        ingestion_timestamp=datetime(2024, 1, 5, tzinfo=timezone.utc),
    )

    assert len(rows) == 2
    for row in rows:
        assert row["country_code"] == "IE"
        assert row["domain"] == "10Y1001A1001A59C"
        assert row["dataset_type"] == "load"
        assert row["source_system"] == SOURCE_SYSTEM
        assert row["unit"] == "MAW"
        assert row["requested_start_date"] == "2024-01-01"
        assert row["ingestion_timestamp"] == "2024-01-05T00:00:00+00:00"


def test_build_bronze_records_retains_production_type_for_generation():
    parsed = parse_time_series(GENERATION_XML, GENERATION)
    rows = build_bronze_records(parsed, IRELAND_DOMAIN, GENERATION, date(2024, 1, 1), date(2024, 1, 1))

    production_types = {row["production_type_raw"] for row in rows}
    assert production_types == {"B19", "B16"}


def test_business_key_distinguishes_production_types_at_same_timestamp():
    parsed = parse_time_series(GENERATION_XML, GENERATION)
    rows = build_bronze_records(parsed, IRELAND_DOMAIN, GENERATION, date(2024, 1, 1), date(2024, 1, 1))

    keys = [business_key(r) for r in rows]
    assert len(keys) == len(set(keys))  # wind (B19) and solar (B16) at same timestamp differ


def test_business_key_is_stable_across_reruns_with_different_ingestion_times():
    parsed = parse_time_series(LOAD_XML, LOAD)
    first = build_bronze_records(
        parsed, IRELAND_DOMAIN, LOAD, date(2024, 1, 1), date(2024, 1, 1),
        ingestion_timestamp=datetime(2024, 1, 5, tzinfo=timezone.utc),
    )
    second = build_bronze_records(
        parsed, IRELAND_DOMAIN, LOAD, date(2024, 1, 1), date(2024, 1, 1),
        ingestion_timestamp=datetime(2024, 1, 6, tzinfo=timezone.utc),
    )

    assert sorted(business_key(r) for r in first) == sorted(business_key(r) for r in second)
