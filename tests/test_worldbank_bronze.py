"""Tests for World Bank Bronze record construction."""
from datetime import datetime, timezone

from src.ingestion.worldbank_bronze import build_bronze_records, business_key

SAMPLE_WB_RECORDS = [
    {
        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
        "country": {"id": "IE", "value": "Ireland"},
        "countryiso3code": "IRL",
        "date": "2023",
        "value": 5311538,
        "unit": "", "obs_status": "", "decimal": 0,
    },
    {
        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
        "country": {"id": "DE", "value": "Germany"},
        "countryiso3code": "DEU",
        "date": "2023",
        "value": 83287273,
        "unit": "", "obs_status": "", "decimal": 0,
    },
]


def test_build_bronze_records_flattens_expected_fields():
    rows = build_bronze_records(
        SAMPLE_WB_RECORDS, ingestion_timestamp=datetime(2026, 9, 8, tzinfo=timezone.utc)
    )

    assert len(rows) == 2
    ie_row = next(r for r in rows if r["country_code"] == "IE")
    assert ie_row["country_name"] == "Ireland"
    assert ie_row["indicator_code"] == "SP.POP.TOTL"
    assert ie_row["indicator_name"] == "Population, total"
    assert ie_row["year"] == 2023
    assert isinstance(ie_row["year"], int)
    assert ie_row["value"] == 5311538
    assert ie_row["source_system"] == "worldbank"
    assert ie_row["ingestion_timestamp"] == "2026-09-08T00:00:00+00:00"


def test_build_bronze_records_keeps_null_value_as_none():
    record = dict(SAMPLE_WB_RECORDS[0])
    record["value"] = None
    rows = build_bronze_records([record])

    assert rows[0]["value"] is None


def test_build_bronze_records_returns_empty_list_for_no_records():
    assert build_bronze_records([]) == []


def test_business_key_distinguishes_country_indicator_and_year():
    rows = build_bronze_records(SAMPLE_WB_RECORDS)
    keys = [business_key(r) for r in rows]

    assert len(keys) == len(set(keys))
    assert keys[0] == ("IE", "SP.POP.TOTL", 2023) or keys[1] == ("IE", "SP.POP.TOTL", 2023)
