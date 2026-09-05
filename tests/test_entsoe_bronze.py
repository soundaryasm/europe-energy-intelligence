"""Tests for ENTSO-E Bronze record construction (Spec 002)."""
from datetime import datetime, timezone
from datetime import date

import pytest

from src.config.entsoe import EntsoeCountryDomain
from src.ingestion.entsoe_bronze import SOURCE_SYSTEM, build_bronze_records, business_key
from src.ingestion.entsoe_datasets import GENERATION, LOAD, PRICE
from src.ingestion.entsoe_xml import EntsoeXmlError, parse_time_series
from tests.fixtures_entsoe_xml import (
    GENERATION_WITH_PUMPED_STORAGE_CONSUMPTION_XML,
    GENERATION_XML,
    LOAD_XML,
)

IRELAND_DOMAIN = EntsoeCountryDomain(country_code="IE", domain="10Y1001A1001A59C", validated=False)

# Deliberately missing <businessType>, unlike every real fixture in
# fixtures_entsoe_xml.py — every ENTSO-E dataset this pipeline ingests has
# been confirmed (against real responses) to always carry one.
PRICE_XML_MISSING_BUSINESS_TYPE = """<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
  <mRID>doc-price-no-business-type</mRID>
  <TimeSeries>
    <currency_Unit.name>EUR</currency_Unit.name>
    <Period>
      <timeInterval><start>2024-01-01T00:00Z</start></timeInterval>
      <resolution>PT60M</resolution>
      <Point><position>1</position><price.amount>45.10</price.amount></Point>
    </Period>
  </TimeSeries>
</Publication_MarketDocument>
"""


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


def test_build_bronze_records_retains_business_type():
    parsed = parse_time_series(LOAD_XML, LOAD)
    rows = build_bronze_records(parsed, IRELAND_DOMAIN, LOAD, date(2024, 1, 1), date(2024, 1, 1))

    assert all(row["business_type"] == "A04" for row in rows)


def test_business_key_distinguishes_production_from_consumption_at_same_timestamp():
    # Real ENTSO-E case (pumped-storage hydro): same psrType, same
    # timestamp, different business_type — must not collide into one
    # business key, or the Delta MERGE would fail with
    # DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE.
    parsed = parse_time_series(GENERATION_WITH_PUMPED_STORAGE_CONSUMPTION_XML, GENERATION)
    rows = build_bronze_records(parsed, IRELAND_DOMAIN, GENERATION, date(2024, 1, 1), date(2024, 1, 1))

    assert len(rows) == 2
    keys = [business_key(r) for r in rows]
    assert len(keys) == len(set(keys))


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


def test_build_bronze_records_rejects_missing_business_type():
    # Real incident this guards against: an ALTER TABLE-added business_type
    # column defaults existing rows to NULL, and a NULL business_type Bronze
    # row silently duplicates instead of merging with a correctly-tagged one
    # (see docs/migrations/001_entsoe_bronze_add_business_type.sql). Refusing
    # to build a row with no businessType at all stops a bad ingestion run
    # from reintroducing that ambiguity in the first place.
    parsed = parse_time_series(PRICE_XML_MISSING_BUSINESS_TYPE, PRICE)

    with pytest.raises(EntsoeXmlError):
        build_bronze_records(parsed, IRELAND_DOMAIN, PRICE, date(2024, 1, 1), date(2024, 1, 1))
