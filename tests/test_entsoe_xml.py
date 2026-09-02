"""Tests for ENTSO-E XML parsing (Spec 002).

Exercises the real parsing logic against synthetic-but-schema-shaped
fixtures (see `fixtures_entsoe_xml.py`) — no mocking of the parsing logic
itself.
"""
from datetime import datetime, timezone

import pytest

from src.ingestion.entsoe_datasets import GENERATION, LOAD, PRICE
from src.ingestion.entsoe_xml import (
    EntsoeXmlError,
    parse_iso8601_duration_minutes,
    parse_time_series,
)
from tests.fixtures_entsoe_xml import (
    ACKNOWLEDGEMENT_ERROR_XML,
    GENERATION_XML,
    LOAD_XML,
    PRICE_XML,
)


@pytest.mark.parametrize(
    "duration, expected_minutes",
    [("PT60M", 60), ("PT30M", 30), ("PT15M", 15), ("PT1H", 60), ("P1D", 1440)],
)
def test_parse_iso8601_duration_minutes(duration, expected_minutes):
    assert parse_iso8601_duration_minutes(duration) == expected_minutes


def test_parse_iso8601_duration_minutes_rejects_unsupported_format():
    with pytest.raises(EntsoeXmlError):
        parse_iso8601_duration_minutes("not-a-duration")


def test_parse_time_series_load_extracts_points_and_unit():
    records = parse_time_series(LOAD_XML, LOAD)

    assert len(records) == 2
    assert records[0]["value"] == 3500.5
    assert records[0]["unit"] == "MAW"
    assert records[0]["resolution"] == "PT60M"
    assert records[0]["source_document_mrid"] == "doc-load-123"
    assert records[0]["production_type_raw"] is None


def test_parse_time_series_load_computes_timestamps_from_position_and_resolution():
    records = parse_time_series(LOAD_XML, LOAD)

    assert records[0]["source_timestamp"] == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert records[1]["source_timestamp"] == datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)


def test_parse_time_series_generation_extracts_production_type_per_series():
    records = parse_time_series(GENERATION_XML, GENERATION)

    production_types = {r["production_type_raw"] for r in records}
    assert production_types == {"B19", "B16"}
    assert all(r["unit"] == "MAW" for r in records)


def test_parse_time_series_price_extracts_currency_and_negative_prices():
    records = parse_time_series(PRICE_XML, PRICE)

    assert len(records) == 2
    assert all(r["currency"] == "EUR" for r in records)
    values = {r["value"] for r in records}
    assert -5.32 in values  # negative prices must survive parsing
    assert 45.10 in values


def test_parse_time_series_raises_on_missing_expected_value_tag():
    # A load document has no <price.amount>, so parsing it as a price
    # dataset must fail loudly rather than silently return nothing useful.
    with pytest.raises(EntsoeXmlError):
        parse_time_series(LOAD_XML, PRICE)


def test_parse_time_series_raises_on_malformed_xml():
    with pytest.raises(EntsoeXmlError):
        parse_time_series("<not><valid", LOAD)
