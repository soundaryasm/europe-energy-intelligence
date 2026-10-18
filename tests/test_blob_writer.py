"""Tests for the historical Parquet writer (Spec 010).

Parquet round-trips are exercised for real — the file format is the
contract with the Bronze load, not an external system to mock. Only the
blob store is a double.
"""
from datetime import date

import pytest

from src.blob import writer
from src.blob.store import BlobStore
from tests.test_blob_store import FakeS3


def _store(objects=None):
    client = FakeS3(objects)
    return BlobStore(
        token="tok",
        credentials_fn=lambda _t: {
            "endpoint": "https://example.invalid", "bucket": "b",
            "accessKeyId": "ak", "secretAccessKey": "sk",
            "sessionToken": "st", "expiresAt": 10**12,
        },
        client_factory=lambda _c: client,
    )


def _weather_row(country, day, variable="temperature_2m_mean", value=1.0):
    return {
        "country_code": country,
        "country_name": "Name",
        "reference_location": "City",
        "latitude": 1.0,
        "longitude": 2.0,
        "timezone": "UTC",
        "returned_latitude": 1.0,
        "returned_longitude": 2.0,
        "returned_timezone": "UTC",
        "utc_offset_seconds": 0,
        "observation_date": day,
        "source_variable": variable,
        "source_value": value,
        "source_unit": "C",
        "source_endpoint": "archive_daily",
        "source_system": "open-meteo",
        "ingestion_timestamp": "2026-01-01T00:00:00+00:00",
    }


def test_arrow_schema_matches_the_bronze_column_set_and_order():
    from src.ingestion.delta_schema import OPEN_METEO_BRONZE_FIELDS

    schema = writer.arrow_schema(writer.SOURCE_OPEN_METEO)

    assert schema.names == [name for name, _, _ in OPEN_METEO_BRONZE_FIELDS]


def test_arrow_schema_preserves_bronze_nullability():
    schema = writer.arrow_schema(writer.SOURCE_OPEN_METEO)

    assert schema.field("country_code").nullable is False
    assert schema.field("source_value").nullable is True


def test_utc_offset_is_64_bit_to_match_the_bronze_table():
    import pyarrow as pa

    schema = writer.arrow_schema(writer.SOURCE_OPEN_METEO)

    assert schema.field("utc_offset_seconds").type == pa.int64()


def test_unknown_source_is_rejected_rather_than_guessed():
    with pytest.raises(writer.UnknownSourceError):
        writer.arrow_schema("not_a_source")


def test_parquet_round_trips_records_unchanged():
    rows = [_weather_row("IE", "2024-03-01"), _weather_row("DE", "2024-03-02")]

    restored = writer.from_parquet_bytes(
        writer.SOURCE_OPEN_METEO,
        writer.to_parquet_bytes(writer.SOURCE_OPEN_METEO, rows),
    )

    assert restored == rows


def test_missing_optional_columns_are_written_as_null_not_dropped():
    row = _weather_row("IE", "2024-03-01")
    del row["source_unit"]

    restored = writer.from_parquet_bytes(
        writer.SOURCE_OPEN_METEO,
        writer.to_parquet_bytes(writer.SOURCE_OPEN_METEO, [row]),
    )

    assert restored[0]["source_unit"] is None
    assert len(restored[0]) == len(writer.arrow_schema(writer.SOURCE_OPEN_METEO).names)


def test_month_key_uses_zero_padded_hive_style_segments():
    assert writer.month_key(writer.SOURCE_ENTSOE, date(2024, 3, 1)) == (
        "entsoe/year=2024/month=03.parquet"
    )


def test_year_key_is_used_for_annual_grain_sources():
    assert writer.year_key(writer.SOURCE_WORLDBANK, 2024) == "worldbank/year=2024.parquet"


def test_merge_keeps_rows_that_differ_by_merge_key():
    existing = [_weather_row("IE", "2024-03-01")]
    incoming = [_weather_row("DE", "2024-03-01")]

    merged = writer.merge_records(writer.SOURCE_OPEN_METEO, existing, incoming)

    assert len(merged) == 2


def test_merge_replaces_rows_sharing_the_merge_key():
    existing = [_weather_row("IE", "2024-03-01", value=1.0)]
    incoming = [_weather_row("IE", "2024-03-01", value=99.0)]

    merged = writer.merge_records(writer.SOURCE_OPEN_METEO, existing, incoming)

    assert len(merged) == 1
    assert merged[0]["source_value"] == 99.0


def test_write_month_adds_to_what_an_earlier_attempt_landed():
    # The behaviour the retry story depends on: a later attempt
    # resolving a different country must not erase the first one's rows.
    store = _store()
    writer.write_month(
        store, writer.SOURCE_OPEN_METEO, date(2024, 3, 1), [_weather_row("IE", "2024-03-01")],
    )
    total = writer.write_month(
        store, writer.SOURCE_OPEN_METEO, date(2024, 3, 1), [_weather_row("DE", "2024-03-01")],
    )

    assert total == 2
    key = store.key(writer.month_key(writer.SOURCE_OPEN_METEO, date(2024, 3, 1)))
    stored = writer.from_parquet_bytes(writer.SOURCE_OPEN_METEO, store.get(key))
    assert {row["country_code"] for row in stored} == {"IE", "DE"}


def test_write_month_with_no_records_leaves_an_existing_file_untouched():
    # An attempt that produced nothing must not erase rows an earlier
    # attempt landed.
    store = _store()
    writer.write_month(
        store, writer.SOURCE_OPEN_METEO, date(2024, 3, 1), [_weather_row("IE", "2024-03-01")],
    )

    assert writer.write_month(store, writer.SOURCE_OPEN_METEO, date(2024, 3, 1), []) == 0

    key = store.key(writer.month_key(writer.SOURCE_OPEN_METEO, date(2024, 3, 1)))
    assert len(writer.from_parquet_bytes(writer.SOURCE_OPEN_METEO, store.get(key))) == 1


def test_write_month_is_idempotent_for_the_same_rows():
    store = _store()
    rows = [_weather_row("IE", "2024-03-01")]
    writer.write_month(store, writer.SOURCE_OPEN_METEO, date(2024, 3, 1), rows)

    assert writer.write_month(store, writer.SOURCE_OPEN_METEO, date(2024, 3, 1), rows) == 1
