"""Tests for loading the historical Parquet layer into Bronze (Spec 010).

Spark is the external system here and is replaced with a double; the
Parquet round-trip is real, since the file format is the contract
between the two halves of this layer.
"""
from datetime import date

import pytest

from src.blob import loader, writer
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


def _weather_row(country, day):
    return {
        "country_code": country, "country_name": "Name", "reference_location": "City",
        "latitude": 1.0, "longitude": 2.0, "timezone": "UTC",
        "returned_latitude": 1.0, "returned_longitude": 2.0, "returned_timezone": "UTC",
        "utc_offset_seconds": 0, "observation_date": day,
        "source_variable": "temperature_2m_mean", "source_value": 1.0, "source_unit": "C",
        "source_endpoint": "archive_daily", "source_system": "open-meteo",
        "ingestion_timestamp": "2026-01-01T00:00:00+00:00",
    }


def test_parse_key_identifies_a_monthly_source_and_period():
    key = "europe-energy/entsoe/year=2024/month=03.parquet"

    assert loader.parse_key(key) == ("entsoe", "2024-03")


def test_parse_key_identifies_an_annual_source_and_period():
    key = "europe-energy/worldbank/year=2024.parquet"

    assert loader.parse_key(key) == ("worldbank", "2024")


def test_parse_key_ignores_the_manifest():
    # The manifest lives under the same prefix and must never be loaded
    # as data.
    assert loader.parse_key("europe-energy/_manifest.json") is None


def test_parse_key_ignores_an_unknown_source():
    assert loader.parse_key("europe-energy/other/year=2024/month=01.parquet") is None


class FakeSpark:
    """Records the DataFrames it was asked to build."""

    def __init__(self):
        self.frames = []

    def createDataFrame(self, records, schema=None):  # noqa: N802 - Spark API name
        frame = FakeFrame(records)
        self.frames.append(frame)
        return frame


class FakeFrame:
    def __init__(self, records):
        self.records = records


def _patch_spark_path(monkeypatch, written):
    """Replace the Delta write and the dedupe with doubles."""
    import src.ingestion.delta_schema as delta_schema
    import src.transformations.dedupe as dedupe

    monkeypatch.setattr(
        delta_schema, "write_with_deterministic_schema",
        lambda spark, df, table, schema, keys: (
            written.append((table, len(df.records))), len(df.records)
        )[1],
        raising=False,
    )
    monkeypatch.setattr(dedupe, "dedupe_latest", lambda df, keys: df, raising=False)
    # The schema builders need PySpark; the double does not use the
    # schema, so they are replaced too.
    for name in ("entsoe_bronze_schema", "open_meteo_bronze_schema", "worldbank_bronze_schema"):
        monkeypatch.setattr(delta_schema, name, lambda: None, raising=False)


def test_load_file_merges_a_month_into_its_bronze_table(monkeypatch):
    written = []
    _patch_spark_path(monkeypatch, written)

    store = _store()
    rows = [_weather_row("IE", "2024-03-01"), _weather_row("DE", "2024-03-01")]
    writer.write_month(store, writer.SOURCE_OPEN_METEO, date(2024, 3, 1), rows)
    key = store.key(writer.month_key(writer.SOURCE_OPEN_METEO, date(2024, 3, 1)))

    count = loader.load_file(FakeSpark(), store, key, writer.SOURCE_OPEN_METEO)

    assert count == 2
    assert written == [("bronze_open_meteo_weather", 2)]


def test_load_file_raises_if_the_key_vanished_after_listing():
    with pytest.raises(FileNotFoundError):
        loader.load_file(FakeSpark(), _store(), "europe-energy/gone.parquet", "entsoe")


def test_load_all_loads_oldest_period_first(monkeypatch):
    # Oldest first so a pass interrupted part-way leaves Bronze holding
    # a contiguous history rather than islands.
    written = []
    _patch_spark_path(monkeypatch, written)

    store = _store()
    for month in (date(2024, 5, 1), date(2024, 1, 1), date(2024, 3, 1)):
        writer.write_month(
            store, writer.SOURCE_OPEN_METEO, month,
            [_weather_row("IE", month.isoformat())],
        )

    order = []
    original = loader.load_file

    def spy(spark, s, key, source, **kwargs):
        order.append(loader.parse_key(key)[1])
        return original(spark, s, key, source, **kwargs)

    monkeypatch.setattr(loader, "load_file", spy)
    result = loader.load_all(FakeSpark(), store)

    assert order == ["2024-01", "2024-03", "2024-05"]
    assert result.files_loaded == 3


def test_load_all_skips_non_data_keys(monkeypatch):
    _patch_spark_path(monkeypatch, [])
    store = _store()
    store.put(store.key("_manifest.json"), b"{}", content_type="application/json")

    result = loader.load_all(FakeSpark(), store)

    assert result.files_loaded == 0
    assert result.files_skipped == 1


def test_load_all_can_be_narrowed_to_one_source(monkeypatch):
    _patch_spark_path(monkeypatch, [])
    store = _store()
    writer.write_month(
        store, writer.SOURCE_OPEN_METEO, date(2024, 3, 1), [_weather_row("IE", "2024-03-01")],
    )

    result = loader.load_all(FakeSpark(), store, sources=[writer.SOURCE_ENTSOE])

    assert result.files_loaded == 0
    assert result.files_skipped == 1


def test_one_unreadable_file_does_not_stop_the_pass(monkeypatch):
    # One bad month must not stop the rest of a decade from loading.
    _patch_spark_path(monkeypatch, [])
    store = _store()
    for month in (date(2024, 1, 1), date(2024, 2, 1)):
        writer.write_month(
            store, writer.SOURCE_OPEN_METEO, month, [_weather_row("IE", month.isoformat())],
        )

    original = loader.load_file

    def flaky(spark, s, key, source, **kwargs):
        if "month=01" in key:
            raise RuntimeError("corrupt")
        return original(spark, s, key, source, **kwargs)

    monkeypatch.setattr(loader, "load_file", flaky)
    result = loader.load_all(FakeSpark(), store)

    assert result.files_loaded == 1
    assert len(result.errors) == 1
    assert not result.succeeded
