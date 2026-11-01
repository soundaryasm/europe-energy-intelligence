"""Tests for staging the historical Parquet layer onto a volume (Spec 010).

The volume is a local directory here. That is the whole of what the
staging half depends on — it writes files with the Python file API and
nothing else — so these tests exercise the real code path rather than a
double.
"""
import os
from datetime import date

import pytest

from src.blob import stager, writer
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


def _write_month(store, month, country="IE"):
    writer.write_month(
        store, writer.SOURCE_OPEN_METEO, month, [_weather_row(country, month.isoformat())],
    )


# --- key parsing -----------------------------------------------------


def test_parse_blob_key_identifies_a_monthly_source():
    key = "europe-energy/entsoe/year=2024/month=03.parquet"

    assert stager.parse_blob_key(key) == ("entsoe", 2024, "03")


def test_parse_blob_key_identifies_an_annual_source():
    key = "europe-energy/worldbank/year=2024.parquet"

    assert stager.parse_blob_key(key) == ("worldbank", 2024, stager.ANNUAL_NAME)


def test_parse_blob_key_ignores_the_manifest():
    # The manifest lives under the same prefix and must never be staged
    # as data.
    assert stager.parse_blob_key("europe-energy/_manifest.json") is None


def test_parse_blob_key_ignores_an_unknown_source():
    assert stager.parse_blob_key("europe-energy/other/year=2024/month=01.parquet") is None


# --- staged layout ---------------------------------------------------


def test_staged_paths_drop_the_hive_partition_syntax():
    # Spark reads `year=`/`month=` directories as a partitioned dataset
    # and synthesises a `year` column, which would no longer match the
    # Bronze schema.
    path = stager.staged_relative_path("entsoe", 2024, "03")

    assert path == "entsoe/2024/03.parquet"
    assert "=" not in path


def test_an_annual_source_stages_into_a_year_directory_like_any_other():
    # A batch is then always one directory, whatever the source's grain.
    path = stager.staged_relative_path("worldbank", 2024, stager.ANNUAL_NAME)

    assert path == "worldbank/2024/year.parquet"


def test_volume_identifier_is_derived_from_the_path():
    assert stager.volume_identifier("/Volumes/workspace/default/historical") == (
        "workspace.default.historical"
    )


def test_volume_identifier_rejects_a_path_that_is_not_a_volume():
    with pytest.raises(ValueError):
        stager.volume_identifier("/tmp/not-a-volume")


# --- staging ---------------------------------------------------------


def test_stage_all_copies_every_file_onto_the_volume(tmp_path):
    store = _store()
    _write_month(store, date(2024, 1, 1))
    _write_month(store, date(2024, 2, 1))

    result = stager.stage_all(store, str(tmp_path))

    assert result.files_staged == 2
    assert result.succeeded
    assert (tmp_path / "open_meteo" / "2024" / "01.parquet").exists()
    assert (tmp_path / "open_meteo" / "2024" / "02.parquet").exists()


def test_the_staged_bytes_are_identical_to_the_blob_object(tmp_path):
    # A byte-for-byte copy, not a re-serialisation: the file is the
    # contract between the two halves of this layer.
    store = _store()
    _write_month(store, date(2024, 1, 1))
    key = store.key(writer.month_key(writer.SOURCE_OPEN_METEO, date(2024, 1, 1)))

    stager.stage_all(store, str(tmp_path))

    staged = (tmp_path / "open_meteo" / "2024" / "01.parquet").read_bytes()
    assert staged == store.get(key)


def test_an_already_staged_file_is_skipped(tmp_path):
    # What makes a resumed pass cheap rather than a re-download of a
    # decade.
    store = _store()
    _write_month(store, date(2024, 1, 1))
    stager.stage_all(store, str(tmp_path))

    result = stager.stage_all(store, str(tmp_path))

    assert result.files_staged == 0
    assert result.files_skipped == 1


def test_staging_leaves_no_part_file_behind(tmp_path):
    # A file at its final path is always complete, which is what lets a
    # resumed pass trust what it finds.
    store = _store()
    _write_month(store, date(2024, 1, 1))

    stager.stage_all(store, str(tmp_path))

    staged = os.listdir(tmp_path / "open_meteo" / "2024")
    assert staged == ["01.parquet"]


def test_an_interrupted_write_is_not_mistaken_for_a_staged_file(tmp_path):
    # Simulates a pass killed mid-download: the partial file carries the
    # .part suffix, so the next pass does not skip the month.
    store = _store()
    _write_month(store, date(2024, 1, 1))
    partial = tmp_path / "open_meteo" / "2024" / f"01.parquet{stager.PART_SUFFIX}"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"truncated")

    result = stager.stage_all(store, str(tmp_path))

    assert result.files_staged == 1
    key = store.key(writer.month_key(writer.SOURCE_OPEN_METEO, date(2024, 1, 1)))
    assert (tmp_path / "open_meteo" / "2024" / "01.parquet").read_bytes() == store.get(key)


def test_stage_all_skips_non_data_keys(tmp_path):
    store = _store()
    store.put(store.key("_manifest.json"), b"{}", content_type="application/json")

    result = stager.stage_all(store, str(tmp_path))

    assert result.files_staged == 0
    assert result.files_skipped == 1


def test_stage_all_can_be_narrowed_to_one_source(tmp_path):
    store = _store()
    _write_month(store, date(2024, 1, 1))

    result = stager.stage_all(store, str(tmp_path), sources=[writer.SOURCE_ENTSOE])

    assert result.files_staged == 0
    assert result.files_skipped == 1


def test_one_unreadable_object_does_not_stop_the_pass(tmp_path, monkeypatch):
    # One bad object must not stop the rest of a decade from staging.
    store = _store()
    _write_month(store, date(2024, 1, 1))
    _write_month(store, date(2024, 2, 1))

    original = stager.stage_file

    def flaky(s, key, volume_root):
        if "month=01" in key:
            raise RuntimeError("corrupt")
        return original(s, key, volume_root)

    monkeypatch.setattr(stager, "stage_file", flaky)
    result = stager.stage_all(store, str(tmp_path))

    assert result.files_staged == 1
    assert len(result.errors) == 1
    assert not result.succeeded


def test_stage_file_raises_if_the_key_vanished_after_listing(tmp_path):
    with pytest.raises(FileNotFoundError):
        stager.stage_file(
            _store(), "europe-energy/open_meteo/year=2024/month=01.parquet", str(tmp_path),
        )
