"""Tests for verifying the historical Bronze load (Spec 010).

Spark is replaced with a double that models the one operation this
module depends on: a left-anti join on the merge key. The point under
test is the verdict — that a staged row with no Bronze counterpart is
reported, and that a Bronze table legitimately larger than the staged
set is not.
"""
import pytest

from src.blob import loader, verify, writer


class FakeRow:
    def __init__(self, data):
        self._data = data

    def asDict(self):  # noqa: N802 - Spark API name
        return self._data


class KeyFrame:
    """A DataFrame reduced to the set of merge keys it holds."""

    def __init__(self, keys, cols=None):
        self.keys = list(keys)
        self.cols = list(cols) if cols else []

    def count(self):
        return len(self.keys)

    def select(self, *cols):
        return KeyFrame(self.keys, cols)

    def join(self, other, on=None, how=None):
        assert how == "left_anti", f"verification must anti-join, not {how!r}"
        present = set(other.keys)
        return KeyFrame([k for k in self.keys if k not in present], self.cols)

    def limit(self, n):
        return KeyFrame(self.keys[:n], self.cols)

    def collect(self):
        return [FakeRow(dict(zip(self.cols, key))) for key in self.keys]


class FakeCatalog:
    def __init__(self, tables):
        self._tables = tables

    def tableExists(self, name):  # noqa: N802 - Spark API name
        return name in self._tables


class FakeReader:
    def __init__(self, staged):
        self._staged = staged
        self.options = {}

    def option(self, key, value):
        self.options[key] = value
        return self

    def parquet(self, path):
        return KeyFrame(self._staged)


class FakeSpark:
    def __init__(self, staged, bronze, tables=None):
        self._bronze = bronze
        self.reader = FakeReader(staged)
        self.catalog = FakeCatalog(
            tables if tables is not None else ["bronze_entsoe_energy"]
        )

    @property
    def read(self):
        return self.reader

    def table(self, name):
        return KeyFrame(self._bronze)


@pytest.fixture(autouse=True)
def _no_pyspark_schemas(monkeypatch):
    """The schema builders need PySpark; the double does not use them."""
    monkeypatch.setattr(
        loader, "_bronze_schema_and_keys",
        lambda source: (None, ("country_code", "source_timestamp")),
    )


def _stage(tmp_path, source, year):
    directory = tmp_path / source / f"{year:04d}"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "01.parquet").write_bytes(b"parquet")


def test_a_fully_loaded_source_verifies(tmp_path):
    keys = [("IE", "2015-01-01"), ("DE", "2015-01-01")]
    spark = FakeSpark(staged=keys, bronze=keys)

    verdict = verify.verify_source(spark, writer.SOURCE_ENTSOE, str(tmp_path))

    assert verdict.ok
    assert verdict.missing_rows == 0


def test_bronze_holding_more_rows_than_the_volume_is_not_a_failure(tmp_path):
    # Bronze also holds rows the daily pipeline wrote, so it is
    # legitimately larger. Counts cannot answer the question; a missing
    # key can.
    staged = [("IE", "2015-01-01")]
    bronze = [("IE", "2015-01-01"), ("IE", "2026-10-01"), ("FR", "2026-10-01")]
    spark = FakeSpark(staged=staged, bronze=bronze)

    verdict = verify.verify_source(spark, writer.SOURCE_ENTSOE, str(tmp_path))

    assert verdict.ok
    assert verdict.bronze_rows > verdict.staged_rows


def test_a_staged_row_absent_from_bronze_is_reported(tmp_path):
    staged = [("IE", "2015-01-01"), ("DE", "2015-01-01")]
    spark = FakeSpark(staged=staged, bronze=[("IE", "2015-01-01")])

    verdict = verify.verify_source(spark, writer.SOURCE_ENTSOE, str(tmp_path))

    assert not verdict.ok
    assert verdict.missing_rows == 1


def test_missing_keys_are_sampled_so_the_failure_is_identifiable(tmp_path):
    staged = [("DE", f"2015-01-{day:02d}") for day in range(1, 20)]
    spark = FakeSpark(staged=staged, bronze=[])

    verdict = verify.verify_source(spark, writer.SOURCE_ENTSOE, str(tmp_path))

    assert verdict.missing_rows == 19
    assert len(verdict.samples) == verify.SAMPLE_LIMIT
    assert verdict.samples[0] == {"country_code": "DE", "source_timestamp": "2015-01-01"}


def test_a_missing_bronze_table_is_an_error_not_a_pass(tmp_path):
    # An absent table means the load never ran, which must not read as
    # "nothing is missing".
    spark = FakeSpark(staged=[("IE", "2015-01-01")], bronze=[], tables=[])

    verdict = verify.verify_source(spark, writer.SOURCE_ENTSOE, str(tmp_path))

    assert not verdict.ok
    assert "does not exist" in verdict.error


def test_verify_all_skips_a_source_with_nothing_staged(tmp_path):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)
    keys = [("IE", "2015-01-01")]

    verdicts = verify.verify_all(FakeSpark(staged=keys, bronze=keys), str(tmp_path))

    assert [v.source for v in verdicts] == [writer.SOURCE_ENTSOE]


def test_one_source_failing_does_not_hide_the_verdict_on_the_rest(tmp_path, monkeypatch):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)
    _stage(tmp_path, writer.SOURCE_OPEN_METEO, 2015)

    original = verify.verify_source

    def flaky(spark, source, volume_root, **kwargs):
        if source == writer.SOURCE_ENTSOE:
            raise RuntimeError("table locked")
        return original(spark, source, volume_root, **kwargs)

    monkeypatch.setattr(verify, "verify_source", flaky)
    keys = [("IE", "2015-01-01")]
    spark = FakeSpark(
        staged=keys, bronze=keys,
        tables=["bronze_entsoe_energy", "bronze_open_meteo_weather"],
    )
    verdicts = verify.verify_all(spark, str(tmp_path))

    by_source = {v.source: v for v in verdicts}
    assert "table locked" in by_source[writer.SOURCE_ENTSOE].error
    assert by_source[writer.SOURCE_OPEN_METEO].ok


def test_describe_reports_both_counts_and_the_verdict(tmp_path):
    spark = FakeSpark(staged=[("IE", "2015-01-01")], bronze=[])

    description = verify.verify_source(spark, writer.SOURCE_ENTSOE, str(tmp_path)).describe()

    assert "staged=1" in description
    assert "MISSING" in description


def test_the_staged_read_is_recursive_and_not_partition_inferred(tmp_path):
    # The years are nested directories under the source, and a directory
    # name must never be read as a column value.
    spark = FakeSpark(staged=[("IE", "2015-01-01")], bronze=[("IE", "2015-01-01")])

    verify.verify_source(spark, writer.SOURCE_ENTSOE, str(tmp_path))

    assert spark.reader.options == {"recursiveFileLookup": "true"}
