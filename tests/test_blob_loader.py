"""Tests for merging the staged historical layer into Bronze (Spec 010).

Spark is the external system here and is replaced with a double. What
these tests pin down is the loader's own logic: which batches it finds,
what order it merges them in, what it records, and what it does when one
of them fails.
"""
import json

import pytest

from src.blob import loader, stager, writer

# --- doubles ---------------------------------------------------------


class FakeFrame:
    """Records the selections asked of it; carries a declared row count."""

    def __init__(self, path, rows=1):
        self.path = path
        self.rows = rows
        self.selected = None

    def select(self, *cols):
        self.selected = list(cols)
        return self


class FakeReader:
    def __init__(self, spark):
        self._spark = spark

    def parquet(self, path):
        frame = FakeFrame(path, self._spark.rows_per_batch)
        self._spark.read_paths.append(path)
        return frame


class FakeSpark:
    def __init__(self, rows_per_batch=1):
        self.read_paths = []
        self.rows_per_batch = rows_per_batch

    @property
    def read(self):
        return FakeReader(self)


@pytest.fixture
def spark_path(monkeypatch):
    """Replace the Delta write, the dedupe and the schema builders."""
    written = []

    import src.ingestion.delta_schema as delta_schema
    import src.transformations.dedupe as dedupe

    monkeypatch.setattr(
        delta_schema, "write_with_deterministic_schema",
        lambda spark, df, table, schema, keys: (
            written.append((table, df)), df.rows
        )[1],
        raising=False,
    )
    monkeypatch.setattr(dedupe, "dedupe_latest", lambda df, keys: df, raising=False)

    class FakeField:
        def __init__(self, name):
            self.name = name

    class FakeSchema:
        fields = [FakeField("country_code"), FakeField("observation_date")]

    for name in ("entsoe_bronze_schema", "open_meteo_bronze_schema", "worldbank_bronze_schema"):
        monkeypatch.setattr(delta_schema, name, lambda: FakeSchema(), raising=False)
    return written


def _stage(volume_root, source, year, names=("01",)):
    """Create the files a staging pass would have left behind."""
    directory = volume_root / source / f"{year:04d}"
    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        (directory / f"{name}.parquet").write_bytes(b"parquet")


# --- batch discovery -------------------------------------------------


def test_discover_batches_finds_one_batch_per_source_year(tmp_path):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015, ("01", "02"))
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2016)

    assert loader.discover_batches(str(tmp_path)) == [
        (writer.SOURCE_ENTSOE, 2015), (writer.SOURCE_ENTSOE, 2016),
    ]


def test_discover_batches_orders_by_year_across_sources(tmp_path):
    # The whole load advances through history together rather than
    # finishing one source's decade before starting the next.
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2016)
    _stage(tmp_path, writer.SOURCE_OPEN_METEO, 2015)

    assert loader.discover_batches(str(tmp_path)) == [
        (writer.SOURCE_OPEN_METEO, 2015), (writer.SOURCE_ENTSOE, 2016),
    ]


def test_discover_batches_ignores_non_year_directories(tmp_path):
    # The load state file lives at the volume root, and a stray
    # directory must not be read as a batch.
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)
    (tmp_path / writer.SOURCE_ENTSOE / "scratch").mkdir()

    assert loader.discover_batches(str(tmp_path)) == [(writer.SOURCE_ENTSOE, 2015)]


def test_discover_batches_is_empty_when_nothing_is_staged(tmp_path):
    assert loader.discover_batches(str(tmp_path)) == []


def test_discover_batches_can_be_narrowed_to_one_source(tmp_path):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)
    _stage(tmp_path, writer.SOURCE_OPEN_METEO, 2015)

    batches = loader.discover_batches(str(tmp_path), sources=[writer.SOURCE_ENTSOE])

    assert batches == [(writer.SOURCE_ENTSOE, 2015)]


# --- merging ---------------------------------------------------------


def test_load_batch_reads_the_whole_year_in_one_go(tmp_path, spark_path):
    # A year is the batch, not a month: the merge is the expensive half,
    # so twelve months are merged in one operation.
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015, ("01", "02", "03"))
    spark = FakeSpark(rows_per_batch=300)

    rows = loader.load_batch(spark, writer.SOURCE_ENTSOE, 2015, str(tmp_path))

    assert rows == 300
    assert spark.read_paths == [f"{tmp_path}/entsoe/2015"]


def test_load_batch_selects_the_schema_columns_explicitly_before_merging(tmp_path, spark_path):
    # write_with_deterministic_schema compares positionally, so a
    # reordering in the file would otherwise be silent.
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)

    loader.load_batch(FakeSpark(), writer.SOURCE_ENTSOE, 2015, str(tmp_path))

    table, frame = spark_path[0]
    assert table == "bronze_entsoe_energy"
    assert frame.selected == ["country_code", "observation_date"]


def test_load_batch_merges_into_the_source_s_bronze_table(tmp_path, spark_path):
    _stage(tmp_path, writer.SOURCE_OPEN_METEO, 2015)

    loader.load_batch(FakeSpark(), writer.SOURCE_OPEN_METEO, 2015, str(tmp_path))

    assert spark_path[0][0] == "bronze_open_meteo_weather"


def test_load_all_merges_oldest_year_first(tmp_path, spark_path):
    # Oldest first so a pass interrupted part-way leaves Bronze holding
    # a contiguous history rather than islands.
    for year in (2017, 2015, 2016):
        _stage(tmp_path, writer.SOURCE_ENTSOE, year)

    result = loader.load_all(FakeSpark(), str(tmp_path))

    assert [frame.path for _table, frame in spark_path] == [
        f"{tmp_path}/entsoe/2015", f"{tmp_path}/entsoe/2016", f"{tmp_path}/entsoe/2017",
    ]
    assert result.batches_loaded == 3


def test_load_all_sums_rows_per_source(tmp_path, spark_path):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2016)

    result = loader.load_all(FakeSpark(rows_per_batch=100), str(tmp_path))

    assert result.rows_by_source == {writer.SOURCE_ENTSOE: 200}


def test_one_bad_batch_does_not_stop_the_pass(tmp_path, spark_path, monkeypatch):
    # One bad year must not stop the rest of a decade from loading.
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2016)

    original = loader.load_batch

    def flaky(spark, source, year, volume_root, **kwargs):
        if year == 2015:
            raise RuntimeError("unreadable")
        return original(spark, source, year, volume_root, **kwargs)

    monkeypatch.setattr(loader, "load_batch", flaky)
    result = loader.load_all(FakeSpark(), str(tmp_path))

    assert result.batches_loaded == 1
    assert list(result.errors) == ["entsoe:2015"]
    assert not result.succeeded


# --- resume state ----------------------------------------------------


def test_a_merged_batch_is_recorded_in_the_load_state(tmp_path, spark_path):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)

    loader.load_all(FakeSpark(rows_per_batch=42), str(tmp_path))

    state = json.loads((tmp_path / loader.LOAD_STATE_FILENAME).read_text())
    assert state["entsoe:2015"]["rows"] == 42
    assert "loaded_at" in state["entsoe:2015"]


def test_an_already_merged_batch_is_skipped_on_the_next_pass(tmp_path, spark_path):
    # The merge is idempotent, so this is an optimisation — but
    # re-merging a decade to retry one year costs compute that is in
    # genuinely short supply.
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)
    loader.load_all(FakeSpark(), str(tmp_path))

    result = loader.load_all(FakeSpark(), str(tmp_path))

    assert result.batches_loaded == 0
    assert result.batches_skipped == 1


def test_resume_can_be_turned_off_to_force_a_re_merge(tmp_path, spark_path):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)
    loader.load_all(FakeSpark(), str(tmp_path))

    result = loader.load_all(FakeSpark(), str(tmp_path), resume=False)

    assert result.batches_loaded == 1
    assert result.batches_skipped == 0


def test_a_failed_batch_is_not_recorded_as_loaded(tmp_path, spark_path, monkeypatch):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015)

    monkeypatch.setattr(
        loader, "load_batch",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    loader.load_all(FakeSpark(), str(tmp_path))

    state = loader.LoadState.load(str(tmp_path))
    fingerprint = loader.batch_fingerprint(str(tmp_path), writer.SOURCE_ENTSOE, 2015)
    assert not state.is_loaded(writer.SOURCE_ENTSOE, 2015, fingerprint)


def test_state_is_saved_after_every_batch_not_at_the_end(tmp_path, spark_path, monkeypatch):
    # A pass that dies after merging eight years must not lose the
    # record of those eight.
    for year in (2015, 2016):
        _stage(tmp_path, writer.SOURCE_ENTSOE, year)

    original = loader.load_batch

    def dies_on_the_second(spark, source, year, volume_root, **kwargs):
        if year == 2016:
            raise KeyboardInterrupt("killed mid-pass")
        return original(spark, source, year, volume_root, **kwargs)

    monkeypatch.setattr(loader, "load_batch", dies_on_the_second)
    with pytest.raises(KeyboardInterrupt):
        loader.load_all(FakeSpark(), str(tmp_path))

    state = loader.LoadState.load(str(tmp_path))
    fingerprint = loader.batch_fingerprint(str(tmp_path), writer.SOURCE_ENTSOE, 2015)
    assert state.is_loaded(writer.SOURCE_ENTSOE, 2015, fingerprint)


def test_load_state_of_a_fresh_volume_is_empty(tmp_path):
    state = loader.LoadState.load(str(tmp_path))

    assert not state.is_loaded(writer.SOURCE_ENTSOE, 2015, {})


def test_bronze_table_can_be_prefixed_for_a_test_run():
    assert loader.bronze_table(writer.SOURCE_ENTSOE, "sandbox_") == (
        "sandbox_bronze_entsoe_energy"
    )


# --- a year that grows after being merged ----------------------------


def test_a_year_that_gains_a_month_is_merged_again(tmp_path, spark_path):
    # The case this exists for: the current year is merged, then the
    # next month's file is staged into it. Keyed on the year alone, the
    # batch would be skipped forever and that month would never reach
    # Bronze — with no error and a green run.
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2026, ("01", "02"))
    loader.load_all(FakeSpark(), str(tmp_path))

    _stage(tmp_path, writer.SOURCE_ENTSOE, 2026, ("03",))
    result = loader.load_all(FakeSpark(), str(tmp_path))

    assert result.batches_loaded == 1
    assert result.batches_skipped == 0


def test_a_year_whose_month_was_re_staged_at_a_new_size_is_merged_again(tmp_path, spark_path):
    # A month re-fetched with more countries in it is a different file,
    # not a different name.
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015, ("01",))
    loader.load_all(FakeSpark(), str(tmp_path))

    (tmp_path / "entsoe" / "2015" / "01.parquet").write_bytes(b"parquet with more rows")
    result = loader.load_all(FakeSpark(), str(tmp_path))

    assert result.batches_loaded == 1


def test_an_unchanged_year_is_still_skipped(tmp_path, spark_path):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015, ("01", "02"))
    loader.load_all(FakeSpark(), str(tmp_path))

    result = loader.load_all(FakeSpark(), str(tmp_path))

    assert result.batches_loaded == 0
    assert result.batches_skipped == 1


def test_the_fingerprint_records_every_staged_file_and_its_size(tmp_path):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015, ("01", "02"))

    fingerprint = loader.batch_fingerprint(str(tmp_path), writer.SOURCE_ENTSOE, 2015)

    assert fingerprint == {"01.parquet": 7, "02.parquet": 7}


def test_the_fingerprint_ignores_non_parquet_files(tmp_path):
    _stage(tmp_path, writer.SOURCE_ENTSOE, 2015, ("01",))
    (tmp_path / "entsoe" / "2015" / "01.parquet.part").write_bytes(b"truncated")

    fingerprint = loader.batch_fingerprint(str(tmp_path), writer.SOURCE_ENTSOE, 2015)

    assert list(fingerprint) == ["01.parquet"]


def test_a_state_entry_without_a_fingerprint_counts_as_not_loaded(tmp_path):
    # Re-merging is idempotent; wrongly skipping is not recoverable.
    state = loader.LoadState(
        str(tmp_path / loader.LOAD_STATE_FILENAME),
        {"entsoe:2015": {"rows": 10, "loaded_at": "2026-01-01T00:00:00+00:00"}},
    )

    assert not state.is_loaded(writer.SOURCE_ENTSOE, 2015, {"01.parquet": 7})
