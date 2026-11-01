"""Merging the staged historical Parquet layer into Bronze (Spec 010).

The second half of the Bronze load. Everything `src/blob/stager.py` put
on the volume is merged into the Bronze Delta tables, one
(source, year) batch at a time.

Spark reads the Parquet files itself, straight off the volume. The
driver never holds a row: a decade of ENTSO-E history is tens of
millions of rows, and materialising them as Python objects to hand to
`createDataFrame` would move the entire dataset through a single
process for no benefit.

A batch is one source-year rather than one source-month. The merge, not
the read, is the expensive half, and a year is small enough to merge in
one operation while still being a unit worth resuming from. Months are
loaded together within a year; years are merged oldest first, so a pass
interrupted part-way leaves Bronze holding a contiguous history rather
than islands.

The merge is the same `write_with_deterministic_schema` path the daily
pipeline uses, keyed on the same Bronze merge key, so merging a period
that is already present updates those rows in place instead of
duplicating them. Rows Bronze holds that the staged files do not are
left alone — a MERGE never deletes.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone as dt_timezone
from typing import Dict, List, Optional, Sequence, Tuple

from src.blob import stager
from src.blob import writer as blob_writer

logger = logging.getLogger(__name__)

# Bronze table and schema per source, resolved lazily so this module
# stays importable without PySpark.
_BRONZE_TABLES = {
    blob_writer.SOURCE_ENTSOE: "bronze_entsoe_energy",
    blob_writer.SOURCE_OPEN_METEO: "bronze_open_meteo_weather",
    blob_writer.SOURCE_WORLDBANK: "bronze_worldbank_indicators",
}

LOAD_STATE_FILENAME = "_load_state.json"


@dataclass
class LoadResult:
    """What one load pass moved into Bronze."""

    batches_loaded: int = 0
    batches_skipped: int = 0
    rows_by_source: Dict[str, int] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return not self.errors


class LoadState:
    """Which (source, year) batches have already been merged, and from what.

    Progress is recorded explicitly rather than inferred from Bronze
    row counts: Bronze also holds rows the daily pipeline wrote, so a
    count can never distinguish "this batch was merged" from "some of
    these rows arrived another way".

    What is recorded is not "this year was merged" but "this year was
    merged from exactly these files". A year is not a fixed quantity:
    the current one gains a month every month, so a batch recorded as
    done by name alone would be skipped forever after the first load and
    silently stop at whatever month it held that day. Comparing the
    file inventory makes a year that has grown look unmerged again,
    which is what it is.

    The merge is idempotent, so skipping is an optimisation rather than
    a correctness mechanism — it exists because re-merging a decade to
    retry one year costs compute that is in genuinely short supply.
    """

    def __init__(self, path: str, data: Optional[Dict[str, dict]] = None):
        self.path = path
        self._data: Dict[str, dict] = data or {}

    @classmethod
    def load(cls, volume_root: str) -> "LoadState":
        path = f"{volume_root.rstrip('/')}/{LOAD_STATE_FILENAME}"
        if not os.path.exists(path):
            return cls(path)
        with open(path, "r", encoding="utf-8") as handle:
            return cls(path, json.load(handle))

    @staticmethod
    def batch_id(source: str, year: int) -> str:
        return f"{source}:{year:04d}"

    def is_loaded(self, source: str, year: int, fingerprint: Dict[str, int]) -> bool:
        """Whether this batch was merged from exactly the files present now.

        An entry recorded without a fingerprint counts as not loaded: it
        cannot be shown to match, and re-merging is idempotent whereas
        wrongly skipping is not recoverable.
        """
        entry = self._data.get(self.batch_id(source, year))
        if not entry:
            return False
        return entry.get("files") == fingerprint

    def record(
        self, source: str, year: int, rows: int, fingerprint: Dict[str, int],
    ) -> None:
        self._data[self.batch_id(source, year)] = {
            "rows": rows,
            "loaded_at": datetime.now(dt_timezone.utc).isoformat(),
            "files": fingerprint,
        }

    def save(self) -> None:
        """Persist the state, saved after every batch rather than at the end.

        A pass that dies after merging eight years must not lose the
        record of those eight.
        """
        with open(self.path, "w", encoding="utf-8") as handle:
            json.dump(self._data, handle, indent=2, sort_keys=True)


def _bronze_schema_and_keys(source: str):
    from src.ingestion.delta_schema import (
        ENTSOE_BRONZE_KEY_COLS,
        OPEN_METEO_BRONZE_KEY_COLS,
        WORLDBANK_BRONZE_KEY_COLS,
        entsoe_bronze_schema,
        open_meteo_bronze_schema,
        worldbank_bronze_schema,
    )

    mapping = {
        blob_writer.SOURCE_ENTSOE: (entsoe_bronze_schema, ENTSOE_BRONZE_KEY_COLS),
        blob_writer.SOURCE_OPEN_METEO: (open_meteo_bronze_schema, OPEN_METEO_BRONZE_KEY_COLS),
        blob_writer.SOURCE_WORLDBANK: (worldbank_bronze_schema, WORLDBANK_BRONZE_KEY_COLS),
    }
    schema_fn, key_cols = mapping[source]
    return schema_fn(), key_cols


def bronze_table(source: str, table_prefix: str = "") -> str:
    """Bronze table name for `source`, optionally prefixed for a test run."""
    return f"{table_prefix}{_BRONZE_TABLES[source]}"


def batch_fingerprint(
    volume_root: str, source: str, year: int,
) -> Dict[str, int]:
    """The staged files of one batch, as `{filename: size}`.

    Name and size rather than a content hash: the files are written once
    and never edited in place, so a month being added, replaced or
    re-staged at a different size is the only change that can occur, and
    all three show up here. Hashing a few hundred megabytes on every
    pass to learn the same thing would not be worth it.
    """
    directory = stager.batch_directory(volume_root, source, year)
    if not os.path.isdir(directory):
        return {}
    return {
        entry: os.path.getsize(f"{directory}/{entry}")
        for entry in sorted(os.listdir(directory))
        if entry.endswith(".parquet")
    }


def discover_batches(
    volume_root: str = stager.DEFAULT_VOLUME_ROOT,
    *,
    sources: Optional[Sequence[str]] = None,
) -> List[Tuple[str, int]]:
    """Every (source, year) batch staged on the volume, oldest year first.

    Sorted by year across sources so the whole load advances through
    history together, rather than finishing one source's decade before
    starting the next.
    """
    wanted = set(sources) if sources else set(stager.KNOWN_SOURCES)
    root = volume_root.rstrip("/")
    batches: List[Tuple[int, str]] = []
    for source in sorted(wanted):
        source_dir = f"{root}/{source}"
        if not os.path.isdir(source_dir):
            continue
        for entry in os.listdir(source_dir):
            if not entry.isdigit():
                continue
            if not os.path.isdir(f"{source_dir}/{entry}"):
                continue
            batches.append((int(entry), source))
    return [(source, year) for year, source in sorted(batches)]


def load_batch(
    spark, source: str, year: int,
    volume_root: str = stager.DEFAULT_VOLUME_ROOT,
    *,
    table_prefix: str = "",
) -> int:
    """Merge one (source, year) batch into its Bronze table. Returns rows merged."""
    from src.ingestion.delta_schema import write_with_deterministic_schema
    from src.transformations.dedupe import dedupe_latest

    schema, key_cols = _bronze_schema_and_keys(source)
    directory = stager.batch_directory(volume_root, source, year)

    df = spark.read.parquet(directory)
    # Selected explicitly rather than trusting the file's column order:
    # the Parquet files are written from the same field definitions as
    # this schema, but `write_with_deterministic_schema` compares
    # positionally and a reordering would be silent otherwise.
    df = df.select(*[f.name for f in schema.fields])
    # The same pre-merge dedupe the daily pipeline applies. Each month's
    # file is already keyed-unique by the Parquet writer and months
    # within a year cannot collide, so this is a safety net — but the
    # MERGE requires uniqueness on the key regardless.
    df = dedupe_latest(df, key_cols)
    return write_with_deterministic_schema(
        spark, df, bronze_table(source, table_prefix), schema, key_cols,
    )


def load_all(
    spark,
    volume_root: str = stager.DEFAULT_VOLUME_ROOT,
    *,
    sources: Optional[Sequence[str]] = None,
    table_prefix: str = "",
    resume: bool = True,
) -> LoadResult:
    """Merge every staged batch into Bronze, oldest year first.

    A batch that fails is recorded and the pass continues: one bad year
    must not stop the rest of a decade from loading. The caller is
    expected to fail the run afterwards, so an incomplete Bronze is
    never reported as a clean load.
    """
    result = LoadResult()
    state = LoadState.load(volume_root)

    for source, year in discover_batches(volume_root, sources=sources):
        fingerprint = batch_fingerprint(volume_root, source, year)
        if resume and state.is_loaded(source, year, fingerprint):
            logger.info(
                "Skipping %s %d: already merged from the same %d file(s)",
                source, year, len(fingerprint),
            )
            result.batches_skipped += 1
            continue
        try:
            rows = load_batch(spark, source, year, volume_root, table_prefix=table_prefix)
        except Exception as exc:  # noqa: BLE001 - one bad batch must not stop the pass
            logger.error("Failed to load %s %d: %s", source, year, exc)
            result.errors[LoadState.batch_id(source, year)] = str(exc)
            continue
        state.record(source, year, rows, fingerprint)
        state.save()
        result.batches_loaded += 1
        result.rows_by_source[source] = result.rows_by_source.get(source, 0) + rows
        logger.info("Merged %s %d: %d rows", source, year, rows)

    return result
