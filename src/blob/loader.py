"""Loading the historical Parquet layer into Bronze (Spec 010).

Reads every Parquet file the historical ingestion landed in blob storage
and merges it into the Bronze Delta tables, one source-month at a time.

Each file is fetched over HTTPS into the driver and read from there,
rather than registered as an external table: the blob bucket is not a
Unity Catalog external location, and a month's file is a few megabytes,
so the download costs less than the configuration it would otherwise
need.

The merge is the same `write_with_deterministic_schema` path the daily
pipeline uses, keyed on the same Bronze merge key, so loading a month
that is already present updates those rows in place instead of
duplicating them. That is what makes this safe to re-run, and safe to
run against Bronze tables that already hold overlapping history.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from src.blob import writer as blob_writer

logger = logging.getLogger(__name__)

# Bronze table and schema per source, resolved lazily so this module
# stays importable without PySpark.
_BRONZE_TABLES = {
    blob_writer.SOURCE_ENTSOE: "bronze_entsoe_energy",
    blob_writer.SOURCE_OPEN_METEO: "bronze_open_meteo_weather",
    blob_writer.SOURCE_WORLDBANK: "bronze_worldbank_indicators",
}

_MONTH_KEY = re.compile(r"/(?P<source>[a-z_]+)/year=(?P<year>\d{4})/month=(?P<month>\d{2})\.parquet$")
_YEAR_KEY = re.compile(r"/(?P<source>[a-z_]+)/year=(?P<year>\d{4})\.parquet$")


@dataclass
class LoadResult:
    """What one load pass moved into Bronze."""

    files_loaded: int = 0
    files_skipped: int = 0
    rows_by_source: Dict[str, int] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return not self.errors


def parse_key(key: str) -> Optional[Tuple[str, str]]:
    """Identify the source and period of a historical Parquet key.

    Returns `(source, period)` where period is `YYYY-MM` for monthly
    sources and `YYYY` for annual ones, or `None` for a key that is not
    a data file — the manifest, most notably, which lives under the same
    prefix and must not be loaded.
    """
    month = _MONTH_KEY.search(key)
    if month:
        source = month.group("source")
        if source in _BRONZE_TABLES:
            return source, f"{month.group('year')}-{month.group('month')}"
        return None
    year = _YEAR_KEY.search(key)
    if year:
        source = year.group("source")
        if source in _BRONZE_TABLES:
            return source, year.group("year")
    return None


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


def load_file(spark, store, key: str, source: str, *, table_prefix: str = "") -> int:
    """Merge one Parquet file into its Bronze table. Returns rows written."""
    from src.ingestion.delta_schema import write_with_deterministic_schema
    from src.transformations.dedupe import dedupe_latest

    data = store.get(key)
    if data is None:
        raise FileNotFoundError(f"Blob key disappeared between listing and read: {key}")

    records = blob_writer.from_parquet_bytes(source, data)
    if not records:
        logger.info("%s holds no rows; nothing to merge", key)
        return 0

    schema, key_cols = _bronze_schema_and_keys(source)
    table_name = f"{table_prefix}{_BRONZE_TABLES[source]}"

    df = spark.createDataFrame(records, schema=schema)
    # The same pre-merge dedupe the daily pipeline applies. A month's
    # file is already keyed-unique by the Parquet writer, but a file is
    # not the only thing that can land here and the MERGE requires
    # uniqueness on the key regardless.
    df = dedupe_latest(df, key_cols)
    return write_with_deterministic_schema(spark, df, table_name, schema, key_cols)


def load_all(
    spark,
    store,
    *,
    sources: Optional[Sequence[str]] = None,
    table_prefix: str = "",
) -> LoadResult:
    """Merge every historical Parquet file into Bronze, oldest period first.

    Oldest first so a pass interrupted part-way leaves Bronze holding a
    contiguous history rather than islands. A file that fails is
    recorded and the pass continues: one unreadable month must not stop
    the rest of a decade from loading.
    """
    wanted = set(sources) if sources else set(_BRONZE_TABLES)
    result = LoadResult()

    keyed: List[Tuple[str, str, str]] = []
    for key in store.list_keys():
        parsed = parse_key(key)
        if parsed is None:
            result.files_skipped += 1
            continue
        source, period = parsed
        if source not in wanted:
            result.files_skipped += 1
            continue
        keyed.append((period, source, key))

    for period, source, key in sorted(keyed):
        try:
            rows = load_file(spark, store, key, source, table_prefix=table_prefix)
        except Exception as exc:  # noqa: BLE001 - one bad file must not stop the pass
            logger.error("Failed to load %s: %s", key, exc)
            result.errors[key] = str(exc)
            continue
        result.files_loaded += 1
        result.rows_by_source[source] = result.rows_by_source.get(source, 0) + rows
        logger.info("Loaded %s %s (%d rows)", source, period, rows)

    return result
