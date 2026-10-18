"""Parquet writing for the historical blob layer (Spec 010).

One Parquet file per (source, month), written with an Arrow schema
derived from the same field definitions as the Bronze Delta schemas
(`src.ingestion.delta_schema`), so a file landed here loads into Bronze
without a type cast or a schema-evolution step.

A month's file is rewritten in full on every write rather than appended
to: a run may resolve only some of a month's country/dataset
combinations, and the next run has to be able to add the rest without
duplicating what is already there. The existing file is read, keyed
rows are replaced by their newer counterparts, and the union is written
back. Keying uses each table's Bronze merge key, so "the same row" means
the same thing here as it does in the Delta MERGE downstream.
"""
from __future__ import annotations

import io
import logging
from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple

from src.ingestion.delta_schema import (
    ENTSOE_BRONZE_FIELDS,
    ENTSOE_BRONZE_KEY_COLS,
    OPEN_METEO_BRONZE_FIELDS,
    OPEN_METEO_BRONZE_KEY_COLS,
    WORLDBANK_BRONZE_FIELDS,
    WORLDBANK_BRONZE_KEY_COLS,
)

logger = logging.getLogger(__name__)

SOURCE_ENTSOE = "entsoe"
SOURCE_OPEN_METEO = "open_meteo"
SOURCE_WORLDBANK = "worldbank"

# Field definitions and merge key per source, so a caller names a source
# rather than threading schemas and keys through every call.
_SOURCE_SPECS = {
    SOURCE_ENTSOE: (ENTSOE_BRONZE_FIELDS, ENTSOE_BRONZE_KEY_COLS),
    SOURCE_OPEN_METEO: (OPEN_METEO_BRONZE_FIELDS, OPEN_METEO_BRONZE_KEY_COLS),
    SOURCE_WORLDBANK: (WORLDBANK_BRONZE_FIELDS, WORLDBANK_BRONZE_KEY_COLS),
}


class UnknownSourceError(ValueError):
    """A source name with no registered schema and merge key."""


def _spec(source: str) -> Tuple[Sequence[tuple], Sequence[str]]:
    try:
        return _SOURCE_SPECS[source]
    except KeyError:
        raise UnknownSourceError(
            f"Unknown source {source!r}; expected one of {sorted(_SOURCE_SPECS)}"
        ) from None


def arrow_schema(source: str):
    """Arrow schema for `source`, derived from its Bronze field definitions."""
    import pyarrow as pa

    arrow_types = {
        "string": pa.string(), "double": pa.float64(),
        "int": pa.int32(), "long": pa.int64(),
    }
    fields, _ = _spec(source)
    return pa.schema(
        [pa.field(name, arrow_types[type_name], nullable) for name, type_name, nullable in fields]
    )


def month_key(source: str, month_start: date) -> str:
    """Object key suffix for one source-month, relative to the project prefix.

    Hive-style `year=`/`month=` segments so a listing sorts
    chronologically and a single month can be addressed without reading
    an index.
    """
    _spec(source)
    return f"{source}/year={month_start.year:04d}/month={month_start.month:02d}.parquet"


def year_key(source: str, year: int) -> str:
    """Object key suffix for one source-year, for sources with annual grain."""
    _spec(source)
    return f"{source}/year={year:04d}.parquet"


def to_parquet_bytes(source: str, records: List[dict]) -> bytes:
    """Serialise `records` as a Parquet file under `source`'s Arrow schema.

    Columns absent from a record are written as null rather than
    dropped, so a record set that legitimately lacks an optional column
    still produces a file with the full Bronze column set.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = arrow_schema(source)
    columns = {
        name: [record.get(name) for record in records] for name in schema.names
    }
    table = pa.Table.from_pydict(columns, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def from_parquet_bytes(source: str, data: bytes) -> List[dict]:
    """Read a Parquet file written by `to_parquet_bytes` back into records."""
    import pyarrow.parquet as pq

    _spec(source)
    table = pq.read_table(io.BytesIO(data))
    return table.to_pylist()


def _key_of(record: dict, key_cols: Sequence[str]) -> tuple:
    return tuple(record.get(col) for col in key_cols)


def merge_records(
    source: str, existing: List[dict], incoming: List[dict]
) -> List[dict]:
    """Union `existing` and `incoming`, with `incoming` winning on key collision.

    Deliberately last-write-wins on the Bronze merge key rather than
    newest-`ingestion_timestamp`-wins: a re-fetch is the authoritative
    value for that key by the time it reaches here, and resolving by
    timestamp would make the result depend on clock agreement between
    runs.
    """
    _, key_cols = _spec(source)
    by_key: Dict[tuple, dict] = {_key_of(r, key_cols): r for r in existing}
    for record in incoming:
        by_key[_key_of(record, key_cols)] = record
    return list(by_key.values())


def write_month(
    store, source: str, month_start: date, records: List[dict]
) -> int:
    """Merge `records` into `source`'s file for `month_start`. Returns rows written.

    "Rows written" is the number of incoming records, matching what the
    Delta writer reports for the same call, not the resulting file's row
    count. Returning the file total would make a run that re-fetched one
    country of an otherwise complete month claim the whole month's rows
    as its own, and a year's reported total would be a sum of file sizes
    rather than of anything ingested. The file total is logged instead.

    Writing nothing when `records` is empty leaves any existing file
    untouched — an attempt that produced no rows must not erase rows an
    earlier attempt landed.
    """
    if not records:
        logger.info("No %s records for %s; leaving existing file untouched", source, month_start)
        return 0

    key = store.key(month_key(source, month_start))
    existing_bytes = store.get(key)
    existing = from_parquet_bytes(source, existing_bytes) if existing_bytes else []
    merged = merge_records(source, existing, records)
    store.put(key, to_parquet_bytes(source, merged), content_type="application/vnd.apache.parquet")
    logger.info(
        "Wrote %s %s: %d incoming, %d total rows in file",
        source, month_start, len(records), len(merged),
    )
    return len(records)


def write_year(store, source: str, year: int, records: List[dict]) -> int:
    """Merge `records` into `source`'s file for `year`, for annual-grain sources.

    Returns rows written, on the same basis as `write_month`.
    """
    if not records:
        logger.info("No %s records for %s; leaving existing file untouched", source, year)
        return 0

    key = store.key(year_key(source, year))
    existing_bytes = store.get(key)
    existing = from_parquet_bytes(source, existing_bytes) if existing_bytes else []
    merged = merge_records(source, existing, records)
    store.put(key, to_parquet_bytes(source, merged), content_type="application/vnd.apache.parquet")
    logger.info(
        "Wrote %s %s: %d incoming, %d total rows in file",
        source, year, len(records), len(merged),
    )
    return len(records)
