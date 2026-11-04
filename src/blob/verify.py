"""Verifying that the staged history actually reached Bronze (Spec 010).

The load reports what it merged. This checks the result independently,
by asking the only question that matters: is there a row on the volume
whose merge key is absent from Bronze?

That is a left-anti join, not a row count. Counts cannot answer it —
Bronze also holds rows the daily pipeline wrote, so it is legitimately
larger than the staged set, and two numbers failing to match proves
nothing in either direction. A missing key is unambiguous.

One join per source rather than one per year: the staged files are read
as a single dataset, so Bronze is scanned once per source instead of
once per year.

This runs before Silver and Gold are rebuilt, and is the reason the
Bronze load is its own job. A decade of Silver and a full dbt build are
the most expensive run in the project, and they should not start on a
Bronze that is quietly short of a year.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from src.blob import loader, stager

logger = logging.getLogger(__name__)

# Enough to identify what went missing without printing a dataset.
SAMPLE_LIMIT = 5


@dataclass
class SourceVerification:
    """The verdict for one source."""

    source: str
    staged_rows: int = 0
    bronze_rows: int = 0
    missing_rows: int = 0
    samples: List[dict] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.missing_rows == 0

    def describe(self) -> str:
        if self.error:
            return f"{self.source}: ERROR {self.error}"
        verdict = "OK" if self.ok else f"{self.missing_rows:,} MISSING"
        return (
            f"{self.source}: staged={self.staged_rows:,} "
            f"bronze={self.bronze_rows:,} -> {verdict}"
        )


def anti_join_on_keys(staged, bronze, key_cols: Sequence[str]):
    """Staged rows whose merge key has no counterpart in `bronze`.

    Keys are compared the way the Bronze MERGE compares them
    (`delta_schema._merge_condition`): `coalesce(col, '')` on both
    sides, over the column cast to string.

    Not an equi-join on the key columns, which is what this was
    originally and which is wrong: SQL `NULL = NULL` is false, so every
    row with a nullable key column unset matches nothing and is
    reported absent. `production_type_raw` is NULL for every load and
    price row — 5.6M of them — so verification failed against a Bronze
    table that was in fact complete. A check that cannot tell "absent"
    from "has a NULL in the key" is worse than no check.

    The cast matters for World Bank's integer `year`. `coalesce` rather
    than `eqNullSafe` keeps NULL and the empty string equivalent, which
    is what the MERGE already treats as one key.

    Kept as its own function because it is the one part of this module
    with genuine Spark semantics: the fake-based tests cannot model
    SQL NULL comparison, so they stub this out and
    `test_blob_verify_spark.py` covers it against a real session.
    """
    from pyspark.sql import functions as F

    staged_keys = staged.select(*key_cols).alias("s")
    bronze_keys = bronze.select(*key_cols).alias("b")
    condition = None
    for col in key_cols:
        same = F.coalesce(F.col(f"s.{col}").cast("string"), F.lit("")) == F.coalesce(
            F.col(f"b.{col}").cast("string"), F.lit("")
        )
        condition = same if condition is None else (condition & same)
    return staged_keys.join(bronze_keys, on=condition, how="left_anti")


def verify_source(
    spark, source: str,
    volume_root: str = stager.DEFAULT_VOLUME_ROOT,
    *,
    table_prefix: str = "",
) -> SourceVerification:
    """Check that every staged row for `source` has a counterpart in Bronze."""
    schema, key_cols = loader._bronze_schema_and_keys(source)
    table = loader.bronze_table(source, table_prefix)
    verification = SourceVerification(source=source)

    # Every year at once. `recursiveFileLookup` is set explicitly rather
    # than relying on Spark's default file discovery: it both guarantees
    # the nested year directories are descended into and disables
    # partition inference outright, so a directory name can never be
    # read as a column value.
    staged = (
        spark.read.option("recursiveFileLookup", "true")
        .parquet(f"{volume_root.rstrip('/')}/{source}")
    )
    verification.staged_rows = staged.count()

    if not spark.catalog.tableExists(table):
        verification.error = f"{table} does not exist"
        return verification

    bronze = spark.table(table)
    verification.bronze_rows = bronze.count()

    missing = anti_join_on_keys(staged, bronze, key_cols)
    verification.missing_rows = missing.count()
    if verification.missing_rows:
        verification.samples = [
            row.asDict() for row in missing.limit(SAMPLE_LIMIT).collect()
        ]
    return verification


def verify_all(
    spark,
    volume_root: str = stager.DEFAULT_VOLUME_ROOT,
    *,
    sources: Optional[Sequence[str]] = None,
    table_prefix: str = "",
) -> List[SourceVerification]:
    """Verify every source that has staged files, reporting rather than raising.

    A source that cannot be verified is reported as an error alongside
    the others, so one broken table does not hide the verdict on the
    rest.
    """
    wanted = sources if sources else stager.KNOWN_SOURCES
    verdicts: List[SourceVerification] = []
    for source in wanted:
        staged_batches = loader.discover_batches(volume_root, sources=[source])
        if not staged_batches:
            logger.info("Nothing staged for %s; skipping", source)
            continue
        try:
            verdict = verify_source(spark, source, volume_root, table_prefix=table_prefix)
        except Exception as exc:  # noqa: BLE001 - one source must not hide the rest
            verdict = SourceVerification(source=source, error=f"{type(exc).__name__}: {exc}")
        logger.info(verdict.describe())
        verdicts.append(verdict)
    return verdicts
