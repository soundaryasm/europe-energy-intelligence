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

    missing = staged.select(*key_cols).join(
        bronze.select(*key_cols), on=list(key_cols), how="left_anti",
    )
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
