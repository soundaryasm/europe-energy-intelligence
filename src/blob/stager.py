"""Staging the historical Parquet layer onto a Databricks volume (Spec 010).

The first half of the Bronze load. Every Parquet file the historical
ingestion landed in blob storage is copied, byte for byte, onto a Unity
Catalog volume. Nothing is parsed, nothing touches Spark, and only one
file is held in memory at a time.

Staging is separated from merging because the two fail differently.
This half is a sequence of small, independent HTTP downloads that can be
resumed at no cost; the merge is one expensive operation per period that
wants to be atomic. Running them as one task means a failure in either
costs the work of both.

A file is written under a `.part` suffix and renamed into place only once
it is complete, so a file present at its final path is always whole.
That is what makes "skip what is already staged" a safe resumption rule
rather than a way to inherit a truncated download.

Staged paths deliberately drop the `year=`/`month=` syntax the blob keys
use. Spark reads a directory tree of that shape as a Hive-partitioned
dataset and synthesises a `year` column from the directory name, which
would no longer match the Bronze schema. Plain `<year>/<month>.parquet`
segments cannot be mistaken for partitioning.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from src.blob import writer as blob_writer

logger = logging.getLogger(__name__)

# The default volume is a managed one in the workspace's own catalog:
# the bucket is not a Unity Catalog external location, so the data has
# to be copied in rather than referenced where it lies.
DEFAULT_VOLUME_ROOT = "/Volumes/workspace/default/historical"

PART_SUFFIX = ".part"

_MONTH_KEY = re.compile(r"/(?P<source>[a-z_]+)/year=(?P<year>\d{4})/month=(?P<month>\d{2})\.parquet$")
_YEAR_KEY = re.compile(r"/(?P<source>[a-z_]+)/year=(?P<year>\d{4})\.parquet$")

# Stands in for the month segment of an annual source, so every source
# stages to `<source>/<year>/<name>.parquet` and a batch is always one
# directory regardless of the source's grain.
ANNUAL_NAME = "year"

KNOWN_SOURCES = (
    blob_writer.SOURCE_ENTSOE,
    blob_writer.SOURCE_OPEN_METEO,
    blob_writer.SOURCE_WORLDBANK,
)


@dataclass
class StageResult:
    """What one staging pass copied onto the volume."""

    files_staged: int = 0
    files_skipped: int = 0
    bytes_staged: int = 0
    by_source: Dict[str, int] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return not self.errors


def volume_identifier(volume_root: str) -> str:
    """The `catalog.schema.volume` name behind a `/Volumes/...` path.

    Derived rather than configured separately so the path a job writes
    to and the volume it creates can never name different things.
    """
    parts = [p for p in volume_root.strip("/").split("/") if p]
    if len(parts) != 4 or parts[0] != "Volumes":
        raise ValueError(
            f"Not a volume root path: {volume_root!r} "
            "(expected /Volumes/<catalog>/<schema>/<volume>)"
        )
    return ".".join(parts[1:])


def parse_blob_key(key: str) -> Optional[Tuple[str, int, str]]:
    """Identify the source, year and name of a historical Parquet key.

    Returns `(source, year, name)`, where `name` is the zero-padded
    month for monthly sources and `ANNUAL_NAME` for annual ones, or
    `None` for a key that is not a data file — the manifest, most
    notably, which lives under the same prefix and must never be staged.
    """
    month = _MONTH_KEY.search(key)
    if month and month.group("source") in KNOWN_SOURCES:
        return month.group("source"), int(month.group("year")), month.group("month")
    year = _YEAR_KEY.search(key)
    if year and year.group("source") in KNOWN_SOURCES:
        return year.group("source"), int(year.group("year")), ANNUAL_NAME
    return None


def staged_relative_path(source: str, year: int, name: str) -> str:
    """Where one blob file is staged, relative to the volume root."""
    return f"{source}/{year:04d}/{name}.parquet"


def batch_directory(volume_root: str, source: str, year: int) -> str:
    """The directory holding one (source, year) batch."""
    return f"{volume_root.rstrip('/')}/{source}/{year:04d}"


def stage_file(store, key: str, volume_root: str) -> int:
    """Copy one blob object onto the volume. Returns bytes written.

    Returns 0 without writing if the file is already staged.
    """
    parsed = parse_blob_key(key)
    if parsed is None:
        raise ValueError(f"Not a historical data key: {key}")

    relative = staged_relative_path(*parsed)
    destination = f"{volume_root.rstrip('/')}/{relative}"
    if os.path.exists(destination):
        return 0

    data = store.get(key)
    if data is None:
        raise FileNotFoundError(f"Blob key disappeared between listing and read: {key}")

    os.makedirs(os.path.dirname(destination), exist_ok=True)
    partial = destination + PART_SUFFIX
    with open(partial, "wb") as handle:
        handle.write(data)
    # Renamed, not written in place: a file at its final path is then
    # always a complete one, which is what lets a resumed pass trust
    # what it finds.
    os.replace(partial, destination)
    return len(data)


def stage_all(
    store,
    volume_root: str = DEFAULT_VOLUME_ROOT,
    *,
    sources: Optional[Sequence[str]] = None,
) -> StageResult:
    """Copy every historical Parquet file onto the volume, oldest first.

    Already-staged files are skipped, so an interrupted pass resumes
    where it stopped rather than re-downloading a decade. A file that
    fails is recorded and the pass continues: one unreadable object must
    not stop the rest from staging.
    """
    wanted = set(sources) if sources else set(KNOWN_SOURCES)
    result = StageResult()

    ordered: List[Tuple[int, str, str, str]] = []
    for key in store.list_keys():
        parsed = parse_blob_key(key)
        if parsed is None:
            result.files_skipped += 1
            continue
        source, year, name = parsed
        if source not in wanted:
            result.files_skipped += 1
            continue
        ordered.append((year, name, source, key))

    for year, name, source, key in sorted(ordered):
        try:
            written = stage_file(store, key, volume_root)
        except Exception as exc:  # noqa: BLE001 - one bad object must not stop the pass
            logger.error("Failed to stage %s: %s", key, exc)
            result.errors[key] = str(exc)
            continue
        if written == 0:
            result.files_skipped += 1
            continue
        result.files_staged += 1
        result.bytes_staged += written
        result.by_source[source] = result.by_source.get(source, 0) + 1
        logger.info("Staged %s %s-%s (%.2f MB)", source, year, name, written / 1e6)

    logger.info(
        "Staging complete: %d staged, %d skipped, %.1f MB",
        result.files_staged, result.files_skipped, result.bytes_staged / 1e6,
    )
    return result
