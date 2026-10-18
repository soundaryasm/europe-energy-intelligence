#!/usr/bin/env python3
"""Resolve one year of history into blob storage (Spec 010).

Entry point for the scheduled historical ingestion. One invocation
resolves one year and exits; the schedule supplies the repetition. When
every month back to `HISTORICAL_START` is accounted for, the invocation
reports that and exits 0 without doing any work, so a schedule left
running after completion costs one short no-op rather than re-fetching.

Exit status:
    0  a year was resolved, or there was nothing left to resolve
    1  configuration or credentials are missing
    2  the year was attempted but left combinations unresolved

Exit 2 is deliberately distinct from exit 0: an unresolved combination is
expected (it is retried on the next invocation, and escalates to
`given_up` after three attempts) but it should still be visible as a
non-success in the run history rather than reported as a clean run.
"""
from __future__ import annotations

import logging
import os
import sys

# Resolve `src.*` imports when invoked as a script from the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.blob.manifest import Manifest
from src.blob.runner import run_year
from src.blob.store import BlobConfigError, BlobStore

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("run_blob_backfill")


def main() -> int:
    token = os.environ.get("ENTSOE_API_TOKEN")
    if not token:
        logger.error("ENTSOE_API_TOKEN is not set")
        return 1

    try:
        store = BlobStore.from_env()
    except BlobConfigError as exc:
        logger.error("%s", exc)
        return 1

    manifest = Manifest.load(store)
    result = run_year(store, token=token, manifest=manifest)

    if result.complete:
        logger.info("Historical ingestion is complete; nothing to do")
        logger.info("Manifest summary: %s", manifest.summary())
        return 0

    resolved = sum(1 for month in result.months if not month.skipped)
    logger.info(
        "Resolved %s: %d month(s) attempted, %d ENTSO-E rows, %d weather rows",
        result.year,
        resolved,
        sum(month.entsoe_rows for month in result.months),
        sum(month.open_meteo_rows for month in result.months),
    )
    logger.info("Manifest summary: %s", manifest.summary())

    unresolved = result.unresolved
    if unresolved:
        logger.warning(
            "%d combination(s) unresolved, retried on the next invocation: %s",
            len(unresolved),
            unresolved,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
