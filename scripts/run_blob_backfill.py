#!/usr/bin/env python3
"""Resolve one year of history into blob storage (Spec 010).

Entry point for the scheduled historical ingestion. One invocation
resolves one year and exits; the schedule supplies the repetition. When
every month back to `HISTORICAL_START` is accounted for, the invocation
reports that and exits 0 without doing any work, so a schedule left
running after completion costs one short no-op rather than re-fetching.

Exit status:
    0  the year was resolved, or progressed with some combinations
       still to retry, or there was nothing left to resolve
    1  configuration or credentials are missing
    3  the year abandoned a combination permanently

A non-zero exit is reserved for the one outcome that needs a person:
a combination escalated to `given_up` will never be retried by the walk,
so that month stays short of data until someone intervenes.

Combinations merely left `failed` do not fail the invocation. They are
retried on the next one, and on a source that times out under load that
happens on most runs — reporting it as a failure made the normal case
indistinguishable from the serious one, which is precisely how the
serious one gets ignored. They are still logged as a warning, and the
run summary still names them.
"""
from __future__ import annotations

import logging
import os
import sys

# Resolve `src.*` imports when invoked as a script from the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.blob.manifest import GIVE_UP_AFTER_ATTEMPTS, Manifest
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

    given_up = result.given_up
    if given_up:
        logger.error(
            "%d combination(s) ABANDONED after %d attempts and will not be "
            "retried by the walk: %s",
            len(given_up),
            GIVE_UP_AFTER_ATTEMPTS,
            given_up,
        )
        logger.error(
            "Those months are permanently short of data. Re-check the cause, "
            "then return them to the walk with: scripts/reset_given_up.py "
            "--month <YYYY-MM>  (or --all)"
        )
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
