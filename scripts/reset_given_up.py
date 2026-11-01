#!/usr/bin/env python3
"""Return given-up combinations to the historical walk (Spec 010).

A combination that failed three times is recorded as `given_up`, which
counts as done so one persistently failing country cannot hold up every
older month. It stays a distinct status so it can be found and
re-attempted once the underlying cause is believed resolved — that
re-attempt is what this script arranges, by resetting those entries to
`failed`.

Resetting clears the attempt count as well as the status. Leaving the
count in place would escalate the entry straight back to `given_up` on
its first new failure, which would make the reset pointless.

With no arguments it reports what is currently given up and changes
nothing, so the normal first step is to look before resetting.

    scripts/reset_given_up.py                      report only
    scripts/reset_given_up.py --all                reset everything given up
    scripts/reset_given_up.py --month 2025-12      reset one month
    scripts/reset_given_up.py --source open_meteo  reset one source
    scripts/reset_given_up.py --country IE --dataset load --include-unavailable

`unavailable` means the source said it has nothing, which is normally
believed and left alone. It is resettable too, because that answer can
be an artefact of how we asked: Ireland's load was recorded unavailable
for twelve months while the data sat under a domain code the config did
not use. Pass `--include-unavailable` only when something has changed
that would make the source answer differently.

Exit status:
    0  reported, or reset what was asked for
    1  configuration or credentials are missing
    2  a filter was given that matched nothing
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.blob.manifest import STATUS_GIVEN_UP, STATUS_UNAVAILABLE, Manifest
from src.blob.store import BlobConfigError, BlobStore

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("reset_given_up")


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--all", action="store_true", help="reset every given-up combination")
    parser.add_argument("--month", help="restrict to one month, as YYYY-MM")
    parser.add_argument("--source", help="restrict to one source, e.g. entsoe or open_meteo")
    parser.add_argument("--country", help="restrict to one country code")
    parser.add_argument("--dataset", help="restrict to one dataset, e.g. load or price")
    parser.add_argument(
        "--include-unavailable", action="store_true",
        help="also reset combinations the source reported as having no data",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)

    try:
        store = BlobStore.from_env()
    except BlobConfigError as exc:
        logger.error("%s", exc)
        return 1

    manifest = Manifest.load(store)
    statuses = [STATUS_GIVEN_UP]
    if args.include_unavailable:
        statuses.append(STATUS_UNAVAILABLE)
    entries = manifest.with_status(*statuses)
    label = "resettable" if args.include_unavailable else "given up"

    if not entries:
        logger.info("Nothing is %s", label)
        return 0

    selected = [
        (month, combo)
        for month, combo in entries
        if (args.month is None or month == args.month)
        and (args.source is None or combo.split(":")[0] == args.source)
        and (args.country is None or combo.split(":")[1] == args.country)
        and (args.dataset is None or combo.split(":")[2] == args.dataset)
    ]

    grouped = defaultdict(list)
    for month, combo in selected:
        grouped[month].append(combo)

    filtered = any(
        (args.month, args.source, args.country, args.dataset)
    )
    if not (args.all or filtered):
        logger.info("%d combination(s) %s:", len(entries), label)
        for month, combo in entries:
            logger.info("  %s  %s", month, combo)
        logger.info(
            "Re-run with --all, or --month/--source/--country/--dataset, to reset them",
        )
        return 0

    if not selected:
        logger.error("No %s combination matched that filter", label)
        return 2

    reset = 0
    for month, combos in sorted(grouped.items()):
        year, mon = (int(part) for part in month.split("-"))
        reset += manifest.reset(date(year, mon, 1), combos)
        logger.info("Reset %s: %s", month, combos)

    manifest.save(store)
    logger.info(
        "Reset %d combination(s); the next invocation will re-attempt them", reset,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
