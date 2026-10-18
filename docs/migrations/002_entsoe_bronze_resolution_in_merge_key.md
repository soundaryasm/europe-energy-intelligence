# Migration 002: `source_resolution` joins the ENTSO-E Bronze merge key

**No DDL to run.** `source_resolution` is already a column on
`bronze_entsoe_energy`; what changed is that it is now part of
`ENTSOE_BRONZE_KEY_COLS`, the business key used by the pre-merge dedupe
and the Delta MERGE condition. The table's schema is untouched, so
`ensure_schema_compatible` has nothing to object to and no run will fail
on it.

## What changed and why

ENTSO-E publishes some series at more than one resolution for the same
instant, with different values. The day-ahead market moved to a
15-minute market time unit (SDAC, 30 September 2025), and an hourly
price and the price for the first quarter of that hour are two distinct
observations.

With resolution outside the key, those two rows collided on the MERGE
and one was discarded. Which one was arbitrary: every row in a batch
shares an `ingestion_timestamp`, so `dedupe_latest` had nothing to order
by. Measured on a single week of DE day-ahead price, 192 of 957 parsed
rows collapsed this way.

Resolution is now part of the key, so both are retained, and the choice
of which to use is made once in Silver
(`entsoe_silver_common.with_preferred_resolution`) rather than by whichever
row won a merge.

## State of data already in the table

Rows written before this change were merged under the old key. Where two
resolutions collided for one instant, only the arbitrary winner is
present; the other was never stored and cannot be recovered from the
table.

Nothing is corrupt — every stored row is a real observation, and the
values are correct for whichever resolution survived. The gap is a
missing sibling row, not a wrong one.

Re-ingesting a window fills those gaps: the new key lets both rows
insert, and the MERGE updates the surviving row in place rather than
duplicating it. So no cleanup step is required — the gaps close wherever
history is re-fetched, including the historical load
(`specs/010-historical-blob-backfill.md`), which re-fetches everything
from January 2015.

## Effect on Silver and Gold

Until a window is re-ingested, Silver sees one resolution per instant
for it and behaves exactly as before.

Once both are present, `with_preferred_resolution` keeps the hourly
series for that country-day and drops the finer one, so the
interval-duration-weighted daily aggregates cover 24 hours either way
and are never double-counted. Daily averages for affected days may shift
slightly, because the arbitrary winner is replaced by a deliberate
choice.

Series with no hourly option — Irish load and generation at PT30M,
German load and generation at PT15M — fall back to the finest resolution
present and are unaffected.
