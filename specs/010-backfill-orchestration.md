# 010 — Backfill Orchestration

## Goal

Backfill ~24 months (2015-01 onward) of ENTSO-E and Open-Meteo history without duplicating any ingestion business logic, and without ever advancing past a month that is only partially covered.

This supersedes Spec 006's "Historical Backfill"/"Backfill Chunking" sections with the concrete design actually implemented, once both are in tension treat this spec as authoritative for backfill specifically.

## Two Pipelines, Same Modules

- Daily freshness pipeline (Spec 006): unchanged, `02:00 Europe/Dublin`, has priority over backfill.
- Backfill pipeline: separate Databricks Job (`europe_energy_intelligence_backfill`), separate schedule, reuses `entsoe_pipeline.run_ingestion` / `open_meteo_pipeline.run_ingestion` unmodified. No duplicated ingestion logic.

## Whole-Month Completeness, Not Row Existence

A month must not be marked done merely because *a* record exists for it. Some (country, dataset) combinations legitimately have zero data for a whole period — that is not the same condition as an incomplete/partially-ingested month, and conflating the two risks silently walking backward past real gaps forever.

A `(source, country_code, dataset, month)` combination is classified, after ingestion:

- `success` — every calendar day in the month has real Bronze data.
- `unavailable` — zero data for the whole month **and** the ingestion run explicitly reported "no data" for every window attempted (ENTSO-E's `Acknowledgement_MarketDocument` no-data case). A confirmed legitimate absence, not a guess from an empty result alone.
- `failed` — anything else, including a *partial* month (some days present, some not). A partial month is never treated as good enough to advance past; it stays `failed`/eligible for retry on the next scheduled run.
- `given_up` — a combination that stayed `failed` for `GIVE_UP_AFTER_ATTEMPTS` (3) consecutive attempts without ever producing a clean `unavailable` acknowledgement. Deliberately distinct from `unavailable`: this means "we don't know and stopped asking," not "confirmed nothing is there." Exists so one persistently-erroring combination (e.g. a single country's data item broken by an upstream migration) can't block every older month forever — without this, `failed` retries indefinitely with no cap. Not retried automatically after this; the real data, if it later becomes available, still reaches Gold on the next daily Silver rebuild regardless of this table, since Silver reprocesses all of Bronze unconditionally every run — a manual, scoped re-fetch (e.g. `jobs submit` targeting just that country) is the intended way to recover a `given_up` combo, not automation.

Only `success`, `unavailable`, and `given_up` let the walker move to an older month.

Implementation: `src/orchestration/backfill_completeness.py` (`evaluate_month_coverage`, `classify_month_result`) — pure Python, compares actual Bronze-covered dates against the full calendar-day set for the month.

## Month Selection & Checkpointing

Progress is tracked in an explicit Delta table (`backfill_checkpoint`, schema in `delta_schema.backfill_checkpoint_schema`), one row per `(source, country_code, dataset, month_start)` — never inferred from MIN/MAX dates in Bronze/Silver, which cannot distinguish "not attempted yet" from "legitimately no data."

Columns: `month_start`, `status`, `attempt_count`, `started_at`, `completed_at`, `last_error`, `updated_at`.

Each job invocation processes exactly one calendar month: the newest month, walking backward from the previous complete month, where at least one expected combination is not yet `success`/`unavailable`. Implementation: `src/orchestration/backfill_checkpoint.next_backfill_month` / `month_is_complete`.

## Within-Month Chunking (ENTSO-E only)

ENTSO-E requests for a backfill month are split into ~7-day windows (`ENTSOE_BACKFILL_CHUNK_DAYS`), not the ~90-day default used for reprocess — reuses `entsoe_client.chunk_date_range` internally via `run_ingestion(..., chunk_days=7)`. No new chunking logic. Reasons: avoids oversized responses, cheaper/more isolated retries, plays naturally with idempotent Bronze MERGE.

Open-Meteo needs no such split — its monthly daily-data request is small.

## Month-Boundary Buffering (ENTSO-E only)

ENTSO-E requests a small buffer either side of the target month (`ENTSOE_MONTH_BUFFER_DAYS = 2`) so Silver can reconstruct complete local-timezone calendar dates at the edges. Buffer-only dates never count toward that month's own completeness check (`evaluate_month_coverage` restricts to the month's own calendar days). Overlap is harmless — Bronze MERGE is idempotent.

Open-Meteo requests the exact calendar month directly; its daily API is already timezone-aware per country.

## Self-Termination

Once every month back to `BACKFILL_HISTORICAL_START` (2015-01-01) is `success`/`unavailable` for every expected combination, the job pauses its own schedule (`pause_status: PAUSED` via the Databricks Jobs API) rather than continuing to fire every 2 hours indefinitely with nothing to do. The notebook reads the job's current full schedule (cron expression + timezone) before writing it back with only `pause_status` changed — the Jobs API does not merge nested fields on `update`, so submitting `schedule` with only `pause_status` set would silently drop the cron expression and timezone.

## Schedule

Every 2 hours, `01:00`–`23:00`, **UTC** (`0 0 1-23/2 * * ?`). UTC, not a DST-observing zone: Databricks documents that an interval-repeating schedule in a DST zone can skip or double-fire during the transition hour. This schedule has no local-calendar-alignment requirement (unlike the daily pipeline's single `02:00 Europe/Dublin` fire), so there is no reason to accept that risk.

## Concurrency

No preemptive capacity check before starting a month. Databricks Free Edition's 5-concurrent-task account-wide cap means a run can fail purely on capacity; that failure leaves the month `failed`, which the checkpoint already treats as retryable on the next scheduled run — the existing idempotent retry behavior handles this without needing a separate deferral mechanism.

Task-level `max_retries: 0` in the job resource, deliberately: an immediate retry against the same failure mode (e.g. an API outage, a capacity ceiling) wastes a concurrent-task slot for no benefit — the next scheduled run (2 hours later) is the retry.

## Out of Scope (this spec)

- Surfacing checkpoint/coverage status through Gold/dbt/Postgres (this covers only what the backfill walker itself needs to decide "is this month done").
- A preemptive concurrency/capacity check before starting a run.
- Country/dataset expansion beyond what's already configured (Spec 009 / `project_country_expansion_plan`).

## Acceptance Criteria

1. `backfill_checkpoint` table exists with one row per `(source, country_code, dataset, month_start)`.
2. A month with full calendar-day coverage for every expected combination is classified `success`.
3. A month with confirmed zero data for a whole combination (every window explicitly "no data") is classified `unavailable`, not `failed`.
4. A month with partial coverage for any combination is classified `failed` and is retried, never silently advanced past.
4a. A combination that fails `GIVE_UP_AFTER_ATTEMPTS` (3) consecutive times without ever producing a confirmed `unavailable` acknowledgement is classified `given_up`, unblocking the walker; it is not retried automatically afterward.
5. ENTSO-E requests within a backfill month use ~7-day chunks with a 2-day boundary buffer; only in-month dates count toward completeness.
6. Open-Meteo requests the exact calendar month, no buffer.
7. The job schedule runs every 2 hours, `01:00`–`23:00` UTC.
8. Once every month back to 2015-01-01 is complete, the job pauses its own schedule without dropping its cron expression or timezone.
9. Task-level retries are disabled (`max_retries: 0`); retry happens via the next scheduled invocation instead.
