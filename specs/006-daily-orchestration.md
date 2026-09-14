# 006 — Daily Orchestration

## Goal

Orchestrate the complete European Energy Intelligence pipeline as a scheduled Databricks workflow.

The production pipeline must run daily at approximately 02:00 Europe/Dublin time and coordinate:

1. Open-Meteo ingestion
2. ENTSO-E ingestion
3. Silver transformations
4. dbt Gold modelling and tests
5. PostgreSQL publication

Databricks-native orchestration must be used.

Do not use:

- local cron
- GitHub Actions scheduling
- Airflow
- an always-on local machine
- any external scheduler for the MVP

## Orchestration Platform

Use Databricks Lakeflow Jobs.

Lakeflow Jobs should manage:

- scheduled execution
- task dependencies
- retries
- parameters
- run history
- failure visibility

The workflow must be represented as multiple dependent tasks rather than one large notebook.

## Schedule

Run once daily at:

`02:00`

Timezone:

`Europe/Dublin`

Configure the schedule explicitly in Databricks.

Do not rely on the developer machine's timezone.

## Processing Strategy

The daily workflow does not treat every source identically.

### Open-Meteo

Reprocess a rolling recent-date window, not only the latest completed weather date (added after empirical testing found the Historical/Archive API's ERA5-reanalysis data has a real settlement lag for very recent dates, and the same date's value differs depending on which Open-Meteo endpoint is queried).

Initial lookback: `3 calendar days` (same length as ENTSO-E's, by explicit decision — not required to always match).

Normal daily execution queries the **Forecast API**, not the Historical/Archive API, for this recent-date window: the Forecast API's operational models refresh every 1-6 hours, while the Historical/Archive API's ERA5 reanalysis settles over roughly 2 days. Backfill and reprocess of older, already-settled dates use the Historical/Archive API as before — the lag is a non-issue for dates that far in the past.

The lookback length must be configurable, same requirement as ENTSO-E's.

## Why Open-Meteo Uses a Lookback

A value fetched from the Forecast API for a very recent date reflects whichever operational model run was most recent at fetch time. A later run (or the eventual ERA5-settled value) can differ from it — confirmed empirically: the same date/location returned different `temperature_2m_mean`/`wind_speed_10m_mean`/`shortwave_radiation_sum` values from the Forecast API versus the Historical/Archive API.

Therefore the system must not assume:

`date successfully ingested once = date's weather value final`

Recent-date reprocessing (the lookback) lets a later, more accurate model run overwrite an earlier one via the existing idempotent Bronze MERGE — the same mechanism ENTSO-E's lookback already relies on.

**User-facing consequence, document this clearly wherever the Gold/dashboard data is described:** a weather (and, separately, an ENTSO-E) metric for a very recent date can show a small delta if viewed again a few days later, once a subsequent pipeline run has re-fetched and replaced the earlier value. This is expected behavior, not a data-quality defect — it is the tradeoff of prioritizing eventual accuracy over freezing the first-seen value permanently.

### ENTSO-E

Reprocess a rolling recent-date window rather than requesting only one immutable previous day.

Initial MVP lookback:

`3 calendar days`

For example, a run on September 4 may re-request:

- September 1
- September 2
- September 3

This allows the pipeline to capture:

- revised ENTSO-E values
- previously missing intervals
- late-arriving source data
- previously partial days that later become complete

The lookback length must be configurable.

Do not permanently hard-code `3` throughout application code.

## Why ENTSO-E Uses a Lookback

ENTSO-E observations may be revised after initial publication.

A successful response can also contain partial timeline coverage.

Therefore the system must not assume:

`date successfully ingested once = date permanently complete`

Recent-date reprocessing is part of normal pipeline behaviour.

## Canonical Processing Window

The workflow should derive a single canonical processing window and pass it to downstream tasks.

Recommended parameters:

- `processing_date`
- `start_date`
- `end_date`
- `execution_mode`

Do not independently calculate date ranges inside every task.

## Execution Modes

Support at least:

### daily

Normal scheduled execution.

Uses the configured recent-date lookback.

### backfill

Processes an explicitly supplied historical range.

Used for the initial 24-month history and controlled historical reloads.

### reprocess

Processes an explicitly supplied date or date range for correction/debugging.

The same underlying implementation should support all three modes.

Do not maintain separate duplicate pipeline implementations.

## Task DAG

The intended workflow is:

```text
          ingest_open_meteo
                  │
                  ├─────────────┐
                  │             │
          ingest_entsoe         │
                  │             │
                  └──────┬──────┘
                         ↓
                 transform_silver
                         ↓
                    build_dbt_gold
                         ↓
                  publish_postgres
```

Open-Meteo and ENTSO-E ingestion may run independently or in parallel.

`transform_silver` must wait for all required ingestion tasks.

`build_dbt_gold` must wait for successful Silver processing.

`publish_postgres` must wait for successful dbt execution and required tests.

## Suggested Task Names

Use clear identifiers:

- `ingest_open_meteo`
- `ingest_entsoe`
- `transform_silver`
- `build_dbt_gold`
- `publish_postgres`

Names should describe responsibilities rather than filenames or implementation details.

## Task Entry Points

Databricks notebooks should remain thin task entry points.

Reusable implementation belongs under:

`src/`

Workflow notebooks/tasks should primarily:

- receive parameters
- initialize configuration
- invoke reusable project code
- expose execution results

Do not place the complete pipeline implementation directly inside orchestration notebooks.

## Dependency Conditions

Required downstream tasks should use success-based dependency behaviour equivalent to:

`ALL_SUCCEEDED`

If required ingestion fails:

- Silver must not run

If Silver fails:

- dbt must not run

If dbt execution or required dbt tests fail:

- PostgreSQL publishing must not run

Do not publish a new serving state after a known-invalid upstream run.

## Partial Source Data

Partial source availability requires more nuance than a technical task failure.

For example, ENTSO-E ingestion may technically succeed but classify a country/date as:

`partial`

That does not necessarily mean the entire ingestion task must fail.

Instead:

1. Bronze retains the source data.
2. completeness status remains visible.
3. Silver prevents incomplete daily metrics from becoming trusted values.
4. Gold preserves correct null semantics.
5. later lookback execution may recover the missing data.

Technical pipeline success and analytical completeness are separate concepts.

## Required Country Availability

Do not fail the entire workflow merely because one country has legitimately unavailable source data unless the relevant quality rule classifies that absence as critical.

The workflow must distinguish:

- source unavailable
- source partial
- API failure
- malformed data
- complete data

These distinctions must remain visible downstream.

## Retries

Configure bounded retries for tasks vulnerable to transient failures.

Examples:

- external API calls
- temporary networking problems
- PostgreSQL connectivity

A reasonable initial task retry configuration is:

- maximum retries: 2
- retry interval: a few minutes

Exact settings may be adjusted after observing actual runtime behaviour.

Do not use excessive retries to hide deterministic failures such as:

- invalid XML parsing
- broken schemas
- failed business validations
- coding errors

## API-Level Retries

Task-level retries do not replace API-client retry behaviour.

API ingestion may additionally implement small bounded request-level retries for individual transient HTTP failures.

Avoid retry multiplication that could produce excessive requests.

Both retry layers must remain bounded.

## Idempotency

Every task must be safe to rerun for the same processing range.

This includes:

- API ingestion
- Bronze persistence
- Silver transformations
- dbt Gold models
- PostgreSQL publication

Retries or manual reruns must not create duplicate logical records.

## Historical Backfill

Historical backfill must reuse the same source-specific ingestion and transformation code used by the daily workflow.

The initial backfill covers:

`previous 24 months`

Backfill should be explicitly invoked using:

`execution_mode = backfill`

with supplied:

- `start_date`
- `end_date`

The normal 02:00 scheduled run must never accidentally trigger a full historical backfill.

## Backfill Chunking

Large historical source ranges should be processed in bounded chunks as defined by the ingestion specifications.

Orchestration may iterate through:

- months
- weeks
- another approved bounded period

depending on API behaviour.

A backfill failure should make the failed range identifiable and rerunnable.

## Reprocessing

Manual reprocessing must support:

- one country/date
- multiple countries
- one explicit date
- an explicit date range

where practical.

The implementation should make targeted recovery possible without requiring a complete historical reload.

## Data Quality Gate

PostgreSQL publication must occur only after required validation succeeds.

At minimum, the serving gate should require:

- Silver processing succeeded
- Gold models built successfully
- required dbt tests passed
- documented logical grains remain unique
- required relationships are valid

Known-invalid Gold models must not reach PostgreSQL.

## Freshness Semantics

A workflow run completing successfully does not automatically mean every metric became fresher.

The pipeline must distinguish:

- workflow execution timestamp
- latest available source date
- latest complete analytical date
- latest PostgreSQL publication timestamp

These concepts must not be treated as interchangeable.

## PostgreSQL Publication Window

The serving task should publish all dates affected by the current processing window.

For a normal ENTSO-E 3-day lookback, this may mean reconciling Gold records for all affected dates rather than only publishing yesterday.

This enables:

- corrections
- revisions
- completion of previously partial data

## Failure Behaviour

If a required task fails:

- downstream dependent tasks must not execute
- the failure must remain visible
- previously valid serving data must remain usable
- rerunning the failed processing window must be safe

Do not automatically delete previously valid PostgreSQL data because a new workflow run failed.

## PostgreSQL Failure

If PostgreSQL publication fails:

- Databricks Gold remains authoritative
- Gold data must not be rolled back
- serving publication can be retried
- duplicate serving records must not be created

## Manual Execution

The workflow must support manual execution from Databricks.

Manual execution is required for:

- development
- debugging
- backfill
- reprocessing
- validating configuration changes

The scheduled trigger must not be the only execution mechanism.

## Concurrency

Avoid overlapping executions of the same production workflow where possible.

The MVP does not require parallel workflow runs for the same processing window.

If a previous scheduled execution is still active, avoid creating competing writes for the same logical dates.

Use Databricks concurrency controls appropriate to the implementation.

## Source Control

GitHub remains the source of truth for code.

Expected workflow:

```text
local editing / coding agent
        ↓
      Git commit
        ↓
       GitHub
        ↓
Databricks Git folder
        ↓
    Lakeflow Job
```

Do not manually copy production code between environments.

## Runtime Dependencies

Each task must use the dependency strategy defined by its specification.

Python/PySpark tasks use the approved Databricks environment/project dependencies.

The dbt task uses its separately pinned:

`dbt-databricks`

runtime configuration.

Do not make the workflow depend on a local `.venv`.

## Secrets

All production secrets must be resolved at Databricks runtime.

Examples:

- ENTSO-E token
- Aiven PostgreSQL password

Secrets must never be passed as visible job parameters.

## Observability

Each workflow run must make it possible to determine:

- workflow start time
- workflow end time
- execution mode
- processing window
- ENTSO-E lookback range
- individual task status
- task duration
- retries performed
- failure reason
- overall workflow status

Source tasks should additionally expose their source-specific metrics.

## Logging

Logs should make failures traceable to:

- source
- country
- dataset
- requested date/range
- pipeline stage

Logs must never expose:

- ENTSO-E tokens
- PostgreSQL passwords
- secret-bearing URLs
- secret values

## Run History

Databricks execution history must remain sufficient to inspect:

- successful runs
- failed runs
- retries
- manual runs
- backfills
- reprocessing runs

Do not build a separate orchestration database for the MVP.

## Notifications

Notifications are optional for the MVP.

Potential future notifications include:

- workflow failure
- repeated API failure
- failed dbt tests
- PostgreSQL publication failure

Notification infrastructure must not become a prerequisite for successful pipeline execution.

## Acceptance Criteria

This specification is complete when:

1. The complete pipeline is represented as a Databricks Lakeflow Job.
2. The workflow runs daily at approximately 02:00 Europe/Dublin time.
3. Open-Meteo and ENTSO-E ingestion are separate tasks.
4. ENTSO-E normal daily execution uses a configurable recent-date lookback.
5. The initial ENTSO-E lookback is three calendar days.
6. Open-Meteo normal daily execution uses a configurable recent-date lookback (initially three calendar days) against the Forecast API, not the Historical/Archive API; backfill/reprocess of older dates use the Historical/Archive API.
6a. The possibility of a small delta appearing later for a recent date's weather (or ENTSO-E) metric, due to lookback reprocessing, is documented for anyone consuming the Gold/dashboard data — not left as a silent, unexplained behavior.
7. A canonical processing window is passed between tasks.
8. Daily, backfill, and reprocess execution modes are supported.
9. Task dependencies enforce correct execution order.
10. Silver does not run after required ingestion task failure.
11. dbt does not run after Silver failure.
12. PostgreSQL does not publish after failed required dbt tests.
13. Partial ENTSO-E data remains distinguishable from technical pipeline failure.
14. Transient failures use bounded retries.
15. Every pipeline stage remains idempotent.
16. Historical backfill reuses the same core implementation.
17. Manual date-range reprocessing is supported.
18. Affected recent dates are republished when source revisions occur.
19. Execution state and failures are visible through Databricks.
20. No local or external scheduler is required.
21. No local runtime dependency exists.

## Out of Scope

This specification does not include:

- Apache Airflow
- GitHub Actions scheduling
- external cron
- streaming
- continuous processing
- event-driven ingestion
- complex workflow branching
- enterprise SLA tooling
- PagerDuty/Slack alerting
- multi-environment deployment