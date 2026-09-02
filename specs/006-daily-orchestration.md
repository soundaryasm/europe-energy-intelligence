# 006 — Daily Orchestration

## Goal

Orchestrate the complete European Energy Intelligence pipeline as a scheduled Databricks workflow.

The production pipeline must run daily at approximately 02:00 and coordinate:

1. weather ingestion
2. ENTSO-E ingestion
3. Silver transformations
4. dbt Gold modelling
5. PostgreSQL publishing

Databricks-native orchestration must be used.

Do not use:

- local cron
- local scripts
- an always-on local machine
- external schedulers for the MVP

## Orchestration Platform

Use Databricks Lakeflow Jobs.

Databricks Jobs supports:

- scheduled triggers
- task dependencies
- retries
- conditional execution
- execution history

The workflow should be represented as a task DAG rather than one large notebook.

## Schedule

Run once daily at:

`02:00`

Use the timezone:

`Europe/Dublin`

Use a Databricks advanced scheduled trigger so the timezone is explicitly configured.

The schedule must not depend on the developer's local machine timezone.

## Daily Processing Date

A run at 02:00 should normally process the most recently completed calendar date for which source data is expected to be available.

The processing date must be passed explicitly between tasks where required.

Do not scatter logic such as:

`current_date - 1`

throughout multiple notebooks.

There should be one clearly defined processing-date strategy.

## Task DAG

The intended workflow is:

Open-Meteo ingestion
        │
        ├──────────────┐
        │              │
ENTSO-E ingestion      │
        │              │
        └──────┬───────┘
               ↓
      Silver transformations
               ↓
          dbt Gold models
               ↓
       PostgreSQL publishing

The two ingestion tasks may execute independently.

Silver processing must not start until all required ingestion tasks have completed successfully.

dbt must not execute until Silver processing succeeds.

PostgreSQL publishing must not execute until dbt succeeds.

## Suggested Task Names

Use clear task identifiers such as:

- `ingest_open_meteo`
- `ingest_entsoe`
- `transform_silver`
- `build_dbt_gold`
- `publish_postgres`

Task names should describe responsibilities rather than implementation details.

## Failure Behaviour

The default downstream condition should be equivalent to:

`ALL_SUCCESS`

If a required upstream task fails:

- dependent transformations must not execute
- dbt must not execute
- PostgreSQL must not publish partial or stale results as though the run succeeded

Failures must remain visible in Databricks.

## Retries

Transient operations should support bounded retries.

Retries are particularly appropriate for:

- external API requests
- temporary network failures
- PostgreSQL connectivity

Do not configure infinite retries.

A reasonable starting policy is:

- maximum retries: 2
- delay between retries: a few minutes

Exact retry values may be tuned after observing actual source behaviour.

Data-quality or deterministic code failures should not be hidden through excessive retries.

## Idempotency

Every task must be safe to rerun for the same processing date.

A failed workflow should allow:

- retrying an individual failed task where safe
- rerunning the full workflow

without creating duplicate logical records.

Idempotency requirements defined in the ingestion, Silver, dbt, and PostgreSQL specifications remain applicable.

## Backfill Execution

Historical backfill is separate from the normal daily schedule.

The same underlying ingestion and transformation code should support parameterized execution for:

- `start_date`
- `end_date`

Backfill should not require maintaining separate duplicated pipeline implementations.

The scheduled daily workflow must not accidentally trigger a full 24-month backfill.

## Parameters

Where practical, workflow tasks should accept explicit parameters.

At minimum support the concept of:

- processing date
- start date
- end date
- execution mode

Potential execution modes:

- `daily`
- `backfill`
- `reprocess`

Avoid tightly coupling notebooks to one hard-coded date strategy.

## Execution Environment

All workflow tasks MUST execute within Databricks.

The local machine is not part of runtime orchestration.

GitHub is the source of truth for code, but GitHub is not the scheduler for the MVP.

## Source Control

Production workflow code must reference code stored in the project's Databricks Git-backed project.

Changes should follow:

local editing
→ Git commit
→ GitHub
→ Databricks Git folder
→ Databricks execution

Do not require manually copying notebook contents between environments.

## Data Availability

The workflow must distinguish between:

- pipeline failure
- valid API response containing no data
- source data not yet available
- partially available source data

Do not automatically interpret an empty dataset as successful ingestion.

Where a required daily source has not yet published the expected data, the run should fail clearly or defer downstream processing according to explicitly implemented behaviour.

## Data Quality Gate

PostgreSQL publishing should only occur after required upstream validation succeeds.

At minimum, before serving publication:

- required countries must be represented where source data is available
- expected Gold models must exist
- dbt tests must pass
- logical-key uniqueness must pass
- required foreign-key relationships must pass

The serving layer should not receive data from a known-invalid Gold build.

## Observability

Databricks workflow execution must make it possible to determine:

- workflow start time
- workflow end time
- processing date
- task status
- task duration
- task retries
- failure reason
- overall workflow status

Individual tasks should additionally emit their domain-specific metrics as defined in earlier specifications.

## Logging

Logs must be useful enough to identify:

- which country failed
- which dataset failed
- which date range was requested
- which stage failed

Logs must never expose:

- ENTSO-E API tokens
- PostgreSQL passwords
- complete secret-bearing connection strings

## Manual Execution

The workflow must support manual execution from Databricks for:

- development
- testing
- reprocessing a date
- debugging

A scheduled trigger must not be the only way to execute the pipeline.

## Recovery

If the workflow fails before PostgreSQL publishing:

- Gold/serving state from the previous successful run remains valid
- the failed date can be rerun

If PostgreSQL publishing fails:

- Databricks Gold remains authoritative
- rerunning the publish task must safely reconcile PostgreSQL

Do not require deleting tables manually to recover from normal pipeline failures.

## Concurrency

The MVP does not require concurrent executions of the same daily workflow.

Avoid overlapping runs for the same processing period where possible.

If a previous daily run is still executing, the design should avoid producing conflicting writes for the same logical data.

## Notifications

Notifications are optional for the MVP.

If enabled later, useful notifications include:

- workflow failure
- repeated task failure
- PostgreSQL publish failure

Notification infrastructure must not become a prerequisite for pipeline execution.

## Acceptance Criteria

This specification is complete when:

1. A Databricks Job represents the full pipeline.
2. The workflow runs daily at approximately 02:00 Europe/Dublin time.
3. Open-Meteo and ENTSO-E ingestion are separate tasks.
4. Task dependencies enforce correct execution ordering.
5. Silver does not run when required ingestion fails.
6. dbt does not run when Silver fails.
7. PostgreSQL publishing does not run when dbt/tests fail.
8. Transient failures have bounded retry behaviour.
9. The workflow supports manual execution.
10. Historical backfill can reuse the same pipeline code.
11. Processing dates are parameterized rather than duplicated across notebooks.
12. Rerunning a processing date remains idempotent.
13. Execution history and failure information are visible in Databricks.
14. No local or external scheduler is required.

## Out of Scope

This specification does not include:

- Apache Airflow
- GitHub Actions scheduling
- streaming pipelines
- event-driven triggers
- complex branching workflows
- SLA monitoring platforms
- PagerDuty/Slack alerting
- production multi-environment deployment