# 005 — PostgreSQL Serving Layer

## Goal

Publish curated Gold datasets from Databricks into Aiven PostgreSQL for downstream consumption by Power BI.

PostgreSQL is the serving layer only.

Databricks remains the analytical system of record.

## Architecture

Databricks Gold
→ Aiven PostgreSQL
→ Power BI

PostgreSQL must not contain:

- Bronze datasets
- Silver datasets
- raw API responses
- raw ENTSO-E interval data
- intermediate transformation datasets

Only approved serving-ready Gold models should be published.

## Source Models

Publish only:

- `dim_country`
- `dim_date`
- `fact_energy_daily`
- `fact_weather_daily`
- `fact_generation_mix_daily`

These models are defined by Spec 004 and form the serving contract.

PostgreSQL must not recreate Gold-layer business logic.

## Target Database

Use Aiven PostgreSQL.

The connection must be established from Databricks.

The local machine is not part of the production data path.

## Execution Environment

Publishing MUST execute on Databricks.

Python may be used for:

- PostgreSQL connectivity
- transaction management
- batch writes
- upsert operations
- serving validation

PySpark may be used to read and prepare Gold Delta datasets before publishing.

Do not require:

- local PostgreSQL
- local pipeline execution
- manually exported CSV files
- manual database imports

## PostgreSQL Client

Use the approved PostgreSQL Python client from the project dependencies:

`psycopg`

Connection management must support:

- SSL as required by Aiven
- explicit connection timeout
- clean connection closure
- transactions
- batch-oriented writes

Do not open one database connection per row.

## Credentials

Database credentials must be retrieved securely at runtime.

Never commit:

- hostname credentials
- database passwords
- secret-bearing connection strings
- certificates containing private credentials
- `.env` production credentials

Databricks-supported secret management must be used for the production runtime.

Credentials must never appear in logs or exception output.

## Serving Schema

Create the following serving tables.

### dim_country

Primary key:

`country_key`

Required fields:

- `country_key`
- `country_code`
- `country_name`
- `reference_location`
- `timezone`
- `entsoe_domain`

Expected uniqueness:

- `country_key`
- `country_code`

### dim_date

Primary key:

`date_key`

Required fields:

- `date_key`
- `date`
- `year`
- `quarter`
- `month`
- `month_name`
- `day_of_month`
- `day_of_week`
- `day_name`
- `week_of_year`
- `is_weekend`

Expected uniqueness:

- `date_key`
- `date`

### fact_energy_daily

Logical key:

`country_key + date_key`

Required fields:

- `country_key`
- `date_key`
- `daily_demand_mwh`
- `avg_day_ahead_price_eur_mwh`
- `min_day_ahead_price_eur_mwh`
- `max_day_ahead_price_eur_mwh`
- `total_generation_mwh`
- `renewable_generation_mwh`
- `renewable_generation_pct`

### fact_weather_daily

Logical key:

`country_key + date_key`

Required fields:

- `country_key`
- `date_key`
- `avg_temperature_c`
- `avg_wind_speed_kmh`
- `solar_radiation_mj_m2`
- `reference_location`

### fact_generation_mix_daily

Logical key:

`country_key + date_key + production_type`

Required fields:

- `country_key`
- `date_key`
- `production_type`
- `generation_mwh`
- `renewable_flag`
- `generation_share_pct`

## Gold Contract

PostgreSQL must preserve the semantics established in Gold.

In particular:

- missing data must remain missing
- valid zero values must remain zero
- negative electricity prices must remain valid
- incomplete source data must not be represented as complete analytical data

Do not use serving-layer transformations such as:

`COALESCE(metric, 0)`

merely to remove null values.

PostgreSQL is not responsible for fixing incomplete upstream data.

## Trusted Data

Only validated Gold models may be published.

Gold builds that fail required dbt tests or upstream quality gates must not overwrite the previous valid serving state.

A known-invalid pipeline run must not be presented to Power BI as a successful refresh.

## Initial Load

The initial serving load must publish the complete approved 24-month Gold history.

The initial load must be rerunnable safely.

Do not depend on:

- manual table deletion
- manual database resets
- blind append behaviour

## Daily Incremental Load

After the initial load, normal publication should process only affected dates.

Affected dates may include:

- newly completed dates
- recently reprocessed dates
- ENTSO-E revisions
- dates that were previously incomplete and later became complete

The serving process must therefore support both inserts and updates.

## Lookback and Reprocessing

The orchestration layer may intentionally reprocess recent dates.

PostgreSQL publication must reconcile these records rather than assuming every date is immutable after its first publication.

Example:

A date initially contains:

`daily_demand_mwh = null`

because trusted demand data was incomplete.

If ENTSO-E later provides complete data, rerunning the pipeline must allow that same logical fact to be updated with the trusted value.

## Upsert Strategy

Use deterministic keys.

Expected behaviour:

- target row does not exist → INSERT
- target row exists and values changed → UPDATE
- target row exists and values are unchanged → leave logically unchanged

PostgreSQL `INSERT ... ON CONFLICT ... DO UPDATE` or an equivalent transactional upsert strategy may be used.

Do not use blind append operations for serving facts.

## Batch Publishing

Rows should be published in batches.

Avoid:

- one INSERT per independent connection
- one transaction per row
- extremely chatty database access

The MVP dataset is small, so favor simple, reliable batch writes over complex bulk-loading infrastructure.

## Transaction Behaviour

Publishing must avoid leaving the serving layer in an inconsistent partial state.

Dimensions must be available before dependent facts are committed.

Where practical, use transactions for logically related operations.

If a transaction fails:

- roll it back
- surface the failure
- leave previously valid serving data intact
- allow safe rerun

## Dimensions

`dim_country` is small and may be synchronized fully on each relevant serving run.

`dim_date` may also be synchronized using a simple deterministic strategy.

Dimension synchronization must not change deterministic keys between executions.

## Facts

Fact tables should normally be synchronized incrementally by affected date range.

Do not truncate and reload the complete 24-month history every day unless there is a documented reason.

The daily workflow should remain efficient even as history grows.

## Referential Integrity

Fact records must reference valid:

- `country_key`
- `date_key`

Dimension data must be published before dependent fact data.

PostgreSQL foreign-key constraints may be used where appropriate.

Regardless of whether physical foreign-key constraints are enabled, referential integrity must be validated.

Orphan fact rows are not permitted.

## Primary and Unique Constraints

The database should enforce the logical grain where practical.

Expected constraints:

### dim_country

`PRIMARY KEY (country_key)`

`UNIQUE (country_code)`

### dim_date

`PRIMARY KEY (date_key)`

`UNIQUE (date)`

### fact_energy_daily

`UNIQUE (country_key, date_key)`

### fact_weather_daily

`UNIQUE (country_key, date_key)`

### fact_generation_mix_daily

`UNIQUE (country_key, date_key, production_type)`

These constraints provide additional protection against accidental duplicate serving records.

## Indexing

Create indexes only for realistic Power BI access patterns.

Useful access patterns include:

- date filtering
- country filtering
- country + date filtering
- production-type filtering

Primary and unique constraints already provide useful indexes.

Do not over-index the small MVP dataset.

## Data Types

Use explicit PostgreSQL data types.

Examples:

### Keys

- INTEGER
- BIGINT
- VARCHAR

as appropriate to the deterministic key design.

### Dates

`DATE`

### Boolean Flags

`BOOLEAN`

### Measurements

Use suitable numeric types.

Examples:

- `NUMERIC`
- `DOUBLE PRECISION`

The selected types should preserve sufficient analytical precision without introducing unnecessary precision.

Do not depend entirely on automatic schema inference.

## Null Handling

Null values from trusted Gold models must retain their meaning.

Examples:

- unavailable demand → null
- unavailable price → null
- unavailable weather → null
- genuine zero generation → `0`

Never transform null into zero simply because Power BI handles zero more conveniently.

## Deletions and Corrections

The serving process must account for cases where a previously published Gold record becomes invalid or disappears after upstream correction.

The implementation must avoid leaving stale serving records indefinitely.

A reasonable approach is to reconcile all rows within the affected reprocessing date window.

Do not assume that upserts alone always handle every possible correction scenario.

## Storage Discipline

Aiven PostgreSQL is intentionally a compact serving layer.

Therefore:

- publish only approved Gold models
- do not replicate Bronze
- do not replicate Silver
- do not retain raw XML/JSON payloads
- avoid unnecessary duplicate aggregates
- monitor database size periodically

Databricks retains the larger historical and intermediate datasets.

## Connection Failure Behaviour

Connection failures must be distinguishable from data-quality failures.

Examples include:

- DNS failure
- timeout
- authentication failure
- SSL failure
- database unavailable

A PostgreSQL connectivity failure must:

- leave Databricks Gold unaffected
- fail the publish task visibly
- permit safe rerun

## Validation Before Publication

Before writing to PostgreSQL, validate:

- required keys are present
- logical keys are unique
- expected Gold models exist
- required dbt tests succeeded
- relationships are valid
- requested processing dates are represented as expected

Do not publish known-invalid Gold datasets.

## Validation After Publication

After publication, validate at minimum:

- expected target rows exist
- logical keys remain unique
- no orphan facts exist
- affected date range was synchronized
- target row counts are plausible
- dimensions required by published facts exist

For incremental runs, validation should focus primarily on the affected date range rather than rescanning unnecessarily large history.

## Idempotency

Publishing the same Gold dataset repeatedly must produce the same logical PostgreSQL state.

This applies to:

- initial-load reruns
- daily retries
- manual reruns
- source revisions
- reprocessing incomplete dates

Repeated runs must not create duplicate serving rows.

## Observability

Each serving execution should expose:

- execution start time
- execution end time
- requested date range
- tables processed
- source rows considered
- rows inserted
- rows updated
- rows removed/reconciled where applicable
- rows rejected
- validation result
- execution status

Credentials must never appear in logs.

## Power BI Contract

Power BI consumes PostgreSQL only.

Power BI must not directly depend on:

- Bronze tables
- Silver tables
- Databricks internal implementation details

The PostgreSQL schema therefore represents the stable external analytical contract.

Power BI should not need to know how ENTSO-E or Open-Meteo data was originally ingested.

## Failure Recovery

If serving publication fails:

1. Databricks Gold remains authoritative.
2. Existing valid PostgreSQL state should remain usable where transactional behaviour permits.
3. The failed publish operation can be rerun.
4. Manual deletion of serving tables should not normally be required.
5. Duplicate facts must not be created during recovery.

## Free-Tier Constraint

The serving implementation must remain compatible with the project's approved free-tier architecture.

Do not move large raw datasets into PostgreSQL merely because they are easier to query there.

The intended separation remains:

Databricks
→ large analytical/system-of-record storage

PostgreSQL
→ small curated serving layer

## Acceptance Criteria

This specification is complete when:

1. Databricks can connect securely to Aiven PostgreSQL.
2. Production credentials are retrieved securely at runtime.
3. All five approved serving tables exist.
4. PostgreSQL schemas match the Gold serving contract.
5. Initial 24-month Gold history can be published.
6. Daily incremental publication works.
7. Recently reprocessed dates can be updated.
8. Previously incomplete metrics can later be corrected.
9. Deterministic upserts prevent duplicate logical records.
10. Primary/unique constraints protect documented grains.
11. Dimension/fact relationships remain valid.
12. Missing Gold values remain null rather than being converted to zero.
13. Negative electricity prices remain valid.
14. Only validated Gold datasets are published.
15. Failed Gold quality gates prevent serving publication.
16. Batch-oriented database writes are used.
17. Failed database transactions can be safely rerun.
18. PostgreSQL remains a serving layer rather than a raw-data store.
19. Power BI can consume the resulting schema.
20. No local runtime dependency exists.

## Out of Scope

This specification does not include:

- Bronze ingestion
- Silver transformations
- dbt modelling
- Power BI dashboard design
- public application APIs
- streaming
- raw ENTSO-E storage in PostgreSQL
- raw Open-Meteo storage in PostgreSQL
- PostgreSQL-based business transformations
- using PostgreSQL as the analytical system of record