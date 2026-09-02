# 005 — PostgreSQL Serving Layer

## Goal

Publish curated Gold datasets from Databricks into Aiven PostgreSQL for downstream consumption by Power BI.

PostgreSQL acts only as the serving layer.

Databricks remains the system of record.

## Architecture

Databricks Gold
→ PostgreSQL serving tables
→ Power BI

PostgreSQL must not contain:

- Bronze data
- Silver data
- raw API payloads
- unnecessary historical intermediate datasets

## Source Models

Publish only the approved Gold models:

- `dim_country`
- `dim_date`
- `fact_energy_daily`
- `fact_weather_daily`
- `fact_generation_mix_daily`

## Target Database

Use Aiven PostgreSQL.

Database credentials must be retrieved securely at runtime.

Never commit:

- database password
- connection string containing credentials
- certificates containing secrets
- tokens

## Execution Environment

Publishing MUST execute from Databricks.

The local machine is not a supported runtime.

Python/PySpark may be used to:

- read Gold Delta tables
- prepare serving datasets
- connect to PostgreSQL
- execute upsert/load operations

## Serving Tables

Create:

### dim_country

Primary key:

`country_key`

Expected fields:

- country_key
- country_code
- country_name
- reference_location
- timezone
- entsoe_domain

### dim_date

Primary key:

`date_key`

Expected fields:

- date_key
- date
- year
- quarter
- month
- month_name
- day_of_month
- day_of_week
- day_name
- week_of_year
- is_weekend

### fact_energy_daily

Logical key:

`country_key + date_key`

Expected fields:

- country_key
- date_key
- daily_demand_mwh
- avg_day_ahead_price_eur_mwh
- min_day_ahead_price_eur_mwh
- max_day_ahead_price_eur_mwh
- total_generation_mwh
- renewable_generation_mwh
- renewable_generation_pct

### fact_weather_daily

Logical key:

`country_key + date_key`

Expected fields:

- country_key
- date_key
- avg_temperature_c
- avg_wind_speed_kmh
- solar_radiation_mj_m2
- reference_location

### fact_generation_mix_daily

Logical key:

`country_key + date_key + production_type`

Expected fields:

- country_key
- date_key
- production_type
- generation_mwh
- renewable_flag
- generation_share_pct

## Initial Load

The initial serving load must publish the complete approved 24-month Gold history.

The implementation must avoid creating duplicate records if the initial load is rerun.

## Daily Incremental Load

After initial publication, only affected dates should normally be loaded.

The publishing process must support:

- new daily records
- updated historical records
- corrections caused by upstream source revisions
- rerunning recent dates

Do not rely on blind append-only behaviour.

## Upsert Strategy

Serving tables must use deterministic keys.

Use PostgreSQL upsert semantics or equivalent transactional behaviour.

Expected behaviour:

- record does not exist → insert
- record already exists → update
- duplicate logical facts must not be created

## Transaction Behaviour

A failed publishing operation must not leave a partially inconsistent serving dataset.

Where practical, logically related writes should execute transactionally.

Failures must surface clearly.

## Referential Integrity

Fact tables should reference:

- `dim_country`
- `dim_date`

Dimensions must be loaded before dependent facts.

Foreign-key constraints may be implemented where they do not unnecessarily complicate the MVP.

At minimum, referential integrity must be validated before or during publishing.

## Schema Ownership

PostgreSQL table schemas must follow the Gold model contract.

Do not create additional business transformations inside PostgreSQL.

Business logic belongs in:

Databricks Silver
or
dbt Gold

PostgreSQL should primarily serve already-curated data.

## Indexing

Create indexes only for realistic serving access patterns.

At minimum consider indexes supporting:

- country + date
- date
- production type where relevant

Primary and unique keys should enforce logical grain.

Do not over-index the small MVP dataset.

## Storage Discipline

The PostgreSQL free-tier storage limit must be treated as a project constraint.

Therefore:

- publish only serving-ready Gold data
- do not replicate Bronze/Silver datasets
- avoid unnecessary duplicate tables
- avoid storing raw payloads
- periodically monitor database size

The serving layer should remain intentionally compact.

## Connection Handling

Connections should:

- use SSL where required
- use explicit timeouts
- avoid embedding credentials
- close cleanly
- fail visibly on authentication/network errors

Avoid opening one database connection per individual record.

Use batch-oriented writes.

## Data Type Mapping

Use explicit PostgreSQL types.

Examples:

Identifiers:
- INTEGER / BIGINT / VARCHAR as appropriate

Dates:
- DATE

Percentages and monetary values:
- NUMERIC with appropriate precision

Boolean flags:
- BOOLEAN

Do not rely entirely on automatic type inference.

## Idempotency

Publishing the same country/date more than once must result in the same logical serving state.

This applies to:

- manual reruns
- retries
- historical corrections
- daily incremental loads

## Validation

Before publication, validate:

- required keys are present
- logical keys are unique
- dimension relationships are valid
- required numeric values have expected types

After publication, validate:

- expected row counts
- no duplicate logical keys
- no orphan fact records
- published date range matches the requested load

## Observability

Each publish execution should expose:

- start time
- end time
- tables processed
- rows inserted
- rows updated
- rows rejected
- failed tables
- requested date range
- execution status

Credentials must never appear in logs.

## Power BI Contract

Power BI must consume PostgreSQL only.

Power BI must not depend directly on:

- Bronze Delta tables
- Silver Delta tables
- Databricks internal transformation models

The PostgreSQL schema therefore represents the stable BI-facing contract.

## Failure Behaviour

If PostgreSQL publication fails:

- Databricks Gold data must remain unaffected
- the failure must be visible
- the serving load must be safely rerunnable
- partial duplicate data must not be created

## Acceptance Criteria

This specification is complete when:

1. Databricks can connect securely to Aiven PostgreSQL.
2. All five approved serving tables exist.
3. Initial 24-month Gold history can be published.
4. Daily incremental publishing works.
5. Existing dates can be safely updated.
6. Duplicate logical facts cannot be created.
7. Dimension/fact relationships remain valid.
8. PostgreSQL contains only serving-layer datasets.
9. Credentials are not present in source control.
10. Batch writes are used.
11. Publish failures are observable and rerunnable.
12. Power BI can consume the resulting schema.
13. No local runtime dependency exists.

## Out of Scope

This specification does not include:

- Bronze ingestion
- Silver transformations
- dbt modelling
- Power BI dashboard design
- public application APIs
- streaming
- PostgreSQL-based business transformations
- storing raw historical data in PostgreSQL