# 004 — dbt Gold Models

## Goal

Build curated analytical models in the Databricks Gold layer using dbt.

The Gold layer is the business-facing analytical layer inside Databricks and serves as the source for PostgreSQL publishing.

dbt is responsible for:

- dimensional modelling
- business logic
- analytical joins
- reusable metrics
- data-quality tests
- serving-ready datasets

## Execution Environment

dbt MUST execute against Databricks.

The dbt project may be edited locally but must not use the local machine as the production runtime.

Do not create:

- a local dbt production runtime
- a separate local analytical database
- local substitutes for Databricks execution

## dbt Runtime

The dbt project MUST run using a Databricks Lakeflow Jobs `dbt` task.

The dbt task must:

- use the dbt project stored in the Databricks Git-backed project
- use `dbt-databricks`
- execute generated SQL against a Databricks SQL Warehouse
- use the approved Databricks catalog/schema
- execute entirely within Databricks

Do not depend on the repository's shared `requirements.txt` for the dbt runtime.

Do not use:

- `dbt-spark`
- a locally running dbt production process
- a locally hosted database

The dbt task must explicitly install and pin:

`dbt-databricks==<approved-version>`

The exact version must be selected and recorded when this specification is implemented based on the supported Databricks/dbt combination at that time.

## Inputs

dbt models must consume the Silver datasets:

- `silver_energy_demand_daily`
- `silver_energy_price_daily`
- `silver_generation_mix_daily`
- `silver_weather_daily`

dbt must not query Bronze datasets directly.

Silver datasets must be declared as dbt sources.

## Trusted Silver Inputs

Gold models must consume only trusted Silver records.

Records classified as:

- `partial`
- `unavailable`
- `invalid`

must not silently contribute to trusted Gold metrics.

For example, if ENTSO-E demand for a country/date has incomplete timeline coverage, `daily_demand_mwh` must not contain an understated value derived from the partial day.

Gold must distinguish between:

- valid zero measurement
- missing measurement
- unavailable source data
- incomplete source data

Do not use `COALESCE(..., 0)` purely to make missing data appear complete.

Absence of a trusted measurement should remain null where appropriate.

## Required Gold Models

Create:

- `dim_country`
- `dim_date`
- `fact_energy_daily`
- `fact_weather_daily`
- `fact_generation_mix_daily`

These models form the initial contract with the PostgreSQL serving layer.

## dim_country

Grain:

`one row per MVP country`

Required fields:

- `country_key`
- `country_code`
- `country_name`
- `reference_location`
- `timezone`
- `entsoe_domain`

The dimension must contain exactly:

- Ireland
- Germany
- France
- Spain
- Netherlands

Use a stable deterministic key strategy.

Country keys must not depend on execution order or generated sequence values that could change between rebuilds.

## dim_date

Grain:

`one row per calendar date`

The dimension must cover at least:

- the complete 24-month historical backfill
- all dates subsequently processed by the daily pipeline

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

Use deterministic date keys.

## fact_energy_daily

Grain:

`country + date`

Combine trusted daily:

- electricity demand
- electricity price
- generation totals

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

Metrics must only be populated from Silver inputs satisfying their required completeness rules.

Absence of a trusted metric must remain null rather than being represented as zero.

Renewable generation:

`renewable_generation_mwh = SUM(generation_mwh WHERE renewable_flag = true)`

Renewable percentage:

`renewable_generation_mwh / total_generation_mwh * 100`

Zero-generation cases must be handled explicitly.

Do not divide by zero.

Do not replace an undefined renewable percentage with zero unless zero is semantically correct.

## fact_weather_daily

Grain:

`country + date`

Required fields:

- `country_key`
- `date_key`
- `avg_temperature_c`
- `avg_wind_speed_kmh`
- `solar_radiation_mj_m2`
- `reference_location`

Weather measurements should preserve nulls when trusted source values are unavailable.

Do not convert missing weather measurements to zero.

## fact_generation_mix_daily

Grain:

`country + date + production_type`

Required fields:

- `country_key`
- `date_key`
- `production_type`
- `generation_mwh`
- `renewable_flag`
- `generation_share_pct`

Generation share:

`production_type_generation_mwh / total_daily_generation_mwh * 100`

Zero-generation cases must be handled explicitly.

Do not divide by zero.

Only trusted generation records should contribute to generation-share calculations.

## Modelling Principles

Gold models should expose clear business concepts.

Avoid leaking unnecessary source-specific implementation details into Gold.

Retain source-specific identifiers only where they provide meaningful analytical value.

Prefer readable analytical names over raw upstream field names.

Business calculations should be centralized in dbt rather than duplicated across Power BI or PostgreSQL.

## Materialization

Choose dbt materializations appropriate for dataset size and refresh behaviour.

For the MVP:

- dimensions may be tables
- daily fact models should be persisted tables or incremental models where justified

Do not introduce complex incremental patterns purely to demonstrate dbt features.

## Incremental Behaviour

Where incremental models are used:

- use stable unique keys
- support reprocessing recent dates
- update revised records
- avoid duplicate facts
- support ENTSO-E revisions

Rebuilding or reprocessing a country/date must result in one correct logical Gold record.

Previously missing or incomplete metrics must be capable of being populated later when trusted Silver data becomes available.

## Relationships

Expected relationships:

`fact_energy_daily.country_key`
→ `dim_country.country_key`

`fact_energy_daily.date_key`
→ `dim_date.date_key`

`fact_weather_daily.country_key`
→ `dim_country.country_key`

`fact_weather_daily.date_key`
→ `dim_date.date_key`

`fact_generation_mix_daily.country_key`
→ `dim_country.country_key`

`fact_generation_mix_daily.date_key`
→ `dim_date.date_key`

Use dbt `ref()` relationships to express dependencies.

Avoid manually hard-coding execution order where dbt lineage can express it.

## dbt Tests

At minimum implement the following tests.

### dim_country

Validate:

- `country_key` is unique
- `country_key` is not null
- `country_code` is unique
- `country_code` is not null
- exactly five configured MVP countries exist

### dim_date

Validate:

- `date_key` is unique
- `date_key` is not null
- `date` is unique
- `date` is not null

### fact_energy_daily

Validate grain:

`country_key + date_key`

Validate:

- logical grain is unique
- foreign keys are not null
- country relationship is valid
- date relationship is valid

### fact_weather_daily

Validate grain:

`country_key + date_key`

Validate:

- logical grain is unique
- foreign keys are not null
- country relationship is valid
- date relationship is valid

### fact_generation_mix_daily

Validate grain:

`country_key + date_key + production_type`

Validate:

- logical grain is unique
- foreign keys are not null
- production type is not null
- country relationship is valid
- date relationship is valid

## Business Validation

At minimum validate:

### Energy

- demand is non-negative where present
- total generation is non-negative where present
- renewable generation is non-negative where present
- renewable generation does not exceed total generation beyond accepted numerical tolerance
- renewable percentage is between 0 and 100 where calculable

### Generation Mix

- generation is non-negative
- generation share is between 0 and 100 where calculable
- renewable flag is present
- daily generation shares approximately sum to 100 where the generation dataset is complete

### Weather

- wind speed is non-negative where present
- solar radiation is non-negative where present

### Prices

Negative electricity prices are valid.

Do not reject them.

## Cross-Model Reconciliation

For each complete:

`country + date`

validate that:

`fact_energy_daily.total_generation_mwh`

reconciles with the sum of:

`fact_generation_mix_daily.generation_mwh`

within an appropriate numerical tolerance.

Similarly:

`fact_energy_daily.renewable_generation_mwh`

must reconcile with the generation-mix rows classified as renewable.

Material discrepancies must fail or surface through dbt testing rather than remaining silent.

## Null Semantics

Null must retain its analytical meaning.

Do not automatically transform null values into zero.

Examples:

- missing demand != zero demand
- missing price != zero price
- unavailable generation != zero generation
- missing weather != zero weather

Dimension keys required for an existing fact record must not be null.

## Documentation

dbt models and important columns must have descriptions.

Documentation should allow someone unfamiliar with ingestion code to understand:

- model grain
- business meaning
- upstream dependency
- calculated metrics
- units
- important null semantics

## Lineage

dbt dependencies must accurately reflect model relationships.

Use:

- `source()` for Silver inputs
- `ref()` for dbt model dependencies

Avoid bypassing dbt lineage with hard-coded fully qualified downstream model references where unnecessary.

## PostgreSQL Contract

The following Gold models form the initial serving contract:

- `dim_country`
- `dim_date`
- `fact_energy_daily`
- `fact_weather_daily`
- `fact_generation_mix_daily`

Changes to:

- model grain
- required fields
- field meaning
- key strategy

must be treated as serving-contract changes.

PostgreSQL must consume these curated models rather than recreating Gold business logic.

## Acceptance Criteria

This specification is complete when:

1. dbt executes successfully on Databricks.
2. A Databricks Lakeflow Jobs `dbt` task is used.
3. `dbt-databricks` is explicitly pinned for the dbt runtime.
4. Generated SQL executes against the configured Databricks SQL Warehouse.
5. Silver datasets are declared as dbt sources.
6. All five required Gold models exist.
7. Gold facts follow their documented grains.
8. Only trusted Silver records contribute to trusted Gold metrics.
9. Partial or unavailable Silver measurements are not silently converted to zero.
10. Renewable generation is calculated correctly.
11. Renewable generation percentage is calculated correctly.
12. Generation-share percentage is calculated correctly.
13. Dimensions use stable deterministic keys.
14. Uniqueness tests pass.
15. Required not-null tests pass.
16. Foreign-key relationship tests pass.
17. Business-validation tests pass.
18. Generation totals reconcile across Gold models.
19. Models and important columns are documented.
20. dbt lineage accurately represents dependencies.
21. Gold models are persisted in Databricks.
22. Gold models are ready for PostgreSQL publishing.
23. No local runtime dependency exists.

## Out of Scope

This specification does not include:

- API ingestion
- Bronze processing
- Silver transformations
- PostgreSQL loading
- Power BI dashboards
- forecasting
- machine learning
- streaming
- hourly analytical models