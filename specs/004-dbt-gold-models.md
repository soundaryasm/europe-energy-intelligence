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

Do not create a separate local database for dbt execution.

## Inputs

dbt models must consume the Silver datasets:

- `silver_energy_demand_daily`
- `silver_energy_price_daily`
- `silver_generation_mix_daily`
- `silver_weather_daily`

dbt must not query Bronze datasets directly.

## Required Gold Models

Create the following models:

- `dim_country`
- `dim_date`
- `fact_energy_daily`
- `fact_weather_daily`
- `fact_generation_mix_daily`

These models are also the initial datasets intended for PostgreSQL publishing.

## dim_country

One row per MVP country.

Required fields:

- `country_key`
- `country_code`
- `country_name`
- `reference_location`
- `timezone`
- `entsoe_domain`

The dimension must contain exactly the configured MVP countries:

- Ireland
- Germany
- France
- Spain
- Netherlands

Use a stable deterministic key strategy.

## dim_date

One row per calendar date required by the project.

The date dimension should cover at least the full historical backfill range and ongoing future daily loads.

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

Combine daily demand and electricity-price data.

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

Renewable percentage:

`renewable_generation_mwh / total_generation_mwh * 100`

Handle zero-generation cases explicitly.

Do not silently divide by zero.

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

Handle zero-generation cases explicitly.

## Modelling Principles

Gold models should expose clear business concepts.

Avoid leaking unnecessary source-specific fields into Gold.

Retain source-specific identifiers only when they provide meaningful analytical value.

Prefer readable business names over raw upstream field names.

## Materialization

Choose dbt materializations appropriate for the dataset size and refresh pattern.

For MVP:

- dimensions may be tables
- daily fact models should be persisted tables or incremental models where justified

Do not introduce complex incremental logic purely for demonstration.

Any incremental model must remain safe for rerunning recent dates.

## Incremental Behaviour

Where incremental models are used:

- use stable unique keys
- support reprocessing recent dates
- update changed records
- avoid duplicate facts

The project must support ENTSO-E revisions to previously ingested dates.

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

## dbt Tests

At minimum implement:

### Dimension Tests

`dim_country`

- `country_key` unique
- `country_key` not null
- `country_code` unique
- `country_code` not null

`dim_date`

- `date_key` unique
- `date_key` not null
- `date` unique
- `date` not null

### Fact Tests

All foreign keys must be non-null.

Relationship tests must confirm country and date keys exist in their dimensions.

The logical grain must be unique:

`fact_energy_daily`
- unique country/date

`fact_weather_daily`
- unique country/date

`fact_generation_mix_daily`
- unique country/date/production_type

## Business Validation

At minimum validate:

- renewable percentage between 0 and 100 where calculable
- generation share between 0 and 100 where calculable
- demand is non-negative
- total generation is non-negative
- weather wind speed is non-negative

Negative electricity prices remain valid.

## Documentation

dbt models and important columns should have descriptions.

The dbt project should make it possible for someone unfamiliar with the ingestion implementation to understand:

- model grain
- business meaning
- upstream dependency
- important calculated metrics

## Lineage

dbt dependencies must reflect actual model relationships.

Avoid hard-coded execution ordering where dbt `ref()` relationships can express dependencies.

Silver datasets should be declared as dbt sources.

## PostgreSQL Contract

The five required Gold models form the initial contract with the serving layer:

- `dim_country`
- `dim_date`
- `fact_energy_daily`
- `fact_weather_daily`
- `fact_generation_mix_daily`

Changes to their grain or required fields must be treated as serving-contract changes.

## Acceptance Criteria

This specification is complete when:

1. dbt executes successfully against Databricks.
2. Silver datasets are declared as dbt sources.
3. All five required Gold models exist.
4. Gold facts follow the documented grains.
5. Renewable generation percentage is calculated correctly.
6. Generation share percentage is calculated correctly.
7. Dimensions use stable keys.
8. dbt uniqueness and not-null tests pass.
9. Foreign-key relationship tests pass.
10. Business-validation tests pass.
11. Models and important columns are documented.
12. Gold models are persisted in Databricks.
13. Gold models are ready for PostgreSQL publishing.
14. No local runtime dependency exists.

## dbt Runtime Dependency

The dbt project must run using a Databricks Lakeflow Jobs `dbt` task.

Do not depend on the repository's shared `requirements.txt` for the dbt runtime.

The dbt task must explicitly install and pin:

`dbt-databricks==<approved-version>`

Do not use `dbt-spark`.

The exact version should be selected and recorded when Spec 004 is implemented, based on the currently supported Databricks/dbt combination.

The dbt task must execute dbt Core on Databricks compute and execute generated SQL against the configured Databricks SQL Warehouse.

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