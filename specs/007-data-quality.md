# 007 — Data Quality

## Goal

Define consistent data-quality checks across Bronze, Silver, Gold, and PostgreSQL serving layers.

The objective is to detect:

- missing data
- duplicate records
- invalid values
- broken relationships
- incomplete source loads
- unexpected schema changes

Data-quality failures must be visible and must not silently propagate into Power BI.

## Principles

Data quality should be applied progressively.

### Bronze

Validate that source ingestion succeeded and produced structurally usable data.

### Silver

Validate cleaned and standardized analytical values.

### Gold

Validate business rules, model grain, uniqueness, and relationships using dbt.

### PostgreSQL

Validate that serving data matches the approved Gold contract.

## Bronze Checks

Bronze checks should remain lightweight.

At minimum validate:

- API request succeeded
- response is not an API/error document
- expected source fields are present
- country is known
- timestamps/dates can be parsed
- numeric values can be parsed where required
- source system is recorded
- ingestion timestamp is present

Do not apply heavy business logic in Bronze.

Raw source values should generally be preserved.

## Source Completeness

Each ingestion run must make it possible to determine whether expected data was received.

For each:

`country + dataset + processing_date`

record:

- request attempted
- request succeeded
- records returned
- records written
- ingestion timestamp

An HTTP success response with unexpectedly empty data must not automatically be considered a successful analytical load.

## Duplicate Checks

Logical duplicate detection must exist throughout the pipeline.

Examples of Bronze/Silver business keys include:

### ENTSO-E Load

- country
- source timestamp
- dataset type

### ENTSO-E Price

- country
- source timestamp
- dataset type

### ENTSO-E Generation

- country
- source timestamp
- production type
- dataset type

### Weather

- country
- source timestamp/date
- weather variable

The most recently ingested valid source record should win when revised source records exist.

## Silver Quality Rules

### Electricity Demand

Validate:

- country is valid
- local date is present
- interval duration is positive
- load values are non-negative
- calculated daily demand is non-negative

Do not assume a fixed number of intervals per day.

### Electricity Prices

Validate:

- country is valid
- local date is present
- prices are numeric
- interval duration is positive

Negative prices are valid.

Do not reject them.

### Generation

Validate:

- country is valid
- local date is present
- production type is present
- normalized production type exists
- renewable classification exists
- generation values are non-negative unless explicitly justified by source semantics

Unknown production types should be surfaced and mapped deliberately.

Do not silently drop them.

### Weather

Validate:

- country is valid
- local date is present
- temperature is numeric
- wind speed is non-negative
- solar radiation is non-negative

Use configurable plausibility bounds for weather measurements.

Plausibility checks should detect obvious source/parser issues rather than enforce overly narrow climate assumptions.

## Gold Quality Rules

Gold model validation should primarily use dbt tests.

### dim_country

Validate:

- unique `country_key`
- unique `country_code`
- no null keys
- exactly five MVP countries

### dim_date

Validate:

- unique `date_key`
- unique `date`
- no null keys
- continuous date coverage for the required analytical range

### fact_energy_daily

Validate grain:

`country_key + date_key`

Validate:

- unique logical grain
- non-null foreign keys
- valid country relationship
- valid date relationship
- demand >= 0
- generation >= 0
- renewable percentage between 0 and 100 where calculable

Negative electricity prices remain valid.

### fact_weather_daily

Validate grain:

`country_key + date_key`

Validate:

- unique logical grain
- valid foreign keys
- wind speed >= 0
- solar radiation >= 0

### fact_generation_mix_daily

Validate grain:

`country_key + date_key + production_type`

Validate:

- unique logical grain
- valid foreign keys
- generation >= 0
- generation share between 0 and 100 where calculable
- renewable flag is not null

## Cross-Dataset Validation

Where appropriate, perform consistency checks across datasets.

Examples:

### Generation Share

For each:

`country + date`

generation-share percentages should approximately sum to 100%.

Allow a small numerical tolerance.

### Renewable Generation

Renewable generation must equal the sum of generation classified as renewable.

### Total Generation

`fact_energy_daily.total_generation_mwh`

must reconcile with the total generation represented by:

`fact_generation_mix_daily`

within an appropriate numerical tolerance.

## Completeness Checks

The expected MVP geography is:

- Ireland
- Germany
- France
- Spain
- Netherlands

For each processing date, determine which countries have usable data.

Do not blindly require all five countries if the upstream source legitimately has delayed or unavailable data.

Instead distinguish:

- complete
- partially available
- unavailable
- failed ingestion

A missing country must be visible in execution results.

## Freshness

For normal daily execution, validate that newly processed data corresponds to the expected processing period.

Detect situations where:

- the pipeline runs successfully
- but only stale historical data is present

Serving publication should not claim a successful new refresh when no valid new data was produced.

## Null Handling

Do not convert null measurements to zero unless zero is explicitly the correct business meaning.

Examples:

- missing price != zero price
- missing generation != zero generation
- missing weather observation != zero weather

Required-key nulls should fail validation.

Optional measurement nulls should remain identifiable.

## Schema Drift

Unexpected source schema changes must be visible.

Examples:

- missing required XML element
- renamed API field
- unexpected data type
- new ENTSO-E production type

Do not silently discard fields or records solely to keep the pipeline running.

## Quarantine / Invalid Records

Where practical, invalid Silver-stage records may be written to a dedicated rejected/quarantine dataset containing:

- source identifier
- country
- processing date
- original record identifier
- validation rule
- rejection reason
- ingestion timestamp

Quarantine is optional for simple failures but preferred when invalid source records need investigation.

Invalid records must not silently enter trusted Gold models.

## Pipeline Gating

Data quality must influence workflow execution.

At minimum:

- failed critical ingestion checks block Silver
- failed critical Silver validation blocks dbt
- failed required dbt tests block PostgreSQL publication

Warnings may be permitted for non-critical quality issues.

Critical vs warning rules must be explicit.

## Critical Failures

Examples of critical failures:

- source authentication failure
- malformed API response
- missing required country/date keys
- duplicate Gold logical keys
- broken dimension relationships
- invalid model grain
- failed required dbt tests
- corrupted serving schema

Critical failures must stop downstream publication.

## Warnings

Examples of warnings:

- one source publishes fewer observations than historically typical
- unusual but plausible price
- unusually high/low weather measurement
- temporary absence of one optional metric

Warnings should be logged but do not necessarily stop the pipeline.

## Observability

Each run should report quality information such as:

- source records received
- valid records
- rejected records
- duplicates removed
- null counts for important fields
- countries available
- countries missing
- dbt test results
- serving validation results

Quality metrics should be easy to inspect in Databricks job logs.

## Historical Backfill

Data-quality checks also apply to historical backfills.

Historical backfill must not bypass validation solely because it processes larger date ranges.

Quality reporting should make it possible to identify specific dates/countries with problems.

## PostgreSQL Validation

Before publishing, validate the Gold dataset.

After publishing, verify:

- expected rows exist
- logical keys remain unique
- no orphan facts exist
- target date range was updated
- serving row counts are reasonable relative to Gold

Do not modify Gold data to accommodate serving-layer failures.

## Power BI Protection

Power BI must consume only validated PostgreSQL serving tables.

Known-invalid pipeline runs must not overwrite the last valid serving state with incomplete data.

## Acceptance Criteria

This specification is complete when:

1. Bronze ingestion validates API response structure.
2. Empty/unavailable source data can be distinguished from successful ingestion.
3. Duplicate logical source records are handled deterministically.
4. Silver datasets enforce basic measurement rules.
5. Unknown production types are surfaced.
6. Gold models enforce documented grains.
7. Gold uniqueness and relationship tests exist.
8. Renewable and generation-share calculations are validated.
9. Cross-model generation totals reconcile within tolerance.
10. Missing countries/dates are observable.
11. Critical quality failures prevent PostgreSQL publishing.
12. Negative electricity prices remain valid.
13. Null measurements are not silently converted to zero.
14. PostgreSQL publication is validated.
15. Quality results are visible from Databricks workflow execution.

## Out of Scope

This specification does not include:

- enterprise data-observability platforms
- Monte Carlo
- Great Expectations
- dedicated alerting infrastructure
- anomaly-detection machine learning
- automated source-quality SLAs
- real-time quality monitoring