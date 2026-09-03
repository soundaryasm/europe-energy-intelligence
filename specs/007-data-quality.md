# 007 — Data Quality

## Goal

Define consistent data-quality rules across Bronze, Silver, Gold, and PostgreSQL serving layers.

The project must detect and surface:

- malformed source responses
- missing data
- incomplete timeline coverage
- duplicate observations
- overlapping observations
- invalid measurements
- unexpected source changes
- broken dimensional relationships
- stale serving data

Known-invalid or incomplete data must not silently propagate into trusted Power BI metrics.

## Principles

Data quality is applied progressively.

### Bronze

Validate that external source data was retrieved and parsed correctly while preserving source fidelity.

### Silver

Validate analytical usability, completeness, units, timestamps, and business-safe transformations.

### Gold

Validate dimensional grain, relationships, business calculations, and model consistency using dbt.

### PostgreSQL

Validate that the serving layer accurately reflects approved Gold models.

## Quality Status

Where source completeness is relevant, use consistent statuses:

- `complete`
- `partial`
- `unavailable`
- `invalid`

A technical execution result and an analytical completeness status are different concepts.

For example:

- API request succeeds
- XML parses correctly
- records are returned
- but three hours are missing

This is technically successful ingestion but analytically:

`partial`

Do not reduce these states to a single success/failure boolean.

## Bronze Validation

Bronze validation should remain lightweight and source-oriented.

At minimum validate:

- HTTP request completed successfully
- response is not an API error document
- response can be parsed
- required structural elements exist
- country/source configuration is known
- timestamps/dates are parseable
- numeric fields can be parsed where required
- source system is recorded
- ingestion timestamp is present

Bronze should preserve valid source records even when the requested period is incomplete.

Do not apply heavy analytical business logic in Bronze.

## ENTSO-E Response Validation

For ENTSO-E, validate:

- expected MarketDocument structure
- expected document/process type
- one or more valid TimeSeries where data exists
- valid Period boundaries
- supported Period resolution
- valid Point positions
- parseable numeric quantities
- required bidding-zone/domain information
- expected units where applicable

Do not interpret ENTSO-E acknowledgement/error XML as valid empty analytical data.

## ENTSO-E Timeline Reconstruction

Completeness must be evaluated using reconstructed timestamps.

Each Point timestamp is derived from:

`Period start + (position - 1) × resolution`

Each Period must be evaluated independently.

Do not assume:

- a TimeSeries has one Period
- positions continue across Periods
- the first Period starts at midnight
- Periods are contiguous
- record count alone proves completeness

Gaps between Periods must remain identifiable.

## ENTSO-E Completeness

For each relevant:

`country + dataset + local_date`

determine whether the required timeline is complete.

Completeness must consider:

- reconstructed timestamps
- source resolution
- interval duration
- missing intervals
- overlapping intervals
- timezone conversion
- local-day boundaries
- daylight-saving-time transitions

Do not hard-code:

`96 points = complete day`

as a universal rule.

A normal PT15M day may contain 96 intervals, but completeness must be based on the expected timeline for that actual date and timezone.

## Partial ENTSO-E Data

If some valid observations exist but expected timeline coverage is incomplete:

classify the dataset/date as:

`partial`

Partial data must:

- remain available for diagnosis
- not be filled with synthetic zero values
- not be extrapolated automatically
- not produce a trusted understated daily metric
- remain eligible for later reprocessing

A later ENTSO-E lookback run may replace the partial state once missing observations become available.

## ENTSO-E Overlaps

Detect overlapping logical observations.

Examples include:

- overlapping Periods
- repeated source intervals
- observations retrieved again during lookback execution

Deduplicate using deterministic source business keys before analytical aggregation.

An overlapping interval must never contribute twice to:

- demand
- generation
- weighted price calculations

## Open-Meteo Validation

For Open-Meteo validate:

- expected response structure
- requested daily dates are represented where expected
- required daily measurement arrays exist
- parallel date/value arrays align correctly
- returned timezone is present
- numeric measurements are parseable
- units are known or explicitly normalized
- returned coordinates are valid

Configured coordinates and returned weather-grid coordinates may differ.

This is expected behaviour and must not be treated as a quality failure.

## Source Availability

Every ingestion run must distinguish:

- data available and complete
- data available but partial
- data legitimately unavailable
- source request failed
- response invalid

An HTTP `200` response alone does not imply usable analytical data.

## Duplicate Handling

Logical duplicates must be detected deterministically.

Example business keys:

### ENTSO-E Load

- country
- dataset type
- source timestamp

### ENTSO-E Price

- country
- dataset type
- source timestamp

### ENTSO-E Generation

- country
- dataset type
- source timestamp
- production type

### Open-Meteo

- country
- local date

When multiple valid versions of the same logical observation exist, prefer the latest valid ingestion/source revision according to the project's deterministic reconciliation logic.

## Silver Quality Rules

## Electricity Demand

Validate:

- country is configured
- source timestamp is valid
- source resolution is supported
- interval duration is positive
- load is numeric
- load is non-negative
- duplicates are resolved
- overlapping intervals are resolved
- local date is valid

Trusted daily demand requires complete timeline coverage.

If coverage is partial:

`daily_demand_mwh`

must not be presented as a trusted complete daily metric.

## Electricity Prices

Validate:

- country is configured
- timestamps are valid
- interval duration is valid
- prices are numeric
- required timeline coverage is complete for trusted daily metrics

Negative prices are valid.

Do not reject:

- zero prices
- negative prices

simply because they appear unusual.

## Generation

Validate:

- country is configured
- timestamp is valid
- production type exists
- production type mapping exists
- renewable classification exists
- source interval duration is valid
- generation is numeric

Generation should normally be non-negative unless documented source semantics justify otherwise.

Previously unseen production types must be surfaced rather than silently discarded.

## Generation Completeness

Evaluate generation timeline coverage at the appropriate production-type level.

Missing observations are not equivalent to:

`0 MWh generation`

A reported zero is valid data.

An absent interval is missing data.

These must remain analytically distinct.

## Weather

Validate:

- configured country exists
- local date exists
- required daily metrics are present where expected
- temperature is numeric
- wind speed is numeric and non-negative
- solar radiation is numeric and non-negative

Use configurable plausibility bounds only to detect obvious corruption or parsing errors.

Do not create overly narrow climate rules that reject legitimate weather.

## Unit Validation

Canonical analytical units are:

### Demand

`MWh`

### Generation

`MWh`

### Price

`EUR/MWh`

### Temperature

`°C`

### Wind Speed

`km/h`

### Solar Radiation

`MJ/m²`

Source units must be validated before conversion.

Unknown units must not be silently assumed to match the canonical unit.

## Null Semantics

Null measurements must retain their meaning.

Never automatically convert missing values to zero.

Examples:

- missing demand != zero demand
- missing generation != zero generation
- missing price != zero price
- missing weather != zero weather

Zero is a valid business value only when the source actually represents zero.

## Trusted Silver Output

Trusted Silver datasets must contain analytically valid daily values.

Records classified as:

- `partial`
- `invalid`
- `unavailable`

must not silently contribute to trusted downstream calculations.

Diagnostic records may be preserved separately.

## Quarantine / Rejected Records

Where useful, invalid records may be persisted to a rejected/quarantine dataset.

Recommended fields:

- source system
- country
- dataset
- processing date
- source record identifier
- validation rule
- rejection reason
- ingestion timestamp

Quarantine is intended for diagnosis.

Rejected records must not enter trusted Gold models.

## Gold Quality Rules

Gold validation should primarily use dbt tests.

## dim_country

Validate:

- `country_key` unique
- `country_key` not null
- `country_code` unique
- `country_code` not null
- exactly five MVP countries exist

Expected countries:

- Ireland
- Germany
- France
- Spain
- Netherlands

## dim_date

Validate:

- `date_key` unique
- `date_key` not null
- `date` unique
- `date` not null
- required analytical date range is covered

## fact_energy_daily

Grain:

`country_key + date_key`

Validate:

- logical grain is unique
- foreign keys are not null
- country relationship is valid
- date relationship is valid
- demand >= 0 where present
- total generation >= 0 where present
- renewable generation >= 0 where present
- renewable percentage between 0 and 100 where calculable

Negative electricity prices remain valid.

Missing trusted energy metrics must remain null rather than being converted to zero.

## fact_weather_daily

Grain:

`country_key + date_key`

Validate:

- logical grain is unique
- foreign keys are valid
- wind speed >= 0 where present
- solar radiation >= 0 where present

Missing weather metrics must remain null.

## fact_generation_mix_daily

Grain:

`country_key + date_key + production_type`

Validate:

- logical grain is unique
- foreign keys are valid
- production type is not null
- renewable flag is not null
- generation >= 0 where present
- generation share between 0 and 100 where calculable

## Cross-Model Reconciliation

For complete country/date generation datasets:

`SUM(fact_generation_mix_daily.generation_mwh)`

must reconcile with:

`fact_energy_daily.total_generation_mwh`

within an approved numerical tolerance.

Likewise:

`SUM(generation_mwh WHERE renewable_flag = true)`

must reconcile with:

`fact_energy_daily.renewable_generation_mwh`

Generation shares should approximately sum to:

`100%`

for complete generation datasets.

Allow a small tolerance for numerical precision.

## Renewable Percentage

Where total generation is greater than zero:

`renewable_generation_pct`

must equal:

`renewable_generation_mwh / total_generation_mwh × 100`

within numerical tolerance.

Where total generation is zero or unavailable:

the percentage must be handled explicitly.

Do not silently divide by zero.

## Completeness Across Countries

The expected MVP geography is:

- Ireland
- Germany
- France
- Spain
- Netherlands

For every processing date, report country-level data availability.

Do not blindly fail because all five countries are not complete.

Instead distinguish:

- complete
- partial
- unavailable
- failed

Missing countries/datasets must remain visible.

## Freshness

Track the difference between:

- workflow execution time
- ingestion time
- source data date
- latest complete analytical date
- PostgreSQL publication time

A successful workflow run must not imply that every source published newer complete data.

Detect stale data where the pipeline executes but no new usable analytical period becomes available.

## Schema Drift

Unexpected source changes must be surfaced.

Examples:

- missing required XML elements
- unexpected Period resolution
- new ENTSO-E production type
- Open-Meteo response-field changes
- unexpected unit
- changed response data type

Do not silently discard unfamiliar records merely to keep the pipeline green.

## Critical Failures

Examples of critical failures include:

- authentication failure
- malformed source response
- unsupported required source schema
- invalid deterministic keys
- duplicate Gold logical grains
- broken dimension relationships
- failed required dbt tests
- corrupted serving schema

Critical failures must block downstream publication.

## Non-Critical Partial Data

Partial or temporarily unavailable source data may be analytically non-critical if the pipeline safely preserves null/incomplete semantics.

For example:

ENTSO-E missing intervals for one country/date may result in:

- successful Bronze ingestion
- partial completeness status
- no trusted daily demand
- valid Gold row with null metric where appropriate
- later correction through lookback reprocessing

Do not convert every partial-source case into a total pipeline outage.

## Warnings

Warnings may include:

- unusual but plausible price
- unusual weather measurement
- reduced interval coverage
- newly observed production type
- one country temporarily unavailable

Warnings should be visible but do not necessarily block the complete workflow.

Critical/warning classification must be explicit.

## Pipeline Gating

At minimum:

- malformed critical source data blocks dependent transformation
- critical Silver validation blocks Gold build
- failed required dbt tests block PostgreSQL publication
- invalid Gold serving contract blocks publication

PostgreSQL must never receive a Gold build known to violate required quality rules.

## PostgreSQL Validation

Before publication validate:

- serving models exist
- required keys are present
- logical keys are unique
- dimension relationships are valid
- required dbt tests passed

After publication validate:

- affected rows exist
- serving grains remain unique
- no orphan facts exist
- affected date range was reconciled
- target row counts are plausible

## Stale Serving Protection

If a new pipeline run fails critically:

do not replace previously valid PostgreSQL data with an incomplete or corrupted serving state.

The last trusted serving state should remain usable where possible.

## Reprocessing Behaviour

Quality state must be capable of improving over time.

Example:

Day 1:

`2026-09-01 Netherlands load = partial`

Later ENTSO-E publishes missing observations.

Lookback rerun:

`2026-09-01 Netherlands load = complete`

The pipeline must allow the trusted Silver, Gold, and PostgreSQL state for that date to be updated accordingly.

## Historical Backfill

All quality rules also apply to the 24-month historical backfill.

Backfill must not bypass:

- completeness checks
- duplicate handling
- unit validation
- business validation

Backfill quality reporting should identify problematic:

- countries
- datasets
- dates

so individual ranges can be reprocessed.

## Observability

Each run should expose quality metrics including:

- source requests attempted
- responses successful
- complete source periods
- partial source periods
- unavailable periods
- failed periods
- records received
- records rejected
- duplicates removed
- overlaps detected
- null counts for key measurements
- countries represented
- countries missing
- dbt tests passed
- dbt tests failed
- serving validation result

Quality state must be inspectable from Databricks execution history/logging.

## Acceptance Criteria

This specification is complete when:

1. Source API errors are distinguishable from valid empty/unavailable data.
2. ENTSO-E XML structure is validated.
3. ENTSO-E completeness is based on reconstructed timeline coverage.
4. Multiple Period elements are handled correctly.
5. Gaps between ENTSO-E Periods are detectable.
6. Point count alone is not used as the universal completeness rule.
7. Partial ENTSO-E days do not produce misleading trusted daily metrics.
8. Duplicate and overlapping source observations are resolved.
9. Open-Meteo daily payload structure is validated.
10. Requested and returned weather coordinates are handled correctly.
11. Missing measurements are not converted to zero.
12. Silver datasets enforce required unit and measurement rules.
13. Unknown production types are surfaced.
14. Gold models enforce documented grains.
15. Required dbt uniqueness and relationship tests exist.
16. Renewable-generation calculations are validated.
17. Generation totals reconcile across Gold models.
18. Missing/partial country data remains observable.
19. Critical quality failures block PostgreSQL publication.
20. Partial but safely represented data does not unnecessarily fail the entire workflow.
21. PostgreSQL publication is validated before and after writes.
22. Previously partial dates can become complete after later reprocessing.
23. Quality results are visible through Databricks workflow execution.

## Out of Scope

This specification does not include:

- Monte Carlo
- Great Expectations
- dedicated enterprise observability platforms
- machine-learning anomaly detection
- real-time data-quality monitoring
- formal SLA management
- PagerDuty/Slack alerting