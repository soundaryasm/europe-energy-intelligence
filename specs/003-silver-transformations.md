# 003 — Silver Transformations

## Goal

Transform Bronze ENTSO-E and Open-Meteo data into clean, normalized, trusted daily datasets suitable for dbt analytical modelling.

The Silver layer is responsible for:

- schema normalization
- source-unit normalization
- timestamp and timezone handling
- interval-to-energy conversion
- daily aggregation
- completeness validation
- duplicate handling
- production-type normalization
- basic data-quality enforcement

Silver remains inside Databricks.

No Silver dataset is published directly to PostgreSQL.

## Execution Environment

All transformations MUST execute on Databricks using PySpark.

Do not use:

- pandas as the transformation engine
- a local Spark runtime
- local databases

Persist trusted Silver datasets as Delta tables.

## Canonical Grain

The MVP analytical grain is daily.

Primary grain:

`country + local_date`

Generation mix grain:

`country + local_date + production_type`

Sub-daily ENTSO-E observations exist only as source data required to derive trustworthy daily metrics.

Hourly analytics are outside the MVP.

## Required Silver Datasets

Create:

- `silver_energy_demand_daily`
- `silver_energy_price_daily`
- `silver_generation_mix_daily`
- `silver_weather_daily`

Where useful, maintain separate invalid/partial-data datasets rather than mixing untrusted observations into these trusted Silver outputs.

## Source Timestamp Handling

ENTSO-E source timestamps are treated as UTC.

For each observation:

1. derive the timestamp from its containing Period
2. preserve the UTC timestamp
3. convert it to the configured country timezone
4. derive `local_date` from the localized timestamp

Do not derive local dates from the API request boundaries.

Do not assume a UTC request day corresponds exactly to one local calendar day.

## ENTSO-E Interval Duration

Parse the source resolution into interval duration.

Examples:

- `PT15M` → 0.25 hours
- `PT30M` → 0.50 hours
- `PT60M` → 1.00 hour

Do not hard-code one expected resolution.

Unsupported or malformed resolutions must fail validation rather than being silently interpreted.

## Electricity Demand

Source:

ENTSO-E actual total load.

ENTSO-E load observations represent power.

Normalize source unit `MAW` to:

`MW`

Convert every valid source interval into energy:

`interval_energy_mwh = load_mw × interval_duration_hours`

Daily demand:

`daily_demand_mwh = SUM(interval_energy_mwh)`

Required trusted output:

- country code
- local date
- daily demand MWh
- source interval count
- covered duration
- completeness status
- source system

## Demand Completeness

Daily demand must only be considered trusted when the complete local-day timeline is represented.

Completeness validation must consider:

- source timestamps
- interval resolution
- actual interval duration
- gaps between Period elements
- overlapping observations
- timezone conversion
- daylight-saving transitions

Do not determine completeness solely from Point count.

Examples such as:

`96 points = complete`

must not be universally hard-coded.

A normal 15-minute day may contain 96 intervals, but local DST days may legitimately represent different durations.

If a daily demand timeline is partial:

- classify it as `partial`
- do not silently extrapolate missing intervals
- do not treat missing intervals as zero
- do not publish an understated value as trusted daily demand

Partial observations should remain diagnosable and capable of being corrected through later reprocessing.

## Day-Ahead Price

Source:

ENTSO-E day-ahead prices.

Normalize prices to:

`EUR/MWh`

Where source resolutions vary, calculate daily price using interval-duration weighting:

`weighted_price = SUM(price × interval_duration) / SUM(interval_duration)`

Also calculate:

- minimum price
- maximum price

Required trusted output:

- country code
- local date
- average day-ahead price EUR/MWh
- minimum price
- maximum price
- interval count
- covered duration
- completeness status
- source system

Negative electricity prices are valid.

Never reject or convert them to zero.

## Price Completeness

A daily price metric should only be treated as complete when the expected daily pricing timeline is represented.

Missing pricing intervals must not be silently ignored while presenting the resulting average as a complete daily value.

Partial price data must remain distinguishable from complete data.

## Generation Mix

Source:

ENTSO-E actual generation by production type.

Normalize power units to MW where required.

For every valid interval:

`generation_mwh = generation_mw × interval_duration_hours`

Aggregate to:

`country + local_date + normalized_production_type`

Required output:

- country code
- local date
- raw production type
- normalized production type
- generation MWh
- renewable flag
- completeness status
- source system

## Generation Completeness

Completeness must be evaluated using timeline coverage for each relevant production-type series.

Do not assume all production types necessarily contain identical Point counts without validation.

Missing intervals must not automatically become zero generation.

A production type with genuinely reported zero generation is different from an absent observation.

## Production-Type Normalization

Maintain production-type mappings centrally in configuration.

Retain:

- raw ENTSO-E production type
- normalized project production type

Normalized categories should include meaningful groups such as:

- wind
- solar
- nuclear
- gas
- coal
- hydro
- biomass
- oil
- other

Do not implement classification through scattered string-matching logic.

Previously unseen source production types must be surfaced.

They may be temporarily mapped to `other`, but the occurrence must remain observable.

## Renewable Classification

Maintain renewable classification centrally alongside production-type mapping.

Examples of renewable categories include:

- wind
- solar
- hydro
- biomass where approved by project mapping

Do not determine renewable status dynamically from display text.

The Silver layer must provide the inputs necessary for Gold to calculate:

`renewable_generation_mwh`

and:

`renewable_generation_pct`

## Open-Meteo Weather

Open-Meteo Bronze data is already daily for the MVP.

Silver should therefore normalize and validate rather than re-aggregate hourly weather observations.

Required output:

- country code
- local date
- average temperature °C
- average wind speed km/h
- solar radiation MJ/m²
- reference location
- source system

Use the approved daily Open-Meteo measurements:

- mean temperature
- mean wind speed
- shortwave radiation sum

Do not introduce hourly weather processing unless a future specification explicitly requires it.

## Requested vs Returned Weather Coordinates

The weather transformation may retain lineage information relating to:

- configured reference coordinates
- returned Open-Meteo grid coordinates

The analytical meaning remains:

weather for the configured reference location proxy.

Returned grid coordinates must not redefine country/reference-location configuration.

## Duplicate Handling

Silver transformations must deterministically resolve duplicate logical source observations.

Example business keys:

### Load

- country
- source timestamp
- dataset type

### Price

- country
- source timestamp
- dataset type

### Generation

- country
- source timestamp
- production type
- dataset type

### Weather

- country
- local date

When multiple valid versions of the same logical source observation exist, prefer the most recently ingested valid version.

This supports source revisions and reruns.

## Overlap Handling

ENTSO-E responses may contain overlapping periods or previously ingested observations.

Overlapping observations must not be double-counted.

Deduplication must occur before daily aggregation.

## Null Handling

Never automatically convert missing measurements into zero.

Examples:

- missing demand != zero demand
- missing generation != zero generation
- missing price != zero price
- missing weather != zero weather

Required identifiers must not be null.

Invalid or incomplete records must not silently enter trusted Silver output.

## Trusted vs Partial Data

Trusted Silver daily datasets should represent analytically valid daily values.

When a source day is partial or invalid:

- preserve diagnostic information
- mark the source period appropriately
- exclude the incomplete metric from trusted downstream use

The pipeline must allow later reprocessing to replace a previously partial day once complete source data becomes available.

## Data Types

Use explicit schemas wherever practical.

Use:

- `DATE` for local date
- timestamp types for source timestamps
- numeric/decimal types for measurements
- stable string codes for countries and production types

Avoid depending solely on automatic schema inference for persisted datasets.

## Idempotency

Silver processing must be safely rerunnable.

Reprocessing the same country/date must reconcile existing Silver state.

Use Delta `MERGE`, deterministic replacement, or an equivalent idempotent strategy.

A previously partial date that later becomes complete must be capable of replacing the earlier state cleanly.

## Data Quality

### Demand

Validate:

- known country
- valid source timestamp
- valid local date
- supported resolution
- non-negative load
- positive interval duration
- no unexplained overlaps
- complete coverage before trusted daily publication

### Prices

Validate:

- known country
- valid date
- numeric price
- valid interval duration
- complete coverage before trusted daily publication

Negative prices are valid.

### Generation

Validate:

- known country
- valid date
- production type present
- normalized mapping available
- renewable classification available
- generation non-negative unless source semantics explicitly justify otherwise
- timeline completeness

### Weather

Validate:

- known country
- date present
- temperature numeric
- wind speed non-negative
- solar radiation non-negative
- required daily fields present

Weather plausibility bounds should be configurable and intended to detect obvious source/parser errors.

## Completeness Status

Use a consistent logical status where completeness information is persisted.

Recommended values:

- `complete`
- `partial`
- `unavailable`
- `invalid`

Do not use a generic success boolean when the distinction matters.

## Performance

The MVP dataset is small.

Do not introduce complex Spark optimization or partitioning merely to demonstrate Spark knowledge.

Prefer:

- clear transformations
- explicit schemas
- deterministic calculations
- understandable execution plans

Optimize only when observed behaviour justifies it.

## Observability

Each transformation run should expose:

- input record count
- output record count
- duplicate observations removed
- overlapping observations detected
- partial country/dates detected
- invalid records
- countries processed
- dates processed
- execution duration

For ENTSO-E datasets, quality information should make missing timeline coverage easy to identify.

## Acceptance Criteria

This specification is complete when:

1. ENTSO-E Point timestamps are correctly derived from their containing Period.
2. UTC source timestamps are correctly mapped to country-local dates.
3. Source resolutions are converted into correct interval durations.
4. Load MW values are correctly converted into MWh.
5. Complete daily demand is calculated correctly.
6. Partial demand days are not presented as complete trusted metrics.
7. Day-ahead prices produce duration-weighted daily metrics.
8. Negative prices remain valid.
9. Generation values are converted from MW to MWh correctly.
10. Generation is aggregated daily by normalized production type.
11. Production-type mappings and renewable classifications are centralized.
12. Open-Meteo daily weather values are normalized without unnecessary hourly aggregation.
13. Duplicate and overlapping source observations are not double-counted.
14. Missing observations are not converted to zero.
15. Complete and partial source days remain distinguishable.
16. Reprocessing can replace previously incomplete data.
17. Trusted Silver outputs are persisted as Delta tables.
18. No Silver data is published directly to PostgreSQL.
19. No local runtime dependency exists.

## Out of Scope

This specification does not include:

- API ingestion
- dbt Gold models
- dimensional modelling
- PostgreSQL publishing
- Power BI
- forecasting
- streaming
- hourly analytical reporting