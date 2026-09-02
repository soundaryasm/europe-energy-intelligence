# 003 — Silver Transformations

## Goal

Transform raw Bronze data from ENTSO-E and Open-Meteo into clean, standardized, daily datasets suitable for analytical modelling.

The Silver layer is responsible for:

- schema normalization
- type casting
- timestamp normalization
- daily aggregation
- unit normalization
- duplicate handling
- basic data-quality validation
- production-type normalization

Silver remains inside Databricks.

No Silver dataset is published to PostgreSQL.

## Execution Environment

All transformations MUST execute on Databricks using PySpark.

Do not use:

- pandas as the transformation engine
- local Python execution
- local databases

Persist Silver datasets as Delta tables.

## Canonical Grain

The MVP analytical grain is:

`country + local_date`

Generation mix additionally includes:

`country + local_date + production_type`

Hourly or sub-hourly source records may exist in Bronze but must be aggregated into daily Silver datasets.

## Required Silver Datasets

Create the following logical datasets:

- `silver_energy_demand_daily`
- `silver_energy_price_daily`
- `silver_generation_mix_daily`
- `silver_weather_daily`

Physical catalog/schema naming should follow the project's Databricks environment conventions.

## Electricity Demand

Source:

ENTSO-E actual total load.

Source values represent power, typically MW, at a specific interval resolution.

Daily electricity demand must represent ENERGY consumed rather than a simple sum of MW observations.

For every source interval:

`energy_mwh = load_mw × interval_duration_hours`

Examples:

- 60-minute interval → `MW × 1.0`
- 30-minute interval → `MW × 0.5`
- 15-minute interval → `MW × 0.25`

Daily demand:

`daily_demand_mwh = SUM(interval_energy_mwh)`

Expose:

- country code
- local date
- daily demand MWh
- number of source intervals
- source system

Do not assume every day contains exactly 24 hourly observations.

## Day-Ahead Price

Source:

ENTSO-E day-ahead electricity prices.

Prices must be normalized to:

`EUR/MWh`

Daily price should be calculated as an interval-duration-weighted average where source resolutions differ.

Expose:

- country code
- local date
- average day-ahead price EUR/MWh
- minimum price
- maximum price
- number of source intervals
- source system

Negative electricity prices are valid and must not be discarded.

## Generation Mix

Source:

ENTSO-E actual generation by production type.

Convert power observations into energy using the interval duration:

`generation_mwh = generation_mw × interval_duration_hours`

Aggregate to:

`country + local_date + normalized_production_type`

Expose:

- country code
- local date
- raw production type
- normalized production type
- generation MWh
- renewable flag
- source system

## Production-Type Normalization

Maintain production-type mapping centrally.

Do not embed production-type classification rules throughout transformation code.

The normalized categories should retain meaningful distinctions such as:

- wind
- solar
- nuclear
- gas
- coal
- hydro
- biomass
- oil
- other

Where ENTSO-E provides more detailed categories, retain the raw source value alongside the normalized category.

## Renewable Classification

Each normalized production type must have an explicit renewable/non-renewable classification.

Renewable classification rules must be centralized and documented.

Do not infer renewable status dynamically from text matching.

Daily renewable percentage will be calculated from generation data using:

`renewable_generation_mwh / total_generation_mwh × 100`

The required inputs for this calculation must be available in Silver.

## Weather

Source:

Open-Meteo.

Aggregate source observations into one daily record per country/reference location.

Expose:

- country code
- local date
- average temperature Celsius
- average wind speed km/h
- solar radiation MJ/m²
- number of temperature observations
- number of wind observations
- reference location
- source system

Temperature:

`daily_avg_temperature = AVG(hourly temperature_2m)`

Wind:

`daily_avg_wind_speed = AVG(hourly wind_speed_10m)`

Solar radiation:

Use the daily `shortwave_radiation_sum` supplied by Open-Meteo.

## Timezone Handling

Daily aggregation must use each country's configured local timezone.

Source timestamps must be converted to the appropriate local date before daily aggregation.

The implementation must correctly handle daylight-saving-time transitions.

Never assume:

`1 day = 24 source intervals`

Completeness checks must account for the actual source resolution and timezone behaviour.

## Duplicate Handling

Silver transformations must deterministically remove duplicate logical source observations.

Duplicate identification should use appropriate business keys such as:

- country
- source timestamp
- dataset type
- production type where applicable

When multiple versions of the same logical observation exist, prefer the most recently ingested valid source record.

## Null Handling

Required analytical fields must be validated before Silver persistence.

Do not silently convert missing numeric values to zero.

Zero and null have different meanings.

Records that cannot safely contribute to analytical calculations should:

- be excluded from the valid Silver output
- or be explicitly flagged

The chosen behaviour must be observable.

## Data Types

Use explicit schemas wherever practical.

Dates:

`DATE`

Timestamps:

Databricks-supported timestamp type

Numeric measurements:

appropriate numeric/decimal types

Country identifiers:

stable string codes

Avoid relying on automatic schema inference for persisted production datasets.

## Idempotency

Silver transformations must be safely rerunnable.

Reprocessing the same source dates must update/replace the corresponding logical Silver records rather than append duplicates.

Use Delta capabilities such as MERGE or equivalent deterministic overwrite strategies where appropriate.

## Data Quality

At minimum validate:

### Demand

- country is known
- date is present
- demand is not negative
- interval duration is valid

### Prices

- country is known
- date is present
- price is numeric
- negative prices are allowed

### Generation

- country is known
- date is present
- production type is known or explicitly mapped to `other`
- generation must not be negative unless the source semantics explicitly justify it

### Weather

- country is known
- date is present
- temperature is within plausible configured bounds
- wind speed is non-negative
- solar radiation is non-negative

Quality-rule failures must be visible.

## Partitioning and Performance

Do not introduce complex partitioning purely to demonstrate Spark features.

The MVP dataset is relatively small.

Optimize only where justified by observed query or processing behaviour.

Prefer understandable transformations over premature optimization.

## Observability

Each transformation execution should expose:

- input record count
- output record count
- duplicates removed
- invalid records rejected/flagged
- countries processed
- dates processed
- execution duration

## Acceptance Criteria

This specification is complete when:

1. Bronze ENTSO-E load data produces daily demand in MWh.
2. Source interval duration is correctly incorporated into demand calculations.
3. Bronze price data produces daily price statistics.
4. Negative prices remain valid.
5. Generation data is aggregated daily by production type.
6. Production types are normalized through centralized configuration.
7. Renewable classification is available.
8. Weather data produces daily temperature, wind, and solar metrics.
9. All daily aggregation respects local country dates.
10. Duplicate source observations do not create duplicate Silver records.
11. Silver outputs are persisted as Delta tables.
12. Reprocessing dates is idempotent.
13. Data-quality failures are visible.
14. No Silver data is written to PostgreSQL.
15. No local runtime dependency exists.

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