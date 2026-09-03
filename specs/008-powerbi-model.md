# 008 — Power BI Model

## Goal

Build a Power BI semantic model and dashboard on top of the Aiven PostgreSQL serving layer.

Power BI is the final consumption layer.

It must not connect directly to Databricks Bronze, Silver, or Gold datasets.

## Source

Power BI must connect to Aiven PostgreSQL.

Approved serving tables:

- `dim_country`
- `dim_date`
- `fact_energy_daily`
- `fact_weather_daily`
- `fact_generation_mix_daily`

PostgreSQL is the BI-facing contract.

## Connectivity

Use the native PostgreSQL connector.

For the MVP, prefer Import mode unless DirectQuery provides a clear demonstrated benefit.

The serving dataset is intentionally compact enough for Import mode.

## Semantic Model

Create relationships:

`dim_country`
→ `fact_energy_daily`

`dim_country`
→ `fact_weather_daily`

`dim_country`
→ `fact_generation_mix_daily`

`dim_date`
→ all fact tables

Use one-to-many relationships from dimensions to facts.

Avoid unnecessary bidirectional relationships.

## Core Measures

Create reusable measures for at least:

- Total Demand MWh
- Average Day-Ahead Price EUR/MWh
- Total Generation MWh
- Renewable Generation MWh
- Renewable Generation %
- Average Temperature °C
- Average Wind Speed km/h
- Solar Radiation MJ/m²

Where useful, also create:

- Previous Day Demand
- Demand Change %
- Previous Day Price
- Price Change %
- 7-Day Average Price
- 7-Day Average Demand

Important business logic should be implemented as reusable measures rather than duplicated inside visuals.

## Null Semantics

Power BI must preserve the distinction between:

- valid zero
- missing measurement
- unavailable source data
- incomplete source data

Do not replace null measurements with zero merely for visual convenience.

Examples:

- unavailable demand != zero demand
- unavailable generation != zero generation
- missing price != zero price
- missing weather != zero weather

Visuals and measures must avoid presenting missing values as genuine zero measurements.

## Incomplete Data

The upstream pipeline may intentionally leave some metrics null when source data is incomplete.

Power BI must respect those semantics.

For example:

If a country/date has incomplete ENTSO-E demand coverage, the dashboard must not display an understated demand value as though it were complete.

Where appropriate, visuals may:

- omit incomplete metric values
- display blank values
- indicate unavailable data

Do not infer or interpolate missing business metrics in Power BI.

## Main Dashboard

Create a primary page:

`European Energy Overview`

The page should provide a fast summary across all five countries.

Include:

- latest complete analytical date
- total demand
- average electricity price
- renewable generation %
- total generation

Provide:

- country filter
- date filter

The page should be understandable within roughly 30 seconds.

## Country Comparison

Create:

`Country Comparison`

Allow comparison across:

- Ireland
- Germany
- France
- Spain
- Netherlands

Metrics should include:

- electricity demand
- average electricity price
- renewable generation %
- total generation
- temperature
- wind speed

Country comparisons must not treat missing values as zero.

## Energy Trends

Create:

`Energy Trends`

Include daily trends for:

- demand
- day-ahead price
- renewable generation %
- total generation

Users should be able to filter by:

- country
- date range

Trend lines should naturally show gaps where trusted measurements are unavailable rather than fabricating values.

## Generation Mix

Create:

`Generation Mix`

Show:

- generation by production type
- renewable vs non-renewable generation
- generation-share percentage

Support:

- country filtering
- date filtering

Do not hide meaningful smaller production types solely for decorative simplicity.

Generation percentages should only be interpreted for dates where the upstream generation dataset is sufficiently complete.

## Weather & Energy

Create:

`Weather & Energy`

Use this page to explore relationships such as:

- temperature vs electricity demand
- wind speed vs renewable generation
- solar radiation vs solar generation

This page is exploratory.

Do not imply causation from correlation alone.

Missing measurements should be excluded from correlation-style visuals rather than replaced with zero.

## Latest Complete Analytical Date

The dashboard must distinguish between:

- latest workflow execution
- latest PostgreSQL publication
- latest source date
- latest complete analytical date

The main dashboard should primarily communicate the:

`latest complete analytical date`

for trusted metrics.

Do not label a partially available date as the latest complete dataset.

## Refresh Metadata

The dashboard should visibly expose freshness.

Where available, show:

- latest complete analytical date
- last successful PostgreSQL publication timestamp
- last successful pipeline refresh timestamp

These values must not be treated as equivalent.

A pipeline may run successfully without receiving complete new source data.

## Data Availability Indicators

Where useful, expose simple data-availability indicators.

Potential statuses:

- complete
- partial
- unavailable

Do not overwhelm the dashboard with engineering metadata.

The primary purpose remains analytical consumption, but users should be able to understand why a metric is blank.

## Date Behaviour

Use `dim_date` as the canonical date dimension.

Mark it as the model's date table where appropriate.

Use it consistently for:

- filtering
- time intelligence
- daily trends
- previous-day calculations
- rolling calculations

Do not create independent date dimensions in individual fact tables.

## Time Intelligence

Time-intelligence measures must account for missing analytical days.

For example:

A previous-day comparison should not blindly interpret missing previous-day data as zero.

Where a comparison cannot be calculated because a required trusted metric is absent, return blank rather than misleading percentage changes.

## Country Behaviour

Use `dim_country` as the single country dimension.

Country names, codes, timezone, and reference location should come from the dimension.

Avoid maintaining separate country lookup logic inside individual reports.

## DAX Responsibilities

Use DAX for:

- reusable measures
- time intelligence
- presentation-layer calculations
- comparisons
- rolling averages

Do not recreate upstream data engineering logic in DAX.

The following belong upstream:

- MW-to-MWh conversion
- interval-duration handling
- renewable classification
- production-type normalization
- completeness determination
- country/date keys
- source cleaning

## Metric Units

Display units clearly and consistently.

Examples:

- demand: MWh or GWh
- generation: MWh or GWh
- price: EUR/MWh
- renewable share: %
- temperature: °C
- wind speed: km/h
- solar radiation: MJ/m²

Power BI may scale MWh to GWh for readability, but underlying measure semantics must remain clear.

## Negative Prices

Negative electricity prices are valid.

Visuals must support displaying:

- positive prices
- zero prices
- negative prices

Do not clamp negative values to zero.

## Renewable Percentage

Renewable percentage should display only where upstream calculation is valid.

Do not calculate a fallback renewable percentage independently in Power BI if the Gold model already provides the approved business metric.

## Generation Share

Use the Gold model's approved generation-share semantics.

Do not independently recompute generation shares using incomplete subsets of generation data.

## Visual Design

Prioritize:

- readability
- restrained number of visuals
- consistent units
- clear titles
- meaningful comparisons
- useful filters
- obvious date context

Avoid:

- unnecessary gauges
- decorative 3D charts
- excessive visual density
- duplicated KPIs
- misleading zero-filled charts

## KPI Behaviour

KPI cards should return blank or an explicit unavailable state when the relevant trusted metric does not exist.

Do not display:

`0`

simply because no valid metric was published.

## Tooltips

Where useful, tooltips may expose additional context such as:

- date
- country
- reference location
- source freshness

Do not expose raw technical implementation details unless they improve interpretation.

## Performance

The PostgreSQL serving dataset is intentionally compact.

Keep the semantic model simple.

Avoid unnecessary:

- calculated columns
- duplicate tables
- composite models
- aggregation tables

unless actual performance testing justifies them.

## Data Quality Protection

Power BI consumes only data that passed the approved serving pipeline.

Known-invalid Gold runs must not be published as current serving data.

Power BI should not contain corrective logic intended to compensate for failed upstream quality checks.

## Portfolio Goal

The dashboard should demonstrate that the engineering pipeline produces useful analytical outcomes.

A reviewer should quickly understand:

- European energy comparison
- renewable generation differences
- electricity-price behaviour
- demand trends
- weather relationships

The dashboard should complement the engineering project rather than become the entire project.

## Portfolio Narrative

The intended architecture visible from the portfolio is:

```text
ENTSO-E + Open-Meteo
        ↓
Databricks Bronze
        ↓
PySpark Silver
        ↓
dbt Gold
        ↓
Aiven PostgreSQL
        ↓
Power BI
```

The Power BI layer should make the final analytical value of that pipeline obvious.

## Acceptance Criteria

This specification is complete when:

1. Power BI connects successfully to Aiven PostgreSQL.
2. All approved serving tables are loaded.
3. Dimension/fact relationships are configured correctly.
4. Core reusable measures exist.
5. An overview page exists.
6. Country comparison exists.
7. Energy trend analysis exists.
8. Generation mix analysis exists.
9. Weather-energy exploration exists.
10. Country and date filters work consistently.
11. `dim_date` is used consistently for time intelligence.
12. Missing metrics are not silently converted to zero.
13. Incomplete upstream metrics are not presented as trusted values.
14. Negative electricity prices display correctly.
15. Latest complete analytical date is distinguishable from refresh timestamp.
16. Power BI does not recreate upstream engineering logic.
17. Power BI does not query Databricks directly.
18. The dashboard is understandable and portfolio-ready.

## Out of Scope

This specification does not include:

- Power BI Premium
- Microsoft Fabric
- streaming dashboards
- Direct Lake
- write-back
- row-level security
- embedded Power BI
- mobile-specific layouts
- machine-learning predictions
- Power BI-based data cleansing