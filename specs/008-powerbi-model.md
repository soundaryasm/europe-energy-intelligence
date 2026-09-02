# 008 — Power BI Model

## Scope Note

This specification exists to document the **end-to-end picture**: what a
BI consumer of this project's PostgreSQL serving layer looks like, so
the overall architecture (API -> Databricks -> PySpark -> dbt ->
PostgreSQL -> BI) is legible as a whole.

**Power BI itself is out of scope for this repository.** It is built and
maintained separately, by whoever owns the BI layer, against the
PostgreSQL contract defined in Spec 005. This repository's delivery ends
at PostgreSQL serving — nothing here builds, deploys, or ships a
Power BI report, `.pbix` file, or dataset.

The remainder of this document describes the semantic model, measures,
and pages a downstream Power BI consumer *should* build against the
serving layer, as a reference contract — not as work this repository is
responsible for delivering.

## Goal

Describe a Power BI semantic model and dashboard built on top of the
PostgreSQL serving layer, for context on the end-to-end picture. Building
this (in Power BI Desktop, against a live PostgreSQL connection) is a
separate, downstream effort — see "Scope Note" above.

Power BI is the final consumption layer.

It must not connect directly to Databricks Bronze, Silver, or Gold datasets.

## Source

Power BI must connect to PostgreSQL.

Approved serving tables:

- `dim_country`
- `dim_date`
- `fact_energy_daily`
- `fact_weather_daily`
- `fact_generation_mix_daily`

PostgreSQL is the BI-facing contract.

## Connectivity

Use the native PostgreSQL connector.

For the MVP, prefer Import mode unless DirectQuery provides a clear benefit.

The dataset is intentionally small enough that Import mode should be sufficient.

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

Avoid embedding important business logic directly inside visuals.

Reusable logic should live in Power BI measures.

## Main Dashboard

Create a primary page:

`European Energy Overview`

The page should provide a fast summary across all five countries.

Include:

- latest available date
- total demand
- average electricity price
- renewable generation %
- total generation

Provide country and date filters.

## Country Comparison

Create a page:

`Country Comparison`

Allow users to compare:

- electricity demand
- average price
- renewable share
- generation mix
- temperature
- wind speed

across:

- Ireland
- Germany
- France
- Spain
- Netherlands

Visuals should prioritize comparison rather than decorative complexity.

## Energy Trends

Create a page:

`Energy Trends`

Include daily trends for:

- demand
- day-ahead price
- renewable percentage
- total generation

Users should be able to select:

- country
- date range

## Generation Mix

Create a page:

`Generation Mix`

Show:

- generation by production type
- renewable vs non-renewable contribution
- generation-share percentage

Support country and date filtering.

Do not hide meaningful smaller generation categories purely for visual simplicity.

## Weather & Energy

Create a page:

`Weather & Energy`

The goal is to visually explore relationships between weather and energy metrics.

Include relationships such as:

- temperature vs electricity demand
- wind speed vs renewable generation
- solar radiation vs solar generation

This page is exploratory.

Do not imply causation purely from visual correlation.

## Refresh Metadata

The dashboard must visibly communicate freshness.

At minimum show:

- latest available data date
- last successful serving refresh timestamp if available

Users should be able to tell whether they are viewing current or stale data.

## Date Behaviour

Use `dim_date` as the primary date dimension.

Mark it as the model's date table where appropriate.

Use it consistently for:

- filtering
- time intelligence
- trend visuals

Do not create disconnected date logic independently in each fact table.

## Country Behaviour

Use `dim_country` as the single country dimension.

Country names/codes should come from the dimension rather than repeated fact-table columns where possible.

## DAX

Use DAX for:

- reusable measures
- time intelligence
- presentation-level calculations

Do not recreate transformations in DAX that properly belong in Silver or dbt Gold.

Examples of logic that should stay upstream:

- renewable classification
- generation normalization
- demand energy calculation
- country/date keys
- source cleaning

## Visual Design

Prioritize:

- readability
- consistent units
- clear labels
- restrained number of visuals
- meaningful comparisons
- obvious filters

Avoid:

- excessive gauges
- unnecessary 3D visuals
- decorative charts with little analytical value
- duplicated KPIs across every page

## Units

Display units clearly.

Examples:

- demand: MWh or GWh where appropriate
- generation: MWh or GWh
- price: EUR/MWh
- renewable share: %
- temperature: °C
- wind speed: km/h
- solar radiation: MJ/m²

Use consistent unit conventions across pages.

## Performance

The serving dataset is intentionally compact.

Avoid unnecessary calculated columns where measures are more appropriate.

Keep the semantic model simple.

Do not introduce aggregations or composite models unless actual performance requires them.

## Data Quality Visibility

The dashboard should not silently present known-invalid data.

Only data successfully published through the validated PostgreSQL serving layer should be consumed.

Where useful, expose freshness or availability indicators.

## Portfolio Goal

The Power BI output should demonstrate that the engineering pipeline produces usable business-facing analytics.

It should be understandable within roughly 30 seconds by someone viewing the portfolio.

The dashboard should visually reinforce the underlying engineering story:

API data
→ Databricks
→ PySpark
→ dbt
→ PostgreSQL
→ Power BI

## Acceptance Criteria

This specification is complete when:

1. Power BI connects successfully to PostgreSQL.
2. All approved serving tables are loaded.
3. Dimension/fact relationships are configured correctly.
4. Core reusable measures exist.
5. An overview page is implemented.
6. Country comparison is implemented.
7. Energy trend analysis is implemented.
8. Generation mix analysis is implemented.
9. Weather-energy analysis is implemented.
10. Country/date filtering works consistently.
11. Data freshness is visible.
12. Business logic is not unnecessarily duplicated in Power BI.
13. The dashboard is clear enough for portfolio demonstration.
14. Power BI does not directly query Databricks.

## Out of Scope

This specification does not include:

- building, deploying, or maintaining the actual Power BI report/`.pbix`
  from this repository (see "Scope Note" above — that is a separate,
  downstream effort)
- Power BI Premium
- Fabric
- streaming dashboards
- write-back
- row-level security
- embedded Power BI
- mobile-specific layouts
- machine-learning predictions