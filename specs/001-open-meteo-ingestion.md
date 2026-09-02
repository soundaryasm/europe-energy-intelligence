# 001 — Open-Meteo Ingestion

## Goal

Ingest weather data for the five MVP countries into Databricks and maintain a reliable historical + daily weather dataset.

The analytical output of this pipeline is daily by country.

## Countries

The MVP covers:

- Ireland
- Germany
- France
- Spain
- Netherlands

Each country must have one configured reference location.

For MVP, use the capital city as the weather proxy:

| Country | Reference Location |
|---|---|
| Ireland | Dublin |
| Germany | Berlin |
| France | Paris |
| Spain | Madrid |
| Netherlands | Amsterdam |

Coordinates must be defined in configuration and must not be duplicated throughout application code.

This is a deliberate MVP simplification.

Weather values represent the configured reference location and must not be described as a geographically averaged national weather measurement.

## Source

Use the Open-Meteo Historical Weather API for historical weather ingestion.

Base endpoint:

`https://archive-api.open-meteo.com/v1/archive`

Open-Meteo requires latitude and longitude and supports explicit start and end dates.

No API key is required for the public non-commercial API used by this project.

## Required Weather Metrics

The resulting daily dataset must provide:

- average temperature
- average wind speed
- solar radiation

Target units:

- temperature: Celsius
- wind speed: km/h
- solar radiation: MJ/m²

## Source Granularity

The project's analytical grain remains daily.

However, Open-Meteo's Historical Weather API does not currently expose daily mean temperature and daily mean wind speed as documented daily variables.

Therefore this ingestion is explicitly allowed to retrieve hourly:

- `temperature_2m`
- `wind_speed_10m`

Daily averages will be calculated downstream using PySpark.

For solar radiation, retrieve:

- `shortwave_radiation_sum`

as a daily variable from Open-Meteo.

Hourly source data exists only because it is required to derive the approved daily metrics.

Hourly analytics are NOT part of the MVP.

## Historical Backfill

The initial execution must support a configurable historical backfill covering the previous 24 months.

The date range must not be permanently hard-coded.

The ingestion implementation should accept:

- `start_date`
- `end_date`

so the same code can be reused for both historical and incremental execution.

## Daily Incremental Ingestion

After the initial backfill, the pipeline will run daily as part of the Databricks workflow.

The daily execution must ingest the latest completed day's weather data.

The ingestion must support rerunning a previously processed date.

A rerun must not cause duplicate downstream records.

## Bronze Storage

Raw weather data must be persisted in Databricks Delta storage.

The Bronze layer should preserve source-level information with minimal transformation.

Bronze data must include enough information to identify:

- country
- reference location
- latitude
- longitude
- timestamp/date
- source variable
- source value
- ingestion timestamp
- source system

The source system value should identify Open-Meteo.

Do not publish Bronze data to PostgreSQL.

## Execution Environment

This pipeline MUST run on Databricks.

Local execution is not supported.

Python may be used for HTTP communication and control flow.

PySpark must be used for DataFrame-based processing and writing persisted Delta datasets.

Do not use pandas as the processing implementation.

## API Behaviour

The implementation must:

- use HTTPS
- define an explicit request timeout
- validate HTTP response status
- fail clearly when the API request fails
- avoid silently writing incomplete responses
- log the country and requested date range being processed

Transient HTTP failures should support limited retry behaviour.

Retries must be bounded.

## Configuration

Country metadata must be separated from ingestion logic.

Configuration should contain at minimum:

- country code
- country name
- reference location
- latitude
- longitude
- timezone

Application logic must iterate over configured countries rather than contain five duplicated country-specific implementations.

## Timezones

Requests and transformations must preserve enough timezone information to associate observations with the correct local calendar date.

Do not assume every country uses the same timezone.

Daily aggregation must ultimately represent the local date associated with each configured country.

## Idempotency

The ingestion design must permit the same country/date range to be processed multiple times safely.

Repeated ingestion must not result in duplicate logical records after the pipeline completes.

The implementation should use deterministic business keys where applicable.

## Observability

Each execution should make it possible to determine:

- execution start time
- execution end time
- requested date range
- countries attempted
- countries successfully ingested
- countries failed
- number of records written

Failures must be visible rather than silently ignored.

## Acceptance Criteria

This specification is complete when:

1. The ingestion runs successfully on Databricks.
2. Data is retrieved from Open-Meteo for all five configured countries.
3. A configurable 24-month historical backfill can be executed.
4. Individual dates can also be ingested for daily incremental processing.
5. Raw source data is persisted as Delta data in the Databricks Bronze layer.
6. Temperature and wind observations required for downstream daily averages are available.
7. Daily solar radiation is available.
8. Country and reference-location metadata are retained.
9. The process can be safely rerun without producing duplicate logical data.
10. API failures are surfaced clearly.
11. No runtime dependency on the developer's local machine exists.

## Out of Scope

This specification does not include:

- ENTSO-E ingestion
- daily weather aggregation
- Silver transformations
- dbt models
- PostgreSQL publishing
- Power BI
- hourly analytical reporting
- multiple weather locations per country
- weather forecasting