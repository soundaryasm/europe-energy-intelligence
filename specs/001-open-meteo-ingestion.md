# 001 — Open-Meteo Ingestion

## Goal

Ingest weather data for the five MVP countries into Databricks and maintain a reliable historical + daily weather dataset.

The analytical grain for the MVP is daily by country.

## Countries

The MVP covers:

- Ireland
- Germany
- France
- Spain
- Netherlands

Use one configured reference location per country:

| Country | Reference Location |
|---|---|
| Ireland | Dublin |
| Germany | Berlin |
| France | Paris |
| Spain | Madrid |
| Netherlands | Amsterdam |

Coordinates and timezone must be maintained centrally in configuration.

Weather represents the configured reference location and must not be described as a geographically averaged national measurement.

## Source

Use Open-Meteo.

### Historical Backfill

Use the Historical Weather API:

`https://archive-api.open-meteo.com/v1/archive`

### Daily Incremental Load

For recently completed weather data, use the Open-Meteo Forecast API with its supported past-day functionality where appropriate.

Do not unnecessarily depend on historical/reanalysis data availability for the normal daily pipeline.

Historical and daily ingestion may use different Open-Meteo endpoints while producing the same canonical Bronze contract.

No API key is required for the approved public non-commercial usage.

## Required Weather Metrics

Retrieve daily:

- mean temperature at 2 metres
- mean wind speed at 10 metres
- shortwave radiation sum

Canonical output units:

- temperature: °C
- wind speed: km/h
- solar radiation: MJ/m²

Use Open-Meteo daily variables where available rather than retrieving hourly observations solely to calculate daily averages.

The MVP does not require hourly weather ingestion.

## Historical Backfill

The initial execution must support a configurable backfill covering the previous 24 months.

The implementation must accept:

- `start_date`
- `end_date`

The historical range must not be permanently hard-coded.

## Daily Incremental Ingestion

After backfill, ingestion runs as part of the daily Databricks workflow.

The normal execution should ingest the latest completed weather day required by the pipeline.

The implementation must also support explicitly reprocessing previous dates.

Reruns must not create duplicate logical records.

## Bronze Storage

Persist weather data in Databricks using Delta.

The Bronze dataset must retain sufficient information to identify:

- country
- country code
- configured reference location
- configured latitude
- configured longitude
- returned latitude
- returned longitude
- local date
- returned timezone
- configured timezone
- UTC offset where supplied
- mean temperature
- mean wind speed
- shortwave radiation sum
- source units
- source endpoint/type
- ingestion timestamp
- source system

The source system must identify Open-Meteo.

Do not publish Bronze data to PostgreSQL.

## Requested vs Returned Coordinates

Open-Meteo may return coordinates that differ slightly from the coordinates supplied in the request because the API resolves requests to an underlying weather-model grid cell.

This must not be treated as an ingestion failure.

Retain both:

- configured/reference-location coordinates
- coordinates returned by Open-Meteo

Do not overwrite the configured reference coordinates with returned grid coordinates.

## Timezone Behaviour

Every request must specify the configured timezone for the country/reference location.

Examples include:

- `Europe/Dublin`
- `Europe/Berlin`
- `Europe/Paris`
- `Europe/Madrid`
- `Europe/Amsterdam`

Daily observations must represent the corresponding local calendar date.

Do not assume Open-Meteo timestamps are UTC when an explicit timezone is requested.

Retain returned timezone and UTC-offset metadata where available.

## Execution Environment

This pipeline MUST execute on Databricks.

Local execution is not a supported production runtime.

Python may be used for:

- HTTP communication
- response parsing
- configuration
- control flow

PySpark must be used for persisted DataFrame processing and Delta writes where applicable.

Do not introduce pandas as the pipeline-processing implementation.

## API Behaviour

The implementation must:

- use HTTPS
- use explicit request timeouts
- validate HTTP status
- validate expected response structure
- detect API error responses
- avoid silently accepting malformed or incomplete responses
- log country and requested date range
- use bounded retries for transient failures

Retries must never be infinite.

## Response Validation

Open-Meteo responses use arrays for returned variables.

The implementation must verify that:

- required daily fields exist
- date arrays are present
- measurement arrays align with their corresponding date arrays
- required units are present or known
- requested dates are represented where data is expected

Do not independently process parallel arrays in a manner that could misalign dates and measurements.

## Configuration

Country metadata must be separate from ingestion logic.

Configuration must contain at minimum:

- country code
- country name
- reference location
- latitude
- longitude
- timezone

The ingestion implementation must iterate over configuration.

Do not create separate hard-coded implementations for each country.

## Idempotency

The same country/date may be processed multiple times safely.

Use a deterministic logical key equivalent to:

`country + local_date`

Repeated ingestion must update/reconcile the existing logical observation rather than create duplicates.

## Data Availability

The ingestion process must distinguish between:

- successful response with expected data
- valid response with data not yet available
- malformed response
- network/API failure

An empty or missing daily value must not automatically be converted to zero.

## Observability

Each execution should expose:

- execution start time
- execution end time
- requested date range
- countries attempted
- countries successful
- countries failed
- records received
- records written
- retries performed

Failures must remain visible in Databricks execution history.

## Acceptance Criteria

This specification is complete when:

1. Open-Meteo ingestion executes successfully on Databricks.
2. All five configured countries can be processed.
3. Daily mean temperature is retrieved.
4. Daily mean wind speed is retrieved.
5. Daily shortwave radiation sum is retrieved.
6. A configurable 24-month historical backfill can be executed.
7. Recent completed days can be ingested incrementally.
8. Individual historical dates can be reprocessed.
9. Data is persisted as Bronze Delta data.
10. Both configured and returned coordinates are retained.
11. Timezone behaviour preserves the correct local calendar date.
12. Reruns do not create duplicate logical observations.
13. API/data-availability failures are surfaced clearly.
14. No runtime dependency on the local development machine exists.

## Out of Scope

This specification does not include:

- ENTSO-E ingestion
- hourly weather analytics
- Silver transformations
- dbt models
- PostgreSQL publishing
- Power BI
- multiple weather locations per country
- forecasting as an analytical feature