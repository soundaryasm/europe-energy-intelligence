# 002 — ENTSO-E Energy Ingestion

## Goal

Ingest European electricity-market data from the ENTSO-E Transparency Platform into Databricks for the five MVP countries.

The pipeline must support:

- 24-month historical backfill
- daily incremental ingestion
- safe reruns
- Bronze Delta persistence

## Countries

The MVP covers:

- Ireland
- Germany
- France
- Spain
- Netherlands

ENTSO-E bidding-zone/domain identifiers must be maintained in configuration.

Do not scatter EIC/domain codes throughout ingestion logic.

Codes must be validated against ENTSO-E before being committed to production configuration.

## Source

Use the ENTSO-E Transparency Platform Web API.

Production API endpoint:

`https://web-api.tp.entsoe.eu/api`

Authentication must use the ENTSO-E security token.

The security token must be retrieved securely at runtime.

Never hard-code or commit the token.

## Required Datasets

The MVP requires three energy datasets.

### Actual Electricity Load

Retrieve actual total load.

ENTSO-E identifies actual total load using:

- Document type: `A65`
- Process type: `A16` — realised

The source may provide sub-daily resolutions such as hourly, 30-minute, or 15-minute intervals.

The source resolution must be preserved in Bronze.

Daily demand will be calculated downstream.

### Actual Generation by Production Type

Retrieve aggregated actual generation by production type.

ENTSO-E identifies this dataset using:

- Document type: `A75`
- Process type: `A16` — realised

Production types must be retained from the source so downstream processing can identify categories such as:

- wind
- solar
- nuclear
- gas
- hydro
- coal
- biomass
- other available ENTSO-E production types

Do not collapse production types during Bronze ingestion.

### Day-Ahead Electricity Prices

Retrieve day-ahead prices.

ENTSO-E identifies day-ahead prices using:

- Document type: `A44`

The appropriate bidding-zone domain must be supplied for both the input and output domain as required by the ENTSO-E API.

Source timestamps and prices must be preserved in Bronze.

Daily average price will be calculated downstream.

## Historical Backfill

The ingestion must support a configurable historical backfill covering the previous 24 months.

The implementation must accept:

- `start_date`
- `end_date`
- country / bidding zone
- dataset type

The 24-month range must not be permanently hard-coded into the ingestion implementation.

Large historical requests should be divided into bounded date windows rather than relying on a single multi-year request.

The chunking strategy must be configurable.

## Daily Incremental Ingestion

After historical backfill, the production workflow will execute daily.

The incremental execution should request the latest completed data period required by downstream processing.

Because ENTSO-E data can occasionally be revised after initial publication, rerunning recent dates must be supported.

Repeated ingestion must not create duplicate logical records.

## Bronze Storage

Each ENTSO-E dataset must be persisted in the Databricks Bronze layer using Delta.

Bronze should preserve the source structure as closely as practical.

At minimum, records must retain sufficient information to identify:

- country
- bidding zone / domain
- dataset type
- source timestamp
- source resolution
- value
- unit
- production type where applicable
- currency where applicable
- source document identifiers where available
- ingestion timestamp
- requested start date
- requested end date
- source system

The source system must identify ENTSO-E.

## Raw Response Handling

ENTSO-E Web API responses are XML-based.

The ingestion layer must safely parse the returned XML.

Unexpected response structures must fail visibly.

Do not silently interpret an ENTSO-E error response as an empty dataset.

Where useful for debugging and traceability, raw response metadata may be retained alongside parsed Bronze records.

## Execution Environment

This pipeline MUST execute on Databricks.

Local execution is not supported.

Python may be used for:

- HTTP requests
- XML parsing
- control flow
- configuration

PySpark must be used for DataFrame-based processing and persisted Delta writes.

Do not use pandas as the pipeline-processing implementation.

## Authentication

The ENTSO-E security token must be stored using Databricks-supported secret management.

The implementation must retrieve the secret at runtime.

The token must never appear in:

- source code
- Git history
- notebooks committed to GitHub
- logs
- exception messages

## API Behaviour

The implementation must:

- use HTTPS
- define an explicit request timeout
- validate HTTP status
- validate ENTSO-E response content
- use bounded retry behaviour for transient failures
- avoid infinite retries
- surface rate-limit or access errors clearly
- log the country, dataset type, and date range being requested

A failed country/dataset request must not be silently ignored.

## Configuration

Separate configuration from ingestion logic.

Configuration should include at minimum:

- country code
- country name
- ENTSO-E domain / bidding-zone identifier
- timezone
- enabled datasets

The ingestion implementation must iterate over configured countries and datasets rather than contain duplicated country-specific functions.

## Time Handling

ENTSO-E source timestamps must be retained accurately.

Daily downstream calculations must ultimately map source observations to the correct local calendar date.

The implementation must account for:

- UTC timestamps
- country timezone
- daylight-saving-time changes

Do not assume every calendar day contains exactly 24 source intervals.

## Idempotency

The ingestion design must support safe reprocessing.

Deterministic identifiers or equivalent business keys must be available so repeated runs do not create duplicate logical observations.

A rerun may update previously ingested data if ENTSO-E has published revised values.

## Observability

Each execution should expose:

- execution start time
- execution end time
- requested date range
- countries attempted
- datasets attempted
- successful requests
- failed requests
- record counts written per dataset
- retries performed

Failures must be clearly visible in Databricks job execution.

## Acceptance Criteria

This specification is complete when:

1. ENTSO-E API authentication works securely from Databricks.
2. All five configured countries can be requested.
3. Actual electricity load is ingested.
4. Actual generation by production type is ingested.
5. Day-ahead prices are ingested.
6. A configurable 24-month historical backfill can be executed.
7. Individual recent dates can be reprocessed.
8. Source data is stored as Bronze Delta data.
9. Source timestamps and source resolution are preserved.
10. Production types are retained.
11. API and XML errors are surfaced clearly.
12. Reruns do not create duplicate logical observations.
13. No runtime dependency on the local development machine exists.

## Out of Scope

This specification does not include:

- daily aggregation
- renewable-percentage calculation
- weather joins
- Silver transformations
- dbt models
- PostgreSQL publishing
- Power BI
- forecasting
- streaming ingestion
- cross-border electricity-flow analytics