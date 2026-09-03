# 002 — ENTSO-E Energy Ingestion

## Goal

Ingest European electricity-market data from the ENTSO-E Transparency Platform into Databricks for the five MVP countries.

The pipeline must support:

- 24-month historical backfill
- daily incremental ingestion
- recent-date reprocessing
- safe reruns
- partial-data detection
- Bronze Delta persistence

## Countries

The MVP covers:

- Ireland
- Germany
- France
- Spain
- Netherlands

ENTSO-E bidding-zone/domain identifiers must be maintained centrally in configuration.

Do not scatter EIC/domain codes throughout ingestion logic.

## Source

Use the ENTSO-E Transparency Platform Web API.

Production endpoint:

`https://web-api.tp.entsoe.eu/api`

Authentication uses the ENTSO-E Web API security token.

The token must be retrieved securely at runtime.

Never hard-code or commit the token.

## Required Datasets

### Actual Electricity Load

Retrieve actual total load using:

- `documentType=A65`
- `processType=A16`

ENTSO-E documents this as realised total load.

The source may return:

- PT60M
- PT30M
- PT15M

or another supported source resolution.

The source resolution must be preserved.

### Actual Generation by Production Type

Retrieve aggregated actual generation by production type using:

- `documentType=A75`
- `processType=A16`

ENTSO-E identifies this as realised aggregated generation per type.

Production type must be preserved exactly as provided by the source.

Do not normalize or collapse production types in Bronze.

### Day-Ahead Electricity Prices

Retrieve day-ahead prices using:

- `documentType=A44`

Use the required bidding-zone/domain parameters according to the ENTSO-E API contract.

Source timestamps, price values, currency, and units must be preserved.

## Historical Backfill

Support a configurable historical backfill covering the previous 24 months.

The implementation must accept:

- `start_date`
- `end_date`
- country / bidding zone
- dataset type

The 24-month range must not be permanently hard-coded.

Historical requests should be divided into bounded date windows.

The chunk size must be configurable.

Do not assume every dataset supports arbitrarily large query windows.

## Daily Incremental Ingestion

After historical backfill, the production workflow runs daily.

Normal incremental execution should process recent completed dates rather than permanently assuming only one date can ever be re-read.

The pipeline must support reprocessing previous dates because ENTSO-E values may be revised or previously missing intervals may later become available.

Repeated ingestion must not create duplicate logical records.

## XML Response Structure

ENTSO-E responses are XML-based.

The parser must handle the actual hierarchical structure:

`MarketDocument`
→ `TimeSeries`
→ one or more `Period`
→ `Point`

Do not assume:

- one response contains one TimeSeries
- one TimeSeries contains one Period
- one Period covers the complete requested day
- point positions are globally unique within a TimeSeries

Each Period independently defines:

- start timestamp
- end timestamp
- resolution
- Point positions

Point positions restart relative to their containing Period.

## Timestamp Derivation

A Point timestamp must be derived from:

- the containing Period start timestamp
- the Period resolution
- the Point position

Conceptually:

`point_timestamp = period_start + (position - 1) × resolution`

Do not derive timestamps from the overall request period.

Do not assume Point position `1` represents midnight.

## Multiple Periods

A single TimeSeries may contain multiple Period elements.

This can occur when the source dataset contains separated ranges of observations.

Each Period must be parsed independently and combined into the canonical Bronze representation.

A gap between Periods must remain identifiable.

Do not manufacture observations for missing intervals.

## Partial-Day Detection

A successful HTTP/XML response does not guarantee a complete day of data.

The ingestion layer must preserve enough information for completeness validation.

For every requested country/date/dataset, determine whether timeline coverage is:

- complete
- partial
- unavailable
- failed

Completeness must be based primarily on expected timeline coverage and source resolution.

Do not determine completeness solely from:

- HTTP status
- presence of any records
- a hard-coded expected number of Points

For example, PT15M commonly produces 96 intervals in a normal 24-hour UTC period, but this must not become a universal hard-coded rule.

## Resolution

Preserve the source resolution exactly.

Examples:

- `PT15M`
- `PT30M`
- `PT60M`

Downstream processing will convert the resolution into interval duration.

Do not normalize all observations to hourly resolution in Bronze.

## Units

Preserve the unit exactly as supplied by ENTSO-E.

For load/generation, ENTSO-E commonly supplies:

`MAW`

ENTSO-E defines this code as megawatts.

Bronze should retain the raw unit code.

Unit normalization into canonical analytical units belongs downstream.

## Bronze Storage

Each ENTSO-E dataset must be persisted in Databricks using Delta.

Bronze should preserve source information with minimal business transformation.

At minimum retain sufficient information to identify:

- country
- bidding zone / domain
- dataset type
- TimeSeries identifier
- Period start
- Period end
- Point position
- derived source timestamp
- source resolution
- value
- raw unit
- production type where applicable
- currency where applicable
- document identifier
- document revision number where available
- source-created timestamp where available
- ingestion timestamp
- requested start date
- requested end date
- source system

The source system must identify ENTSO-E.

## Raw Response Handling

Unexpected response structures must fail visibly.

Do not silently interpret:

- ENTSO-E error XML
- authorization errors
- malformed XML
- schema changes

as an empty successful dataset.

Where useful, retain document-level metadata for debugging and lineage.

## Execution Environment

This pipeline MUST execute on Databricks.

Local execution is not the supported production runtime.

Python may be used for:

- HTTP requests
- XML parsing
- configuration
- control flow

PySpark must be used for DataFrame-based pipeline processing and persisted Delta writes.

Do not use pandas as the pipeline-processing implementation.

## Authentication

Store the ENTSO-E security token using Databricks-supported secret management.

Retrieve it at runtime.

The token must never appear in:

- source code
- Git history
- committed notebooks
- logs
- exception messages

## API Behaviour

The implementation must:

- use HTTPS
- use explicit request timeouts
- validate HTTP status
- validate ENTSO-E response content
- use bounded retries for transient failures
- surface access/rate-limit errors
- log country, dataset, and requested date range

A failed request must not be silently ignored.

## Configuration

Configuration must contain at minimum:

- country code
- country name
- ENTSO-E bidding-zone/domain identifier
- timezone
- enabled datasets

The implementation must iterate over configuration.

Do not implement separate duplicated ingestion logic per country.

## Time Handling

ENTSO-E API periods and source timestamps must be handled as UTC unless the source semantics explicitly indicate otherwise.

The API's time granularity is the same as the data published on the Transparency Platform.

Bronze must preserve source timestamps accurately.

Conversion to country-local calendar dates belongs downstream.

The implementation must retain enough information to correctly handle:

- UTC
- local timezone conversion
- daylight-saving transitions
- varying interval resolutions

Do not assume every local calendar day has exactly 24 hours.

## Idempotency

The ingestion design must support safe reprocessing.

Construct deterministic logical identifiers using fields appropriate to the dataset, such as:

- country
- dataset type
- source timestamp
- production type where applicable

When revised versions of an observation exist, the newest valid source version should be capable of replacing the previous state downstream.

Repeated execution must not create duplicate logical observations.

## Observability

Each execution should expose:

- execution start time
- execution end time
- requested date range
- countries attempted
- datasets attempted
- successful requests
- failed requests
- complete periods
- partial periods
- unavailable periods
- records written
- retries performed

Failures and partial data must remain visible in Databricks job execution.

## Acceptance Criteria

This specification is complete when:

1. ENTSO-E authentication works securely from Databricks.
2. All five configured countries can be requested.
3. Actual total load can be ingested.
4. Actual generation by production type can be ingested.
5. Day-ahead prices can be ingested.
6. A configurable 24-month historical backfill can be executed.
7. Recent dates can be reprocessed safely.
8. Source data is stored as Bronze Delta data.
9. Source timestamps and resolutions are preserved.
10. Multiple TimeSeries and Period elements are handled correctly.
11. Point timestamps are derived relative to their containing Period.
12. Point-position resets across Periods are handled correctly.
13. Gaps between Periods remain identifiable.
14. Complete and partial source periods can be distinguished.
15. Raw units such as `MAW` are retained.
16. Production types are retained.
17. XML/API errors are surfaced clearly.
18. Reruns do not create duplicate logical observations.
19. No runtime dependency on the local development machine exists.

## Out of Scope

This specification does not include:

- daily aggregation
- MW-to-MWh calculation
- renewable-percentage calculation
- weather joins
- Silver transformations
- dbt models
- PostgreSQL publishing
- Power BI
- forecasting
- streaming
- cross-border electricity-flow analytics