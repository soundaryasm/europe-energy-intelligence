# European Energy Intelligence

Daily analytics on European electricity demand, prices, generation mix and
weather, for 15 countries, from January 2015 onward.

## Pipeline

```
ENTSO-E API ─┐
Open-Meteo  ─┼─→ Bronze ─→ Silver ─→ Gold (dbt) ─→ PostgreSQL ─→ Power BI
World Bank  ─┘   (Delta)   (Delta)    (Delta)
```

Databricks is the system of record and runs every transformation. PostgreSQL
is a serving layer holding curated Gold tables only.

## Data

| Layer | Grain | Rows |
|---|---|---|
| Bronze | source interval (PT15M/PT30M/PT60M) | ~36.5M |
| Silver | country + day | ~695K |
| Gold | country + day (+ production type) | ~453K |

Countries: IE, DE, FR, ES, NL, BE, PT, PL, CZ, FI, HU, RO, SK, SI, EE

## Serving layer

PostgreSQL holds the curated Gold tables and nothing else:

`dim_country`, `dim_date`, `fact_energy_daily`, `fact_weather_daily`,
`fact_generation_mix_daily`

It is a plain star schema in standard SQL — no vendor extensions, no stored
procedures, no API in front of it. Any client that speaks PostgreSQL can read
it directly: Power BI, Tableau, Metabase, Grafana, a web app, a notebook,
`psql`. All aggregation has already happened upstream, so a dashboard query is
a fact table joined to two dimensions.

Power BI is the reference consumer. It is not a dependency.

Full history is published, not a recent slice: around 453K rows and under
100 MB, growing roughly 12 MB a year.

## Historical backfill

Coverage starts at **January 2015**, when the ENTSO-E Transparency Platform
went live (5 January 2015, under EU Regulation 543/2013). No earlier data
exists at the source. January 2015 is therefore partial by nature — the
platform began publishing mid-month.

Backfill runs on GitHub Actions, one year per invocation, and writes one
Parquet file per source-month to S3-compatible blob storage. Databricks then
stages those files to a Unity Catalog volume and merges them into Bronze.

Blob storage sits in the middle for three reasons:

- backfill is HTTP requests, Parquet serialisation and an object-store write —
  no Spark, so it does not need to consume the Databricks compute allowance
- a year can be re-fetched, inspected or repaired without touching Bronze
- the merge into Bronze becomes a single idempotent pass over local files
  rather than thousands of API calls

A per-month manifest records every source/country/dataset outcome, so a
failed combination is retried without re-fetching what already succeeded.

## Jobs

| Job | Schedule | Purpose |
|---|---|---|
| `..._daily` | 02:00 UTC | Ingest → Silver → Gold → PostgreSQL |
| `..._historical_bronze_load` | manual | Blob Parquet → Bronze |
| `historical-backfill` (Actions) | disabled | One year of history → blob storage |

The backfill completed on 2026-11-04 and both historical jobs are now
dormant: the Actions workflow is guarded off and the blob bucket it
staged from has been emptied. `archive/historical-backfill/` holds the
record. Any stage can be re-run from the daily job.

The window is two optional job parameters, `start_date` and `end_date`.
Both blank — what the schedule sends — means the last 3 completed days.
A blank `end_date` is the latest completed UTC date; a blank `start_date`
is the lookback behind that end.

```bash
# any single stage, plus everything downstream of it
databricks bundle run <job> -t prod --only transform_silver+

# a one-day repair, or the full history
--params start_date=2026-11-01
--params start_date=2015-01-01
```

## Layout

```
src/            ingestion, transformations, serving, blob layer
notebooks/      Databricks task entry points
dbt/models/     Gold models
resources/      Databricks Asset Bundle job definitions
migrations/     one-time Bronze schema migrations
archive/        records of completed one-off work
specs/          numbered specifications
scripts/        operational scripts
tests/          pytest suite
```

## Running

```bash
pytest                                   # test suite
databricks bundle validate -t prod       # check job definitions
databricks bundle deploy  -t prod        # deploy jobs
databricks bundle run <job> -t prod      # run a job
```

Add `--only <task_key>` to run a single task.

## Configuration

Runtime secrets live in the Databricks secret scope
`europe-energy-intelligence`: `ENTSOE_API_TOKEN`, `UPSTASH_BLOB_TOKEN`,
`POSTGRES_URL`. Countries and ENTSO-E domain codes are in `src/config/`.

See `AGENTS.md` for architecture constraints and `specs/` for detail.
