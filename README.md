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

Historical ingestion runs on GitHub Actions and writes one Parquet file per
source-month to S3-compatible blob storage; Databricks stages those files to a
Unity Catalog volume and merges them into Bronze.

## Data

| Layer | Grain | Rows |
|---|---|---|
| Bronze | source interval (PT15M/PT30M/PT60M) | ~36.5M |
| Silver | country + day | ~695K |
| Gold | country + day (+ production type) | ~453K |

Countries: IE, DE, FR, ES, NL, BE, PT, PL, CZ, FI, HU, RO, SK, SI, EE

Serving tables: `dim_country`, `dim_date`, `fact_energy_daily`,
`fact_weather_daily`, `fact_generation_mix_daily`

## Jobs

| Job | Schedule | Purpose |
|---|---|---|
| `..._daily` | 02:00 UTC | Ingest → Silver → Gold → PostgreSQL |
| `..._historical_bronze_load` | manual | Blob Parquet → Bronze |
| `..._historical_rebuild` | manual | Silver → Gold → PostgreSQL over full history |
| `historical-backfill` (Actions) | dispatch | One year of history → blob storage |

Silver and the PostgreSQL publish process a rolling window
(`lookback_days`, default 3). A `reprocess` run with explicit
`start_date`/`end_date` covers any wider range.

## Layout

```
src/            ingestion, transformations, serving, blob layer
notebooks/      Databricks task entry points
dbt/models/     Gold models
resources/      Databricks Asset Bundle job definitions
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
