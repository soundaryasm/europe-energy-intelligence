# European Energy Intelligence

A portfolio analytics engineering project that ingests, processes,
models, serves, and visualises daily European electricity and weather
data for five countries: **Ireland, Germany, France, Spain, and the
Netherlands**.

## The business problem

Electricity demand, day-ahead prices, generation mix, and weather are
public but scattered across separate APIs in separate shapes. This
project turns them into one coherent, daily, country-level analytical
dataset — the kind of thing a market analyst or portfolio reviewer would
actually want to query — and demonstrates the full engineering path from
raw API to BI dashboard, not just a model or a chart.

## Architecture

```text
ENTSO-E          Open-Meteo
    \               /
     \             /
      -> Databricks Bronze
             |
        PySpark Silver
             |
          dbt Gold
             |
        PostgreSQL
             |
     (Power BI — separate)

  Orchestrated daily (02:00 Europe/Dublin) by Databricks Lakeflow Jobs.
  GitHub is the source of truth for all code; Databricks pulls from a
  Git-backed workspace folder.
```

The diagram shows the full end-to-end picture for context, but **this
repository's delivery boundary stops at the PostgreSQL serving layer**.
Power BI (or any other BI tool) is a downstream consumer of that
serving layer, built and maintained separately — see
`specs/008-powerbi-model.md`.

All ingestion, transformation, and orchestration executes on
**Databricks** — the local machine is only used for editing, AI-assisted
development, Git, and documentation (see `AGENTS.md`).

## Data sources

- **[ENTSO-E Transparency Platform](https://transparency.entsoe.eu/)** —
  actual electricity load (A65), actual generation by production type
  (A75), day-ahead prices (A44).
- **[Open-Meteo Historical Weather API](https://open-meteo.com/)** —
  hourly temperature/wind speed and daily solar radiation, for one
  reference location (capital city) per country.

## Technology choices

| Layer | Technology | Why |
|---|---|---|
| Ingestion | Python (`requests`) | API calls, XML/JSON parsing, retry logic |
| Bronze / Silver | PySpark on Databricks | Explicit spec requirement; scales past the MVP's small dataset |
| Storage (Bronze/Silver/Gold) | Delta Lake on Databricks | System of record, ACID MERGE for idempotent reprocessing |
| Gold modelling | dbt (`dbt-databricks`) | Dimensional modelling, `ref()`/`source()` lineage, native schema tests |
| Serving | PostgreSQL (any standard host, free-tier compatible) | Small, stable, BI-facing contract; not a data warehouse; provider is not prescribed |
| Publishing | Python (`psycopg`) | Batch upsert from Databricks into Postgres |
| Orchestration | Databricks Lakeflow Jobs (Asset Bundle) | Native scheduling, task DAG, retries, run history |

Everything above is chosen to remain within a **free-tier budget** (see
`specs/009-project-structure-and-delivery.md` — "Cost Constraint").

## Pipeline flow

1. **Ingest** (`src/ingestion/`, `notebooks/ingestion/`) — Open-Meteo and
   ENTSO-E run as independent Databricks tasks, writing raw, minimally
   transformed "tall" rows to Bronze Delta tables with deterministic
   business keys (safe to rerun).
2. **Transform** (`src/transformations/`,
   `notebooks/transformations/transform_silver.py`) — PySpark aggregates
   Bronze into four daily Silver datasets: demand (MWh, interval-duration
   corrected), day-ahead price (interval-weighted average, negative
   prices preserved), generation mix (normalized production type +
   renewable classification), and weather (daily temp/wind/solar).
3. **Model** (`dbt/`) — dbt builds the Gold star schema (`dim_country`,
   `dim_date`, `fact_energy_daily`, `fact_weather_daily`,
   `fact_generation_mix_daily`) from the Silver sources, with schema
   tests for uniqueness, referential integrity, and value ranges.
4. **Serve** (`src/serving/`, `notebooks/serving/publish_postgres.py`) —
   batch upsert of the five Gold tables into PostgreSQL. This is where
   this project's delivery ends.

Visualisation (Power BI or any other BI tool) is a separate, downstream
concern: it should query PostgreSQL only, never Databricks directly, but
building and maintaining that report is out of scope for this repository
— see `specs/008-powerbi-model.md`.

Orchestration (`databricks.yml`, `resources/daily_pipeline.yml`) wires
ingestion through serving into one Databricks Job with explicit task
dependencies: Silver waits on both ingestion tasks, dbt waits on Silver,
and Postgres publishing waits on dbt (including `dbt test` — a failed
Gold build never reaches the serving layer).

## Data model

Star schema, grain **country + date** (generation mix additionally by
**production type**):

- `dim_country` — the 5 MVP countries.
- `dim_date` — one row per calendar day, covering the historical
  backfill and future daily loads.
- `fact_energy_daily` — demand, day-ahead price stats, renewable
  generation %.
- `fact_weather_daily` — temperature, wind speed, solar radiation.
- `fact_generation_mix_daily` — generation by normalized production
  type, with a renewable flag and generation share %.

Full field-level detail: `specs/004-dbt-gold-models.md`.

## Repository layout

```text
europe-energy-intelligence/
├── AGENTS.md                 # runtime/architecture rules for humans and coding agents
├── specs/                    # numbered specifications (001-009), the source of truth for behaviour
├── src/
│   ├── config/                # country + ENTSO-E domain reference config (no duplication elsewhere)
│   ├── ingestion/              # Open-Meteo / ENTSO-E Bronze clients, parsers, pipelines
│   ├── transformations/        # PySpark Silver builders + shared calc/dedupe/production-type logic
│   ├── serving/                 # PostgreSQL schema, connection, upsert publisher
│   └── quality/                  # ingestion completeness classification
├── notebooks/                 # thin Databricks entry points (ingestion/transformations/serving)
├── dbt/                       # Gold dimensional models, sources, schema tests, singular reconciliation tests
├── resources/, databricks.yml # Databricks Asset Bundle job definition (Spec 006)
├── docs/                      # reference docs that aren't runnable code (e.g. Power BI model)
└── tests/                     # pytest: local unit tests + pyspark.importorskip-guarded Databricks tests
```

## Orchestration

Databricks Lakeflow Jobs, scheduled daily at **02:00 Europe/Dublin**
(explicit timezone, not the local machine's clock). Task DAG:

```text
ingest_open_meteo  ─┐
                     ├─> transform_silver -> build_dbt_gold -> publish_postgres
ingest_entsoe      ─┘
```

Bounded retries (max 2, 5-minute backoff) on network-facing tasks.
Manual execution supports `daily`, `backfill`, and `reprocess` modes
without duplicating pipeline code. See
`specs/006-daily-orchestration.md` and `resources/daily_pipeline.yml`.

## Testing

`pytest` (see `requirements.txt`). Local tests never run the production
pipeline — they exercise pure business logic (parsing, config,
calculations, upsert SQL construction) with `unittest.mock` isolating
external systems (HTTP APIs, the database connection). PySpark
transformation tests exist as real Spark-executing tests
(`tests/test_*_spark.py`, `@pytest.mark.databricks`) but are
automatically skipped wherever PySpark isn't installed — they are
written to run on Databricks (or any PySpark-enabled machine), not here.

```bash
pip install -r requirements.txt
pytest
```

## Limitations and known blockers

This is an active build, not a finished, deployed system. Honestly, as
of the current state:

- **ENTSO-E domain codes and XML schema are unverified against a live
  account** — implemented from public documentation only (no API token
  was available to this build). Must be validated on first real run.
- **No Databricks workspace, SQL Warehouse, or PostgreSQL instance has
  been used** — nothing in Bronze/Silver/Gold/serving has executed
  against real infrastructure. `dbt run`/`dbt test`, the Databricks Job,
  and PostgreSQL publishing are implemented and unit-tested (with mocks)
  but not integration-verified.
- **Power BI is out of scope for this repository** — see
  `specs/008-powerbi-model.md`; it describes the end-to-end picture for
  context, but this project's delivery ends at PostgreSQL serving.
- **`dbt/seeds/country_reference.csv` duplicates `src/config/*.yaml`** —
  no automated sync exists between the dbt seed and the Python ingestion
  config yet.

## Future work (explicitly out of MVP scope)

Additional countries, hourly analytics, streaming, ML forecasting,
extra data sources, and enterprise CI/CD — see each spec's "Out of
Scope" section and `specs/009-project-structure-and-delivery.md`
"Scope Control".
