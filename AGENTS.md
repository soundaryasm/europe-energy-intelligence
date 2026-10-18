# AGENTS.md

## Project

European Energy Intelligence is a portfolio-focused analytics engineering project built around European electricity and weather data.

The platform ingests, processes, models, serves, and visualises daily energy data for:

- Ireland
- Germany
- France
- Spain
- Netherlands

## Core Architecture

Daily:

External APIs
→ Databricks Bronze
→ Databricks Silver
→ dbt Gold models
→ PostgreSQL
→ Power BI

Historical:

External APIs
→ GitHub Actions
→ Parquet in S3-compatible blob storage
→ Databricks Bronze
→ (the same Silver / Gold / PostgreSQL path as above)

## Runtime Constraints

All transformation and data-processing workloads MUST run on Databricks. Databricks remains the system of record and the only place Bronze, Silver and Gold are built.

Daily ingestion also runs on Databricks.

Historical ingestion runs on GitHub Actions and writes Parquet to blob storage, from where Databricks loads it into Bronze. It requires no Spark — it is HTTP requests, Parquet serialisation and an object-store write — and running it off-platform keeps a bounded Databricks compute allowance available for the daily pipeline. It reuses the same ingestion modules as the daily path rather than duplicating them. See `specs/010-historical-blob-backfill.md`.

The local machine may only be used for:

- editing code
- AI-assisted code generation
- Git operations
- documentation

The local machine is NOT a supported runtime environment.

Do not design pipeline components that depend on local execution.

## Processing

Use PySpark for data processing where applicable.

Do not replace PySpark transformations with pandas purely for convenience.

Python may be used for:

- API requests
- configuration
- orchestration glue
- utilities
- PostgreSQL publishing

## Storage Responsibilities

### Databricks

Databricks is the system of record.

It stores:

- Bronze raw data
- Silver cleaned/transformed data
- Gold analytical models
- historical data

Use Delta tables for persisted Databricks datasets.

### PostgreSQL

PostgreSQL is a serving layer only. The hosting provider is not
prescribed — any standard PostgreSQL instance (managed or self-hosted)
is acceptable.

Do not store raw or Silver datasets in PostgreSQL.

Only curated Gold datasets required by downstream consumers should be published.

Initial serving tables:

- dim_country
- dim_date
- fact_energy_daily
- fact_weather_daily
- fact_generation_mix_daily

### Power BI

Power BI must consume data from PostgreSQL.

Power BI should not directly query Bronze or Silver Databricks datasets.

## Data Grain

The MVP canonical analytical grain is daily by country.

Do not introduce hourly processing into the MVP unless a specification explicitly requires it.

## Historical Data

Historical coverage starts at January 2015 and runs to the last complete calendar month.

It is ingested by the layer in `specs/010-historical-blob-backfill.md`, one year per invocation, landing one Parquet file per source-month in blob storage. Databricks then loads those files into Bronze in a single pass.

Alongside that, pipelines perform daily incremental synchronisation.

Daily loads must be idempotent.

Reprocessing the same date must not create duplicate records.

Loading the same historical month twice must not create duplicate records.

## Data Sources

Primary energy source:

- ENTSO-E Transparency Platform API

Weather source:

- Open-Meteo API

Do not introduce additional production data sources without updating the specifications.

## dbt

dbt is responsible for analytical modelling in the Gold layer.

Use dbt for:

- fact models
- dimension models
- business transformations
- model-level tests
- analytical relationships

Do not use dbt for API ingestion.

## Orchestration

The production pipeline is intended to run daily at approximately 02:00 using Databricks-native orchestration.

Individual tasks should have explicit dependencies and failure behaviour.

## Engineering Principles

Prefer:

- simple implementations
- explicit schemas
- idempotent pipelines
- incremental processing
- reproducible transformations
- clear separation of Bronze, Silver, Gold, and serving layers
- testable components
- observable pipeline failures

Avoid:

- unnecessary infrastructure
- premature optimisation
- duplicated business logic
- hidden local dependencies
- hard-coded secrets
- hard-coded environment-specific credentials

## Secrets

Never commit:

- API tokens
- database passwords
- access tokens
- connection strings containing credentials

Use Databricks-supported secret management for runtime secrets.

## Source Control

GitHub is the source of truth for code and documentation.

Typical workflow:

local editor / coding agent
→ Git commit
→ GitHub
→ Databricks Git folder pull
→ execute on Databricks

## Scope Discipline

This is an MVP-first portfolio project.

Do not expand scope unless required by an approved specification.

Features such as additional countries, hourly analytics, additional APIs, streaming, machine learning, or advanced infrastructure belong to later iterations unless explicitly specified.