# 009 — Project Structure and Delivery

## Goal

Define the repository structure, development workflow, testing expectations, source-control practices, and portfolio delivery standards for the European Energy Intelligence project.

The repository should be easy to:

- understand
- navigate
- review
- test
- execute from Databricks
- present publicly on GitHub

## Repository Structure

Use the following high-level structure:

```text
europe-energy-intelligence/
├── AGENTS.md
├── README.md
├── requirements.txt
├── .gitignore
│
├── specs/
│   ├── 001-open-meteo-ingestion.md
│   ├── 002-entsoe-ingestion.md
│   ├── 003-silver-transformations.md
│   ├── 004-dbt-gold-models.md
│   ├── 005-postgres-serving.md
│   ├── 006-daily-orchestration.md
│   ├── 007-data-quality.md
│   ├── 008-powerbi-model.md
│   └── 009-project-structure-and-delivery.md
│
├── src/
│   ├── ingestion/
│   ├── transformations/
│   ├── orchestration/
│   ├── serving/
│   ├── config/
│   └── quality/
│
├── notebooks/
│   ├── ingestion/
│   ├── transformations/
│   ├── serving/
│   └── exploration/
│
├── dbt/
│   ├── models/
│   ├── tests/
│   ├── macros/
│   └── dbt_project.yml
│
└── tests/
    ├── fixtures/
    │   ├── entsoe/
    │   └── open_meteo/
    └── test_*.py
```

The exact internal layout may evolve, but responsibilities must remain clearly separated.

## Source Code vs Notebooks

Reusable production logic belongs under:

`src/`

Databricks notebooks should remain thin entry points.

Use notebooks primarily for:

- task entry points
- parameter handling
- exploration
- demonstrations
- invoking reusable project modules

Avoid placing large amounts of reusable business logic directly inside notebooks.

A notebook should normally delegate work to code under `src/`.

## Runtime Rule

All production pipeline execution MUST occur on Databricks.

The local machine may be used for:

- editing
- Codex/AI-assisted development
- Git operations
- documentation
- isolated non-Spark unit tests

The local machine must not become a hidden production runtime dependency.

Do not install or configure a local Spark runtime merely to imitate Databricks.

## PySpark Runtime

PySpark processing belongs on Databricks.

Do not add `pyspark` to the shared project `requirements.txt` merely to support local execution.

Spark/PySpark-dependent tests should be written so they can execute against the Databricks runtime.

Do not replace PySpark production transformations with pandas for local convenience.

## Python Dependencies

The shared `requirements.txt` should contain only dependencies required by the Python pipeline and its lightweight tests.

The initial approved dependencies are:

```text
requests
PyYAML
psycopg[binary]
pytest
```

Do not add infrastructure libraries unnecessarily.

`dbt-databricks` is managed separately by the Databricks dbt task as defined in Spec 004.

## Testing Strategy

Use both:

- Python/PySpark unit tests
- dbt tests

They serve different purposes.

### Python Tests

Root-level:

`tests/`

Use pytest.

Use `unittest.mock` where external systems need isolation.

Appropriate unit-test targets include:

- API response parsing
- ENTSO-E XML parsing
- Period/Point timestamp derivation
- interval-duration parsing
- MW-to-MWh calculations
- production-type mappings
- renewable classification
- configuration validation
- API error handling
- deterministic key generation
- Open-Meteo payload parsing

### Spark Tests

Spark-dependent transformation tests should:

- use small deterministic Spark DataFrames
- test real transformation logic
- execute on Databricks
- avoid mocking Spark transformations merely to make tests easy to run locally

Do not configure a local JVM/Spark environment as a project requirement.

### dbt Tests

dbt-specific tests belong inside the dbt project.

Use:

- generic schema tests
- relationship tests
- uniqueness tests
- not-null tests
- custom singular SQL tests where useful

The root `tests/` directory is not reserved for dbt tests.

## Fixtures

Realistic sanitized API payloads should be stored under:

`tests/fixtures/`

Examples:

```text
tests/fixtures/entsoe/
├── actual_load_complete_day.xml
├── actual_load_partial_day.xml
├── generation_sample.xml
└── price_sample.xml

tests/fixtures/open_meteo/
└── historical_daily_sample.json
```

Fixtures must never contain:

- API tokens
- credentials
- private URLs
- secret-bearing request parameters

Fixtures should represent realistic external API behaviour.

## Agent Usage

Coding agents may implement approved specifications.

Before modifying project code, an agent must read:

1. `AGENTS.md`
2. the relevant numbered specification
3. applicable test fixtures

Agents must not:

- silently alter architecture
- introduce new production data sources
- change runtime assumptions
- introduce local Spark as a requirement
- fabricate successful external integrations
- commit credentials
- broaden project scope without approval

## Specification Discipline

Specifications are authoritative for required behaviour.

If implementation reveals that a specification is technically wrong or impractical:

1. stop relying on the incorrect assumption
2. document the issue
3. update the specification deliberately
4. then update implementation

Do not silently work around incorrect specifications.

## External Integration Boundaries

Some implementation steps require real external infrastructure.

Examples:

- ENTSO-E API access
- Databricks secrets
- Databricks SQL Warehouse
- Databricks Lakeflow Jobs
- Aiven PostgreSQL
- Power BI

Agents may implement code and configuration scaffolding, but must not claim an integration is validated until it has actually been executed against the intended environment.

## Git Workflow

GitHub is the source of truth.

Typical workflow:

```text
local editor / coding agent
        ↓
      Git commit
        ↓
       GitHub
        ↓
Databricks Git folder pull
        ↓
   Databricks execution
```

Do not manually copy production code between local files and Databricks when Git synchronization can be used.

## Branching

For the MVP, keep branching simple.

Use:

- `main` for stable project state
- short-lived feature branches where useful

Examples:

- `feature/open-meteo-ingestion`
- `feature/entsoe-ingestion`
- `feature/silver-transformations`

Avoid introducing a complex GitFlow process.

## Commit Quality

Commits should represent understandable project changes.

Prefer:

- `Add Open-Meteo Bronze ingestion`
- `Handle ENTSO-E multi-period responses`
- `Add daily demand completeness validation`
- `Implement dbt Gold energy model`
- `Publish Gold tables to PostgreSQL`

Avoid:

- `update`
- `changes`
- `fix`
- `stuff`

## Secrets

Secrets must never be committed.

Protect:

- ENTSO-E API token
- Aiven PostgreSQL credentials
- Databricks credentials
- `.env`
- secret-bearing configuration
- private certificates

`.env` must remain gitignored.

Production secrets must use Databricks-supported secret management.

## Configuration

Non-secret configuration should be version controlled.

Examples:

- supported countries
- country codes
- capital/reference locations
- coordinates
- timezones
- ENTSO-E domain identifiers
- production-type mappings
- renewable classifications
- default lookback length

Configuration should be centralized.

Avoid duplicating the same values across multiple modules.

## Data Source Scope

Approved MVP sources are:

- ENTSO-E Transparency Platform
- Open-Meteo

Do not add additional production sources without updating the approved specifications.

## Documentation

The final README should explain:

- business problem
- architecture
- countries covered
- data sources
- technology choices
- Bronze/Silver/Gold flow
- daily orchestration
- historical backfill
- recent-date reprocessing
- dbt modelling
- data quality
- PostgreSQL serving
- Power BI output
- repository structure
- limitations
- future enhancements

The README should serve both:

- technical interviewers
- recruiters scanning the repository quickly

## Architecture Diagram

Final documentation should include a clear architecture diagram similar to:

```text
ENTSO-E              Open-Meteo
    \                    /
     \                  /
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

Also indicate:

- Databricks Lakeflow Jobs orchestration
- GitHub source control
- Databricks as the production runtime

## Portfolio Presentation

A reviewer should quickly be able to identify:

- real public APIs
- 24-month historical backfill
- daily incremental processing
- source revisions/lookback handling
- medallion architecture
- PySpark
- dbt
- data-quality gates
- Databricks orchestration
- PostgreSQL serving
- Power BI consumption

The project must not rely solely on the Power BI dashboard to demonstrate engineering depth.

## Evidence

Useful final portfolio evidence may include:

- architecture diagram
- Databricks Job DAG screenshot
- successful workflow execution
- Bronze/Silver/Gold examples
- dbt lineage screenshot
- dbt test output
- PostgreSQL serving schema
- Power BI dashboard screenshots

Do not expose:

- tokens
- credentials
- secret scopes
- sensitive workspace details

## Cost Constraint

The architecture must remain compatible with the project's free-tier objective.

Do not introduce paid services merely for convenience.

The intended storage separation remains:

Databricks
→ Bronze, Silver, Gold, historical analytical data

Aiven PostgreSQL
→ compact serving datasets only

## Scope Control

The MVP ends with implementation of the approved specifications.

Do not delay completion for optional features such as:

- additional countries
- additional sources
- hourly analytics
- streaming
- Kafka
- Airflow
- machine learning
- forecasting
- Terraform
- Kubernetes
- enterprise CI/CD
- advanced observability tooling

These may be considered later.

## Definition of Done

The MVP is complete when:

1. Open-Meteo ingestion works on Databricks.
2. ENTSO-E ingestion works on Databricks.
3. 24 months of historical data can be backfilled.
4. Daily incremental execution works.
5. Recent ENTSO-E dates are reprocessed through the configured lookback.
6. Bronze Delta datasets exist.
7. Trusted Silver daily datasets exist.
8. Partial ENTSO-E data does not become misleading daily metrics.
9. PySpark performs approved transformations.
10. dbt builds and tests Gold models.
11. Gold models are persisted in Databricks.
12. Gold serving data reaches Aiven PostgreSQL.
13. PostgreSQL remains compact and serving-only.
14. Databricks Lakeflow Jobs orchestrates the workflow.
15. Critical quality failures block PostgreSQL publication.
16. Python/PySpark tests cover high-value processing behaviour.
17. dbt tests validate the analytical models.
18. Power BI consumes PostgreSQL.
19. All five countries are represented.
20. Documentation is clear and portfolio-ready.
21. The architecture can be explained confidently in an interview.
22. No production pipeline depends on local execution.
23. The project remains within the approved free-tier design.

## Out of Scope

This specification does not require:

- local Spark
- enterprise CI/CD
- multiple deployment environments
- Terraform
- Kubernetes
- Airflow
- Kafka
- streaming
- machine learning
- paid observability tooling
- production-grade enterprise security architecture