# 009 — Project Structure and Delivery

## Goal

Define the repository structure, development workflow, source-control expectations, and portfolio delivery standards for the European Energy Intelligence project.

The repository should be easy to:

- understand
- navigate
- review
- run from Databricks
- present publicly on GitHub

## Repository Structure

Use the following high-level structure:

```text
europe-energy-intelligence/
├── AGENTS.md
├── README.md
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
│   ├── serving/
│   ├── config/
│   └── utils/
│
├── notebooks/
│   ├── ingestion/
│   ├── transformations/
│   ├── serving/
│   └── exploration/
│
├── dbt/
├── tests/
├── docs/
└── .gitignore
````

The exact internal layout may evolve, but responsibilities must remain clearly separated.

## Source Code vs Notebooks

Prefer reusable Python modules under:

`src/`

Use Databricks notebooks primarily for:

* workflow entry points
* orchestration-friendly execution
* exploration
* demonstrations

Avoid placing all business logic directly inside large notebooks.

Reusable logic should live in Python modules wherever practical.

## Runtime Rule

All production execution must occur on Databricks.

The local machine may be used for:

* editing
* Codex/AI-assisted development
* Git
* documentation

The local machine must not become a hidden runtime dependency.

## Git Workflow

GitHub is the source of truth.

Typical workflow:

1. Edit locally.
2. Review generated changes.
3. Commit locally.
4. Push to GitHub.
5. Pull changes into the Databricks Git folder.
6. Execute and validate on Databricks.

Do not manually copy code between local files and Databricks notebooks when Git synchronization can be used.

## Branching

For the MVP, simple branching is sufficient.

Use:

* `main` for stable project state
* short-lived feature branches where useful

Example:

`feature/open-meteo-ingestion`

Avoid introducing a complex GitFlow process.

## Commit Quality

Commits should represent understandable project changes.

Prefer messages such as:

* `Add Open-Meteo Bronze ingestion`
* `Implement daily weather aggregation`
* `Add dbt energy fact model`
* `Publish Gold tables to PostgreSQL`

Avoid meaningless commit messages such as:

* `update`
* `changes`
* `fix stuff`

## Agent Usage

Coding agents may implement approved specifications.

Agents must:

* read `AGENTS.md`
* read the relevant numbered specification
* follow existing architecture
* avoid introducing new infrastructure without approval
* avoid changing project scope silently

Agents should not make product or architecture decisions merely because an alternative implementation is easier.

## Specification Discipline

Specifications define intended behaviour.

If implementation reveals that a specification is technically incorrect or impractical:

1. do not silently work around it
2. document the issue
3. update the specification deliberately
4. then change implementation

The repository should reflect the final architectural decisions rather than preserve outdated assumptions.

## Secrets

Secrets must never be committed.

The repository `.gitignore` and development practices must protect:

* ENTSO-E tokens
* PostgreSQL credentials
* Databricks credentials
* local environment files
* temporary secret-bearing files

Use runtime secret management.

## Configuration

Non-secret configuration should be version controlled.

Examples:

* supported countries
* country codes
* reference locations
* coordinates
* timezones
* ENTSO-E domains
* production-type mappings
* renewable classifications

Avoid duplicating configuration across multiple modules.

## Testing

Tests should focus on high-value behaviour.

Examples:

* production-type mappings
* interval-duration calculations
* renewable-percentage calculations
* deterministic keys
* configuration validation
* parsing edge cases

Do not create large amounts of low-value testing solely to increase test count.

Tests that require the Databricks runtime must be designed accordingly.

Local tests must not imply that the production pipeline itself is intended to run locally.

## Documentation

The repository README should eventually explain:

* the business problem
* architecture
* data sources
* technology choices
* pipeline flow
* data model
* orchestration
* Power BI output
* how to navigate the repository
* limitations and future work

The README should be written for both:

* technical interviewers
* recruiters who may only scan the repository briefly

## Architecture Diagram

Include a clear architecture diagram in the final portfolio documentation.

It should communicate:

```text
ENTSO-E          Open-Meteo
    \               /
     \             /
      → Databricks Bronze
             ↓
        PySpark Silver
             ↓
          dbt Gold
             ↓
        PostgreSQL
             ↓
          Power BI
```

The diagram should also indicate:

* Databricks Jobs orchestration
* GitHub source control

## Portfolio Presentation

The final project should make the engineering story understandable quickly.

A reviewer should be able to identify:

* real external data sources
* historical backfill
* daily incremental processing
* medallion architecture
* PySpark transformations
* dbt modelling
* data-quality checks
* orchestration
* serving layer
* BI consumption

The project should not depend solely on the Power BI dashboard to demonstrate technical depth.

## Evidence

Where practical, final documentation may include:

* Databricks workflow screenshots
* dbt lineage/documentation screenshots
* Power BI dashboard screenshots
* example data model
* architecture diagram

Do not expose secrets, workspace tokens, or private connection details in screenshots.

## Cost Constraint

The project is intended to remain effectively free to operate.

Technology choices must remain compatible with the approved free-tier architecture unless explicitly reconsidered.

Do not introduce paid services merely for convenience.

## Scope Control

The MVP is complete when the approved specifications are implemented.

Do not delay completion for optional enhancements such as:

* additional countries
* hourly analytics
* streaming
* ML forecasting
* extra APIs
* complex infrastructure
* enterprise CI/CD

Those belong to later iterations.

## Definition of Done

The project MVP is complete when:

1. Both external sources ingest successfully.
2. 24 months of historical data can be processed.
3. Daily incremental execution works.
4. Bronze, Silver, and Gold datasets exist in Databricks.
5. PySpark performs approved transformations.
6. dbt builds and tests Gold models.
7. Gold serving data reaches PostgreSQL.
8. Databricks Jobs orchestrates the pipeline.
9. Critical quality failures block serving publication.
10. Power BI consumes PostgreSQL successfully.
11. All five countries are represented.
12. The repository contains clear documentation.
13. The architecture can be explained confidently in an interview.
14. The pipeline does not depend on local runtime execution.
15. The project remains within the approved free-tier design.

## Out of Scope

This specification does not require:

* enterprise CI/CD
* multiple deployment environments
* Terraform
* Kubernetes
* Airflow
* Kafka
* streaming
* machine learning
* paid observability tooling
* production-grade enterprise security architecture