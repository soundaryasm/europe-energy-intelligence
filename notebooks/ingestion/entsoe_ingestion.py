# Databricks notebook source
# MAGIC %md
# MAGIC # ENTSO-E Bronze Ingestion (Spec 002)
# MAGIC
# MAGIC Workflow entry point for the `ingest_entsoe` task. All business logic
# MAGIC lives in `src/ingestion/entsoe_pipeline.py`; this notebook only reads
# MAGIC job parameters, resolves the canonical processing window (Spec 006 —
# MAGIC `daily` mode reprocesses a rolling `lookback_days`-day window, not just
# MAGIC yesterday, because ENTSO-E values may be revised after first
# MAGIC publication), retrieves the ENTSO-E security token from Databricks
# MAGIC secrets, and wires up the active `spark` session.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `dbutils`
# MAGIC and `spark`, which only exist in a Databricks notebook runtime.
# MAGIC
# MAGIC **Known blocker:** most ENTSO-E domain codes in
# MAGIC `src/config/entsoe_domains.yaml` are still marked `validated: false` —
# MAGIC they were sourced from public documentation, not confirmed against a
# MAGIC live ENTSO-E account. NL is now `validated: true`, confirmed against a
# MAGIC real tested request/response — see `tmp/entsoe.md`. Confirm
# MAGIC the remaining domains (and obtain a security token) before relying on
# MAGIC this notebook's output for IE/DE/FR/ES.

# COMMAND ----------

from datetime import date

from src.ingestion.entsoe_pipeline import run_ingestion
from src.orchestration.processing_window import (
    DEFAULT_ENTSOE_LOOKBACK_DAYS,
    VALID_EXECUTION_MODES,
    resolve_entsoe_window,
)

# COMMAND ----------

dbutils.widgets.dropdown("execution_mode", "daily", list(VALID_EXECUTION_MODES), "Execution mode")
dbutils.widgets.text("start_date", "", "Start date (backfill/reprocess, YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End date (backfill/reprocess, YYYY-MM-DD)")
dbutils.widgets.text(
    "lookback_days", str(DEFAULT_ENTSOE_LOOKBACK_DAYS), "Daily-mode recent-date lookback (days)"
)
dbutils.widgets.text("secret_scope", "entsoe", "Databricks secret scope")
dbutils.widgets.text("secret_key", "api-token", "Databricks secret key")

execution_mode = dbutils.widgets.get("execution_mode")
start_date_param = dbutils.widgets.get("start_date")
end_date_param = dbutils.widgets.get("end_date")
lookback_days = int(dbutils.widgets.get("lookback_days"))
secret_scope = dbutils.widgets.get("secret_scope")
secret_key = dbutils.widgets.get("secret_key")

# COMMAND ----------

explicit_start = date.fromisoformat(start_date_param) if start_date_param else None
explicit_end = date.fromisoformat(end_date_param) if end_date_param else None

start_date, end_date = resolve_entsoe_window(
    execution_mode,
    start_date=explicit_start,
    end_date=explicit_end,
    lookback_days=lookback_days,
)

# COMMAND ----------

# Token retrieved via Databricks-managed secrets only — never hard-coded,
# never logged, never committed.
security_token = dbutils.secrets.get(scope=secret_scope, key=secret_key)

result = run_ingestion(start_date, end_date, token=security_token, spark=spark)

print(f"Execution mode:        {execution_mode}")
print(f"Requested range:       {result.start_date} to {result.end_date}")
print(f"Countries attempted:   {result.countries_attempted}")
print(f"Datasets attempted:    {result.datasets_attempted}")
print(f"Succeeded (country:dataset): {result.succeeded}")
print(f"Failed (country:dataset):    {result.failed}")
print(f"Records written:       {result.records_written}")

if not result.all_succeeded:
    raise RuntimeError(f"ENTSO-E ingestion failed for: {result.failed} ({result.errors})")
