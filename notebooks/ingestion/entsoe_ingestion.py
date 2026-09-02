# Databricks notebook source
# MAGIC %md
# MAGIC # ENTSO-E Bronze Ingestion (Spec 002)
# MAGIC
# MAGIC Workflow entry point for the `ingest_entsoe` task. All business logic
# MAGIC lives in `src/ingestion/entsoe_pipeline.py`; this notebook only reads
# MAGIC job parameters, retrieves the ENTSO-E security token from Databricks
# MAGIC secrets, and wires up the active `spark` session.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `dbutils`
# MAGIC and `spark`, which only exist in a Databricks notebook runtime.
# MAGIC
# MAGIC **Known blocker:** the ENTSO-E domain codes in
# MAGIC `src/config/entsoe_domains.yaml` are all marked `validated: false` —
# MAGIC they were sourced from public documentation, not confirmed against a
# MAGIC live ENTSO-E account. Confirm them (and obtain a security token) before
# MAGIC relying on this notebook's output.

# COMMAND ----------

from datetime import date

from src.ingestion.entsoe_pipeline import run_ingestion
from src.ingestion.open_meteo_pipeline import backfill_date_range, daily_processing_date

# COMMAND ----------

dbutils.widgets.dropdown("mode", "daily", ["daily", "backfill"], "Execution mode")
dbutils.widgets.text("start_date", "", "Start date (backfill, YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End date (backfill, YYYY-MM-DD)")
dbutils.widgets.text("secret_scope", "entsoe", "Databricks secret scope")
dbutils.widgets.text("secret_key", "api-token", "Databricks secret key")

mode = dbutils.widgets.get("mode")
start_date_param = dbutils.widgets.get("start_date")
end_date_param = dbutils.widgets.get("end_date")
secret_scope = dbutils.widgets.get("secret_scope")
secret_key = dbutils.widgets.get("secret_key")

# COMMAND ----------

if mode == "backfill":
    explicit_start = date.fromisoformat(start_date_param) if start_date_param else None
    explicit_end = date.fromisoformat(end_date_param) if end_date_param else None
    default_start, default_end = backfill_date_range(end_date=explicit_end)
    start_date = explicit_start or default_start
    end_date = explicit_end or default_end
else:
    processing_date = daily_processing_date()
    start_date, end_date = processing_date, processing_date

# COMMAND ----------

# Token retrieved via Databricks-managed secrets only — never hard-coded,
# never logged, never committed.
security_token = dbutils.secrets.get(scope=secret_scope, key=secret_key)

result = run_ingestion(start_date, end_date, token=security_token, spark=spark)

print(f"Requested range:      {result.start_date} to {result.end_date}")
print(f"Countries attempted:  {result.countries_attempted}")
print(f"Datasets attempted:   {result.datasets_attempted}")
print(f"Succeeded (country:dataset): {result.succeeded}")
print(f"Failed (country:dataset):    {result.failed}")
print(f"Records written:      {result.records_written}")

if not result.all_succeeded:
    raise RuntimeError(f"ENTSO-E ingestion failed for: {result.failed} ({result.errors})")
