# Databricks notebook source
# MAGIC %md
# MAGIC # Open-Meteo Bronze Ingestion (Spec 001)
# MAGIC
# MAGIC Workflow entry point for the `ingest_open_meteo` task. All business logic
# MAGIC lives in `src/ingestion/open_meteo_pipeline.py`; this notebook only reads
# MAGIC job parameters and wires up the active `spark` session.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `dbutils`
# MAGIC and `spark`, which only exist in a Databricks notebook runtime.

# COMMAND ----------

from datetime import date

from src.ingestion.open_meteo_pipeline import (
    backfill_date_range,
    daily_processing_date,
    run_ingestion,
)

# COMMAND ----------

dbutils.widgets.dropdown("mode", "daily", ["daily", "backfill"], "Execution mode")
dbutils.widgets.text("start_date", "", "Start date (backfill, YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End date (backfill, YYYY-MM-DD)")

mode = dbutils.widgets.get("mode")
start_date_param = dbutils.widgets.get("start_date")
end_date_param = dbutils.widgets.get("end_date")

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

result = run_ingestion(start_date, end_date, spark=spark)

print(f"Requested range:     {result.start_date} to {result.end_date}")
print(f"Countries attempted: {result.countries_attempted}")
print(f"Countries succeeded: {result.countries_succeeded}")
print(f"Countries failed:    {result.countries_failed}")
print(f"Records written:     {result.records_written}")

if not result.succeeded:
    raise RuntimeError(
        f"Open-Meteo ingestion failed for countries: {result.countries_failed} "
        f"({result.errors})"
    )
