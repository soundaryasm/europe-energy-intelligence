# Databricks notebook source
# MAGIC %md
# MAGIC # Open-Meteo Bronze Ingestion (Spec 001)
# MAGIC
# MAGIC Workflow entry point for the `ingest_open_meteo` task. All business logic
# MAGIC lives in `src/ingestion/open_meteo_pipeline.py`; this notebook only
# MAGIC reads job parameters, resolves the canonical processing window (Spec 006),
# MAGIC and wires up the active `spark` session.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `dbutils`
# MAGIC and `spark`, which only exist in a Databricks notebook runtime.

# COMMAND ----------

from datetime import date

from src.ingestion.open_meteo_pipeline import (
    backfill_date_range,
    run_ingestion,
)
from src.orchestration.processing_window import (
    BACKFILL,
    VALID_EXECUTION_MODES,
    resolve_open_meteo_window,
)

# COMMAND ----------

dbutils.widgets.dropdown("execution_mode", "daily", list(VALID_EXECUTION_MODES), "Execution mode")
dbutils.widgets.text("start_date", "", "Start date (backfill/reprocess, YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End date (backfill/reprocess, YYYY-MM-DD)")

execution_mode = dbutils.widgets.get("execution_mode")
start_date_param = dbutils.widgets.get("start_date")
end_date_param = dbutils.widgets.get("end_date")

# COMMAND ----------

explicit_start = date.fromisoformat(start_date_param) if start_date_param else None
explicit_end = date.fromisoformat(end_date_param) if end_date_param else None

# Spec 001: backfill defaults to the previous 24 months when no explicit
# range is supplied, rather than requiring the operator to type it out.
if execution_mode == BACKFILL and (explicit_start is None or explicit_end is None):
    default_start, default_end = backfill_date_range(end_date=explicit_end)
    explicit_start = explicit_start or default_start
    explicit_end = explicit_end or default_end

start_date, end_date = resolve_open_meteo_window(
    execution_mode, start_date=explicit_start, end_date=explicit_end
)

# COMMAND ----------

result = run_ingestion(start_date, end_date, spark=spark)

print(f"Execution mode:      {execution_mode}")
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
