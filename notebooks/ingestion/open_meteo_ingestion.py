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

# Bundle deployments run this notebook from a plain Workspace Files
# location, not a Databricks Repo — which does not add the repo root to
# sys.path automatically the way a Repo clone does. Confirmed via a
# real deployed run failing with `ModuleNotFoundError: No module named
# 'src'` (2026-09-09) despite the identical code working fine from a
# Repos-synced folder. Must run before any `from src...` import below.
dbutils.widgets.text("bundle_root", "", "Workspace root to add to sys.path (bundle deployments)")

import sys

_bundle_root = dbutils.widgets.get("bundle_root")
if _bundle_root and _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------

from datetime import date

from src.ingestion.open_meteo_client import select_endpoint
from src.ingestion.open_meteo_pipeline import run_ingestion
from src.orchestration.processing_window import (
    DEFAULT_OPEN_METEO_LOOKBACK_DAYS,
    VALID_EXECUTION_MODES,
    resolve_open_meteo_window,
)

# COMMAND ----------

dbutils.widgets.dropdown("execution_mode", "daily", list(VALID_EXECUTION_MODES), "Execution mode")
dbutils.widgets.text("start_date", "", "Start date (reprocess, YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End date (reprocess, YYYY-MM-DD)")
dbutils.widgets.text(
    "lookback_days", str(DEFAULT_OPEN_METEO_LOOKBACK_DAYS), "Daily-mode recent-date lookback (days)"
)

execution_mode = dbutils.widgets.get("execution_mode")
start_date_param = dbutils.widgets.get("start_date")
end_date_param = dbutils.widgets.get("end_date")
lookback_days = int(dbutils.widgets.get("lookback_days"))

# COMMAND ----------

explicit_start = date.fromisoformat(start_date_param) if start_date_param else None
explicit_end = date.fromisoformat(end_date_param) if end_date_param else None

# `reprocess` requires both dates explicitly: re-running a range is a
# deliberate, operator-chosen act, and a defaulted range is exactly the
# kind of thing that gets run by accident against the wrong window.
start_date, end_date = resolve_open_meteo_window(
    execution_mode, start_date=explicit_start, end_date=explicit_end, lookback_days=lookback_days
)

# COMMAND ----------

# Chosen from the window's oldest date, not from execution_mode. Recent
# dates go to the Forecast API, whose operational models refresh every
# 1-6 hours; settled dates go to Archive (ERA5), which has a real
# settlement lag. See `select_endpoint` for why mode was the wrong rule.
endpoint_url, source_endpoint_label = select_endpoint(start_date)

result = run_ingestion(
    start_date, end_date, spark=spark,
    endpoint_url=endpoint_url, source_endpoint_label=source_endpoint_label,
)

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
