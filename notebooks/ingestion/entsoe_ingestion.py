# Databricks notebook source
# MAGIC %md
# MAGIC # ENTSO-E Bronze Ingestion (Spec 002)
# MAGIC
# MAGIC Workflow entry point for the `ingest_entsoe` task. All business logic
# MAGIC lives in `src/ingestion/entsoe_pipeline.py`; this notebook only reads
# MAGIC job parameters, resolves the canonical processing window (Spec 006 —
# MAGIC a rolling `lookback_days`-day window, not just yesterday, because
# MAGIC ENTSO-E values may be revised after first publication), retrieves the
# MAGIC ENTSO-E security token from Databricks secrets, and wires up the
# MAGIC active `spark` session.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `dbutils`
# MAGIC and `spark`, which only exist in a Databricks notebook runtime.
# MAGIC
# MAGIC Every domain in `src/config/entsoe_domains.yaml` is now
# MAGIC `validated: true`, confirmed against live ENTSO-E. Ireland is the
# MAGIC exception worth knowing about: its load is published only under
# MAGIC the EirGrid control area, not the SEM bidding zone in that file,
# MAGIC so a dataset that comes back with no data is re-requested under
# MAGIC `src/config/entsoe.ALTERNATIVE_DOMAINS` — see Spec 002
# MAGIC "Alternative Domains".

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

from src.ingestion.entsoe_pipeline import run_ingestion
from src.orchestration.processing_window import (
    DEFAULT_LOOKBACK_DAYS,
    entsoe_fetch_window,
    resolve_window,
)

# COMMAND ----------

dbutils.widgets.text("start_date", "", "Start date (YYYY-MM-DD; blank = lookback_days back)")
dbutils.widgets.text("end_date", "", "End date (YYYY-MM-DD; blank = latest completed date)")
dbutils.widgets.text("lookback_days", str(DEFAULT_LOOKBACK_DAYS), "Lookback when start_date is blank (days)")
dbutils.widgets.text("secret_scope", "europe-energy-intelligence", "Databricks secret scope")
dbutils.widgets.text("secret_key", "ENTSOE_API_TOKEN", "Databricks secret key")

start_date_param = dbutils.widgets.get("start_date")
end_date_param = dbutils.widgets.get("end_date")
lookback_days = int(dbutils.widgets.get("lookback_days"))
secret_scope = dbutils.widgets.get("secret_scope")
secret_key = dbutils.widgets.get("secret_key")

# COMMAND ----------

explicit_start = date.fromisoformat(start_date_param) if start_date_param else None
explicit_end = date.fromisoformat(end_date_param) if end_date_param else None

start_date, end_date = resolve_window(
    start_date=explicit_start,
    end_date=explicit_end,
    lookback_days=lookback_days,
)

# ENTSO-E is asked for one extra day at the start, so the first local
# hour of the window's oldest day is covered — see `entsoe_fetch_window`.
fetch_start, fetch_end = entsoe_fetch_window(start_date, end_date)

# COMMAND ----------

# Token retrieved via Databricks-managed secrets only — never hard-coded,
# never logged, never committed.
security_token = dbutils.secrets.get(scope=secret_scope, key=secret_key)

result = run_ingestion(fetch_start, fetch_end, token=security_token, spark=spark)

print(f"Processing window:     {start_date} to {end_date}")
print(f"Fetched range:         {result.start_date} to {result.end_date}")
print(f"Countries attempted:   {result.countries_attempted}")
print(f"Datasets attempted:    {result.datasets_attempted}")
print(f"Succeeded (country:dataset):    {result.succeeded}")
print(f"Unavailable (country:dataset):  {result.unavailable}")
print(f"Failed (country:dataset):       {result.failed}")
print(f"Records written:       {result.records_written}")

# A legitimately unavailable source (Spec 006 "Partial Source Data") must
# not fail the task — only a genuine technical/invalid-response failure
# does.
if not result.all_succeeded:
    raise RuntimeError(f"ENTSO-E ingestion failed for: {result.failed} ({result.errors})")
