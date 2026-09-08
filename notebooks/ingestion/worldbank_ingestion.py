# Databricks notebook source
# MAGIC %md
# MAGIC # World Bank Indicators Bronze Ingestion
# MAGIC
# MAGIC Workflow entry point for the `ingest_worldbank` task. All business logic
# MAGIC lives in `src/ingestion/worldbank_pipeline.py`; this notebook only reads
# MAGIC the `year` parameter and wires up the active `spark` session.
# MAGIC
# MAGIC Deliberately dumb/simple by design: there is no execution_mode and no
# MAGIC date-range concept here, unlike ENTSO-E/Open-Meteo. This always fetches
# MAGIC exactly one calendar year for every configured country. The daily
# MAGIC pipeline should pass the current year; a future backfill run should
# MAGIC pass whichever year the backfill month falls in — the year to fetch is
# MAGIC derived from whatever period is already being processed elsewhere,
# MAGIC not tracked separately here. Re-fetching the same year repeatedly
# MAGIC (once a day, or many times during a backfill covering several months
# MAGIC of the same year) is expected and cheap — no rate limit is documented
# MAGIC for the World Bank API, and idempotent Bronze MERGE makes reruns safe.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `spark`,
# MAGIC which only exists in a Databricks notebook runtime.

# COMMAND ----------

from datetime import datetime, timezone as dt_timezone

from src.ingestion.worldbank_pipeline import run_ingestion

# COMMAND ----------

dbutils.widgets.text("year", str(datetime.now(dt_timezone.utc).year), "Year to fetch (defaults to current year)")

year = int(dbutils.widgets.get("year"))

# COMMAND ----------

result = run_ingestion(year, spark=spark)

print(f"Year:                 {result.year}")
print(f"Indicators attempted: {result.indicators_attempted}")
print(f"Indicators succeeded: {result.indicators_succeeded}")
print(f"Indicators failed:    {result.indicators_failed}")
print(f"Records written:      {result.records_written}")

if not result.succeeded:
    raise RuntimeError(
        f"World Bank ingestion failed for indicators: {result.indicators_failed} "
        f"({result.errors})"
    )
