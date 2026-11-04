# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformations (Spec 003)
# MAGIC
# MAGIC Workflow entry point for the `transform_silver` task. Reads the Bronze
# MAGIC Delta tables produced by Spec 001/002 ingestion, runs the four real
# MAGIC PySpark Silver builders (`src/transformations/silver_*.py`), and
# MAGIC MERGEs the results into the four Silver Delta tables.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `spark`,
# MAGIC which only exists in a Databricks notebook runtime, and it has never
# MAGIC been run here: this environment has no PySpark installation and no
# MAGIC Bronze Delta tables to read.

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

from src.config.countries import load_countries
from src.ingestion.open_meteo_pipeline import BRONZE_TABLE_NAME as OPEN_METEO_BRONZE_TABLE
from src.ingestion.entsoe_pipeline import BRONZE_TABLE_NAME as ENTSOE_BRONZE_TABLE
from src.ingestion.worldbank_pipeline import BRONZE_TABLE_NAME as WORLDBANK_BRONZE_TABLE
from src.transformations.silver_energy_demand import build_silver_energy_demand_daily
from src.transformations.silver_energy_price import build_silver_energy_price_daily
from src.transformations.silver_generation_mix import build_silver_generation_mix_daily
from src.transformations.silver_weather import build_silver_weather_daily
from src.transformations.silver_worldbank import build_silver_worldbank_annual
from src.transformations.silver_window import (
    filter_bronze,
    restrict_to_window,
)
from src.transformations.silver_writer import write_silver_table
from src.orchestration.processing_window import (
    DEFAULT_ENTSOE_LOOKBACK_DAYS,
    VALID_EXECUTION_MODES,
    resolve_entsoe_window,
    resolve_open_meteo_window,
)

# COMMAND ----------

# The same canonical processing window the ingestion tasks use (Spec
# 006), so Silver rebuilds the days that were just ingested rather than
# all twelve years on every run. A `reprocess` run with an explicit
# range is how the whole history is (re)built — that is the only mode
# that should ever span years.
dbutils.widgets.dropdown("execution_mode", "daily", list(VALID_EXECUTION_MODES), "Execution mode")
dbutils.widgets.text("start_date", "", "Start date (reprocess, YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End date (reprocess, YYYY-MM-DD)")
dbutils.widgets.text(
    "lookback_days", str(DEFAULT_ENTSOE_LOOKBACK_DAYS), "Daily-mode lookback (days)"
)

execution_mode = dbutils.widgets.get("execution_mode")
_start_param = dbutils.widgets.get("start_date")
_end_param = dbutils.widgets.get("end_date")
lookback_days = int(dbutils.widgets.get("lookback_days"))

explicit_start = date.fromisoformat(_start_param) if _start_param else None
explicit_end = date.fromisoformat(_end_param) if _end_param else None

# Resolved per source, matching how ingestion resolves them, so Silver
# never processes a day the matching ingestion task did not fetch.
entsoe_start, entsoe_end = resolve_entsoe_window(
    execution_mode, start_date=explicit_start, end_date=explicit_end,
    lookback_days=lookback_days,
)
weather_start, weather_end = resolve_open_meteo_window(
    execution_mode, start_date=explicit_start, end_date=explicit_end,
    lookback_days=lookback_days,
)

print(f"Execution mode:  {execution_mode}")
print(f"ENTSO-E window:  {entsoe_start} to {entsoe_end}")
print(f"Weather window:  {weather_start} to {weather_end}")

# COMMAND ----------

country_timezones = {c.country_code: c.timezone for c in load_countries()}

# Read padded, write only the window — see `silver_window` for why both
# halves are needed.
open_meteo_bronze = filter_bronze(
    spark.table(OPEN_METEO_BRONZE_TABLE), "observation_date", weather_start, weather_end,
)
entsoe_bronze = filter_bronze(
    spark.table(ENTSOE_BRONZE_TABLE), "source_timestamp", entsoe_start, entsoe_end,
)

# World Bank Bronze may not exist yet on a fresh workspace (its
# ingestion task can legitimately write zero rows — e.g. the current
# year's indicators not published yet — and never create the table).
# Silver must not fail the whole run over a source that simply hasn't
# landed anything yet.
worldbank_bronze = (
    spark.table(WORLDBANK_BRONZE_TABLE) if spark.catalog.tableExists(WORLDBANK_BRONZE_TABLE) else None
)

# COMMAND ----------

weather_daily = restrict_to_window(
    build_silver_weather_daily(open_meteo_bronze), weather_start, weather_end,
)
weather_written = write_silver_table(
    spark, weather_daily, "silver_weather_daily", key_cols=["country_code", "local_date"]
)

demand_daily = restrict_to_window(
    build_silver_energy_demand_daily(entsoe_bronze, country_timezones), entsoe_start, entsoe_end,
)
demand_written = write_silver_table(
    spark, demand_daily, "silver_energy_demand_daily", key_cols=["country_code", "local_date"]
)

price_daily = restrict_to_window(
    build_silver_energy_price_daily(entsoe_bronze, country_timezones), entsoe_start, entsoe_end,
)
price_written = write_silver_table(
    spark, price_daily, "silver_energy_price_daily", key_cols=["country_code", "local_date"]
)

generation_mix_daily = restrict_to_window(
    build_silver_generation_mix_daily(entsoe_bronze, country_timezones), entsoe_start, entsoe_end,
)
generation_mix_written = write_silver_table(
    spark,
    generation_mix_daily,
    "silver_generation_mix_daily",
    key_cols=["country_code", "local_date", "normalized_production_type"],
)

worldbank_written = 0
if worldbank_bronze is not None:
    worldbank_annual = build_silver_worldbank_annual(worldbank_bronze)
    worldbank_written = write_silver_table(
        spark, worldbank_annual, "silver_worldbank_annual", key_cols=["country_code", "year"]
    )

# COMMAND ----------

print(f"silver_weather_daily rows written:          {weather_written}")
print(f"silver_energy_demand_daily rows written:     {demand_written}")
print(f"silver_energy_price_daily rows written:      {price_written}")
print(f"silver_generation_mix_daily rows written:     {generation_mix_written}")
print(f"silver_worldbank_annual rows written:         {worldbank_written}")
