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

from src.config.countries import load_countries
from src.ingestion.open_meteo_pipeline import BRONZE_TABLE_NAME as OPEN_METEO_BRONZE_TABLE
from src.ingestion.entsoe_pipeline import BRONZE_TABLE_NAME as ENTSOE_BRONZE_TABLE
from src.ingestion.worldbank_pipeline import BRONZE_TABLE_NAME as WORLDBANK_BRONZE_TABLE
from src.transformations.silver_energy_demand import build_silver_energy_demand_daily
from src.transformations.silver_energy_price import build_silver_energy_price_daily
from src.transformations.silver_generation_mix import build_silver_generation_mix_daily
from src.transformations.silver_weather import build_silver_weather_daily
from src.transformations.silver_worldbank import build_silver_worldbank_annual
from src.transformations.silver_writer import write_silver_table

# COMMAND ----------

country_timezones = {c.country_code: c.timezone for c in load_countries()}

open_meteo_bronze = spark.table(OPEN_METEO_BRONZE_TABLE)
entsoe_bronze = spark.table(ENTSOE_BRONZE_TABLE)

# World Bank Bronze may not exist yet on a fresh workspace (its
# ingestion task can legitimately write zero rows — e.g. the current
# year's indicators not published yet — and never create the table).
# Silver must not fail the whole run over a source that simply hasn't
# landed anything yet.
worldbank_bronze = (
    spark.table(WORLDBANK_BRONZE_TABLE) if spark.catalog.tableExists(WORLDBANK_BRONZE_TABLE) else None
)

# COMMAND ----------

weather_daily = build_silver_weather_daily(open_meteo_bronze)
weather_written = write_silver_table(
    spark, weather_daily, "silver_weather_daily", key_cols=["country_code", "local_date"]
)

demand_daily = build_silver_energy_demand_daily(entsoe_bronze, country_timezones)
demand_written = write_silver_table(
    spark, demand_daily, "silver_energy_demand_daily", key_cols=["country_code", "local_date"]
)

price_daily = build_silver_energy_price_daily(entsoe_bronze, country_timezones)
price_written = write_silver_table(
    spark, price_daily, "silver_energy_price_daily", key_cols=["country_code", "local_date"]
)

generation_mix_daily = build_silver_generation_mix_daily(entsoe_bronze, country_timezones)
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
