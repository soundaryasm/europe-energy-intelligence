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

from src.config.countries import load_countries
from src.ingestion.open_meteo_pipeline import BRONZE_TABLE_NAME as OPEN_METEO_BRONZE_TABLE
from src.ingestion.entsoe_pipeline import BRONZE_TABLE_NAME as ENTSOE_BRONZE_TABLE
from src.transformations.silver_energy_demand import build_silver_energy_demand_daily
from src.transformations.silver_energy_price import build_silver_energy_price_daily
from src.transformations.silver_generation_mix import build_silver_generation_mix_daily
from src.transformations.silver_weather import build_silver_weather_daily
from src.transformations.silver_writer import write_silver_table

# COMMAND ----------

country_timezones = {c.country_code: c.timezone for c in load_countries()}

open_meteo_bronze = spark.table(OPEN_METEO_BRONZE_TABLE)
entsoe_bronze = spark.table(ENTSOE_BRONZE_TABLE)

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

# COMMAND ----------

print(f"silver_weather_daily rows written:          {weather_written}")
print(f"silver_energy_demand_daily rows written:     {demand_written}")
print(f"silver_energy_price_daily rows written:      {price_written}")
print(f"silver_generation_mix_daily rows written:     {generation_mix_written}")
