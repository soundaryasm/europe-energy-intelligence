# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Gold -> PostgreSQL (Spec 005)
# MAGIC
# MAGIC Workflow entry point for the `publish_postgres` task. Reads the five
# MAGIC approved Gold Delta tables (built by dbt, Spec 004) and upserts them
# MAGIC into PostgreSQL via `src/serving/postgres_publisher.py`.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `spark`
# MAGIC and `dbutils`, which only exist in a Databricks notebook runtime.
# MAGIC
# MAGIC The Gold tables are filtered to the canonical processing window
# MAGIC (Spec 006) before being collected, so a daily run publishes a few
# MAGIC days rather than the whole history — see Spec 005 "Daily
# MAGIC Incremental Load".

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

import os
from datetime import date

from src.orchestration.processing_window import (
    DEFAULT_LOOKBACK_DAYS,
    resolve_window,
)
from src.transformations.silver_window import date_key_bounds
from src.serving.postgres_connection import connect, load_connection_config_from_env
from src.serving.postgres_publisher import DEFAULT_TABLE_ORDER, publish
from src.serving.postgres_schema import ALL_TABLE_DDL, INDEX_DDL

# COMMAND ----------

dbutils.widgets.text("gold_catalog", "", "Gold catalog.schema (e.g. main.gold)")
dbutils.widgets.text("secret_scope", "europe-energy-intelligence", "Databricks secret scope")
dbutils.widgets.text("secret_key", "POSTGRES_URL", "Databricks secret key")
dbutils.widgets.text("start_date", "", "Start date (YYYY-MM-DD; blank = lookback_days back)")
dbutils.widgets.text("end_date", "", "End date (YYYY-MM-DD; blank = latest completed date)")
dbutils.widgets.text("lookback_days", str(DEFAULT_LOOKBACK_DAYS), "Lookback when start_date is blank (days)")

gold_catalog = dbutils.widgets.get("gold_catalog")
secret_scope = dbutils.widgets.get("secret_scope")
secret_key = dbutils.widgets.get("secret_key")
_start_param = dbutils.widgets.get("start_date")
_end_param = dbutils.widgets.get("end_date")
lookback_days = int(dbutils.widgets.get("lookback_days"))

# Spec 005: "After the initial load, normal publication should process
# only affected dates." Collecting every Gold row on every run means
# pulling ~1M rows to the driver as Python dicts and upserting all of
# them to add one day. The window is the same canonical one the
# ingestion and Silver tasks use.
#
# Gold itself is still a full rebuild (its models are
# `materialized='table'`), which is cheap because it reads Silver rather
# than Bronze. The consequence is that changing a Gold calculation
# leaves older serving rows stale until a `reprocess` run republishes
# the affected range — that is what reprocess with an explicit
# start_date/end_date is for.
publish_start, publish_end = resolve_window(
    start_date=date.fromisoformat(_start_param) if _start_param else None,
    end_date=date.fromisoformat(_end_param) if _end_param else None,
    lookback_days=lookback_days,
)
low_date_key, high_date_key = date_key_bounds(publish_start, publish_end)

print(f"Publish window:  {publish_start} to {publish_end} "
      f"(date_key {low_date_key}..{high_date_key})")

# Connection URL retrieved via Databricks-managed secrets only — never
# hard-coded, never logged, never committed.
os.environ["POSTGRES_URL"] = dbutils.secrets.get(scope=secret_scope, key=secret_key)

# COMMAND ----------

connection_config = load_connection_config_from_env()
connection = connect(connection_config)

try:
    with connection.cursor() as cursor:
        for statement in (*ALL_TABLE_DDL, *INDEX_DDL):
            cursor.execute(statement)
    connection.commit()

    # `dim_country` has no date grain and `dim_date` must stay complete
    # for the fact tables' foreign keys to resolve, so both publish
    # whole. They are 15 and ~4,700 rows. The facts carry `date_key` and
    # are windowed.
    UNWINDOWED_TABLES = {"dim_country", "dim_date"}

    tables = {}
    for table_name in DEFAULT_TABLE_ORDER:
        df = spark.table(f"{gold_catalog}.{table_name}")
        if table_name not in UNWINDOWED_TABLES:
            df = df.filter(
                (df["date_key"] >= low_date_key) & (df["date_key"] <= high_date_key)
            )
        tables[table_name] = [row.asDict() for row in df.collect()]
        print(f"  {table_name}: {len(tables[table_name])} row(s) to publish")

    result = publish(connection, tables)

    print(f"Tables processed: {result.tables_processed}")
    print(f"Rows written:     {result.rows_written}")
    print(f"Failed tables:    {result.failed_tables}")

    if not result.all_succeeded:
        raise RuntimeError(f"PostgreSQL publish failed for: {result.failed_tables} ({result.errors})")
finally:
    connection.close()
