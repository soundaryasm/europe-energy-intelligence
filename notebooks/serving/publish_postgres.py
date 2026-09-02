# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Gold -> PostgreSQL (Spec 005)
# MAGIC
# MAGIC Workflow entry point for the `publish_postgres` task. Reads the five
# MAGIC approved Gold Delta tables (built by dbt, Spec 004) and upserts them
# MAGIC into PostgreSQL via `src/serving/postgres_publisher.py`.
# MAGIC
# MAGIC This notebook must be executed on Databricks — it relies on `spark`
# MAGIC and `dbutils`, which only exist in a Databricks notebook runtime, and
# MAGIC it has never been run here: no Databricks Gold tables or PostgreSQL
# MAGIC credentials are available in this environment.

# COMMAND ----------

import os

from src.serving.postgres_connection import connect, load_connection_config_from_env
from src.serving.postgres_publisher import DEFAULT_TABLE_ORDER, publish
from src.serving.postgres_schema import ALL_TABLE_DDL, INDEX_DDL

# COMMAND ----------

dbutils.widgets.text("gold_catalog", "", "Gold catalog.schema (e.g. main.gold)")
dbutils.widgets.text("secret_scope", "postgres", "Databricks secret scope")

gold_catalog = dbutils.widgets.get("gold_catalog")
secret_scope = dbutils.widgets.get("secret_scope")

# Connection URL retrieved via Databricks-managed secrets only — never
# hard-coded, never logged, never committed. Most managed PostgreSQL
# providers hand out one connection URL rather than separate fields;
# discrete POSTGRES_HOST/USER/etc. env vars remain supported as a
# fallback (see src/serving/postgres_connection.py) if that's what a
# given provider exposes instead.
os.environ["POSTGRES_URL"] = dbutils.secrets.get(scope=secret_scope, key="url")

# COMMAND ----------

connection_config = load_connection_config_from_env()
connection = connect(connection_config)

try:
    with connection.cursor() as cursor:
        for statement in (*ALL_TABLE_DDL, *INDEX_DDL):
            cursor.execute(statement)
    connection.commit()

    tables = {
        table_name: [row.asDict() for row in spark.table(f"{gold_catalog}.{table_name}").collect()]
        for table_name in DEFAULT_TABLE_ORDER
    }

    result = publish(connection, tables)

    print(f"Tables processed: {result.tables_processed}")
    print(f"Rows written:     {result.rows_written}")
    print(f"Failed tables:    {result.failed_tables}")

    if not result.all_succeeded:
        raise RuntimeError(f"PostgreSQL publish failed for: {result.failed_tables} ({result.errors})")
finally:
    connection.close()
