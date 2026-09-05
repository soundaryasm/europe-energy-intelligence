"""Shared pytest fixtures.

`spark_session` is only used by tests marked `@pytest.mark.databricks`
(see tests/test_silver_*_spark.py). PySpark is imported lazily inside the
fixture body, not at module import time, so collecting this conftest
never requires PySpark to be installed. Those test modules independently
call `pytest.importorskip("pyspark")` at module scope, so on a machine
without PySpark (e.g. this local dev environment) they are skipped before
this fixture is ever requested.
"""
import pytest


@pytest.fixture(scope="session")
def spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    # Databricks Serverless runs through Spark Connect, which does not
    # allow `.master(...)` to be set — and the notebook process already
    # has an active session running for it. Reuse that session instead of
    # trying to configure one, and never .stop() a session we didn't
    # create ourselves.
    active = SparkSession.getActiveSession()
    if active is not None:
        yield active
        return

    # No active session (plain local/CI run, no Databricks environment):
    # same local Spark fallback as before.
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("europe-energy-intelligence-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
