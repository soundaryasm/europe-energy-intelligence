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

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("europe-energy-intelligence-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
