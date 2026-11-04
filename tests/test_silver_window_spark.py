"""Spark-backed tests for the Silver/serving date window (Spec 006).

Skipped where pyspark is unavailable, as the other `*_spark.py` tests
are. The bounds arithmetic these two functions apply is covered without
Spark in `test_silver_window.py`; what is exercised here is that the
filters are wired to the right columns and inclusivity.
"""
from datetime import date

import pytest

pytest.importorskip("pyspark")

from src.transformations.silver_window import filter_bronze, restrict_to_window


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[1]").appName("window-tests").getOrCreate()
    yield session
    session.stop()


def test_filter_bronze_keeps_the_padded_window_only(spark):
    df = spark.createDataFrame(
        [
            ("2026-10-29T12:00:00Z",),  # before the pad
            ("2026-10-31T12:00:00Z",),  # the pad itself
            ("2026-11-02T12:00:00Z",),  # inside
            ("2026-11-04T23:45:00Z",),  # trailing pad, last instant
            ("2026-11-05T00:00:00Z",),  # past the exclusive upper bound
        ],
        "source_timestamp string",
    )

    kept = {
        r["source_timestamp"]
        for r in filter_bronze(df, "source_timestamp", date(2026, 11, 1), date(2026, 11, 3)).collect()
    }

    assert kept == {
        "2026-10-31T12:00:00Z", "2026-11-02T12:00:00Z", "2026-11-04T23:45:00Z",
    }


def test_restrict_to_window_is_inclusive_and_drops_the_pad(spark):
    # The edge days computed from padded input must not be written: a
    # partial day would merge over a complete one.
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [("2026-10-31",), ("2026-11-01",), ("2026-11-03",), ("2026-11-04",)],
        "local_date string",
    ).withColumn("local_date", F.to_date("local_date"))

    kept = sorted(
        r["local_date"].isoformat()
        for r in restrict_to_window(df, date(2026, 11, 1), date(2026, 11, 3)).collect()
    )

    assert kept == ["2026-11-01", "2026-11-03"]


def test_restrict_to_window_rejects_an_inverted_window(spark):
    df = spark.createDataFrame([("2026-11-01",)], "local_date string")
    with pytest.raises(ValueError, match="must not be after"):
        restrict_to_window(df, date(2026, 11, 3), date(2026, 11, 1))
