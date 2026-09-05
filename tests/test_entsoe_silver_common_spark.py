"""Databricks-runtime tests for DST-transition handling in the shared
ENTSO-E Silver helpers (Spec 003).

See `test_silver_weather_spark.py` for why these are skipped locally
(`pytest.importorskip("pyspark")`) and only ever run for real on a
machine/cluster with PySpark installed.

EU-wide DST transitions happen at 01:00 UTC on the last Sunday of March
(spring forward) and October (fall back), for every EU timezone
simultaneously — only the local wall-clock effect differs per zone. 2026's
transitions are 2026-03-29 (23-hour local day) and 2026-10-25 (25-hour
local day), confirmed directly against Python's `zoneinfo` (the same
library `entsoe_silver_common._expected_day_duration_hours` uses) rather
than assumed:

    Europe/Dublin 2026-03-29: UTC [2026-03-29T00:00, 2026-03-29T23:00) = 23h
    Europe/Dublin 2026-10-25: UTC [2026-10-24T23:00, 2026-10-26T00:00) = 25h
    Europe/Berlin 2026-03-29: UTC [2026-03-28T23:00, 2026-03-29T22:00) = 23h
    Europe/Berlin 2026-10-25: UTC [2026-10-24T22:00, 2026-10-25T23:00) = 25h
"""
from datetime import date, datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from src.ingestion.delta_schema import entsoe_bronze_schema
from src.transformations.entsoe_silver_common import (
    _expected_day_duration_hours,
    with_completeness_status,
    with_local_date,
)
from src.transformations.silver_energy_demand import build_silver_energy_demand_daily

pytestmark = pytest.mark.databricks

# (tz_name, local_date, utc_window_start, expected_hours)
DST_CASES = [
    ("Europe/Dublin", date(2026, 3, 29), datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc), 23),
    ("Europe/Dublin", date(2026, 10, 25), datetime(2026, 10, 24, 23, 0, tzinfo=timezone.utc), 25),
    ("Europe/Berlin", date(2026, 3, 29), datetime(2026, 3, 28, 23, 0, tzinfo=timezone.utc), 23),
    ("Europe/Berlin", date(2026, 10, 25), datetime(2026, 10, 24, 22, 0, tzinfo=timezone.utc), 25),
]


@pytest.mark.parametrize("tz_name, local_date, utc_start, expected_hours", DST_CASES)
def test_expected_day_duration_hours_on_dst_transition_dates(tz_name, local_date, utc_start, expected_hours):
    assert _expected_day_duration_hours(local_date, tz_name) == pytest.approx(expected_hours)


@pytest.mark.parametrize("tz_name, local_date, utc_start, expected_hours", DST_CASES)
def test_expected_day_duration_hours_is_24_the_day_before_and_after(tz_name, local_date, utc_start, expected_hours):
    # Control: the transition is isolated to exactly one calendar date —
    # neighbouring days must still report a plain 24 hours.
    assert _expected_day_duration_hours(local_date - timedelta(days=1), tz_name) == pytest.approx(24.0)
    assert _expected_day_duration_hours(local_date + timedelta(days=1), tz_name) == pytest.approx(24.0)


def _hourly_bronze_rows(country_code: str, domain: str, utc_start: datetime, n_hours: int) -> list:
    """Bronze-shaped `load` rows for exactly one DST test window.

    Every field present in `entsoe_bronze_schema()` is set explicitly, so
    these rows can be loaded with that schema instead of relying on
    per-column type inference (which fails on Spark Connect/Serverless
    when an all-None column, e.g. `production_type_raw` here, gives it
    nothing to infer a type from). `production_type_raw` and `currency`
    remain `None` because that's what a real `load` Bronze row has — a
    `load` document carries no psrType or currency (see
    `entsoe_bronze.build_bronze_records`) — not filled with a placeholder
    just to satisfy inference. `business_type` is `"A04"` (not None)
    because that's what a real `load` row's businessType actually is.
    """
    return [
        {
            "country_code": country_code,
            "domain": domain,
            "dataset_type": "load",
            "source_timestamp": (utc_start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_resolution": "PT60M",
            "value": 1000.0,
            "unit": "MAW",
            "production_type_raw": None,
            "business_type": "A04",
            "currency": None,
            "source_document_mrid": "doc-dst-test",
            "requested_start_date": utc_start.date().isoformat(),
            "requested_end_date": utc_start.date().isoformat(),
            "source_system": "entsoe",
            "ingestion_timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
        }
        for h in range(n_hours)
    ]


@pytest.mark.parametrize("tz_name, local_date, utc_start, expected_hours", DST_CASES)
def test_with_local_date_buckets_dst_window_into_one_local_date_without_losing_rows(
    spark_session, tz_name, local_date, utc_start, expected_hours
):
    # Every real UTC hour in the transition day's true window — 23 for
    # spring forward, 25 for fall back — must land in exactly one
    # local_date bucket. Fall back specifically repeats a wall-clock hour
    # locally, but since bucketing goes UTC -> local (never the reverse),
    # there is no ambiguity to lose a row to: all 25 distinct UTC
    # timestamps must survive as 25 distinct rows for that one local_date.
    rows = _hourly_bronze_rows("XX", "domain", utc_start, expected_hours)
    df = spark_session.createDataFrame(rows, schema=entsoe_bronze_schema())

    result = with_local_date(df, {"XX": tz_name}).collect()

    assert len(result) == expected_hours
    assert {r.local_date for r in result} == {local_date}


@pytest.mark.parametrize("tz_name, local_date, utc_start, expected_hours", DST_CASES)
def test_with_completeness_status_marks_fully_covered_dst_day_as_complete(
    spark_session, tz_name, local_date, utc_start, expected_hours
):
    # A spring-forward day only ever HAS 23 real hours of data (the
    # skipped wall-clock hour was never observed by anything, ENTSO-E
    # included) — that must not be falsely read as 1 missing hour of
    # otherwise-available data and marked `partial`.
    rows = _hourly_bronze_rows("XX", "domain", utc_start, expected_hours)
    df = with_local_date(spark_session.createDataFrame(rows, schema=entsoe_bronze_schema()), {"XX": tz_name})
    aggregated = df.groupBy("country_code", "local_date").agg({"source_timestamp": "count"}).withColumnRenamed(
        "count(source_timestamp)", "source_interval_count"
    )
    from pyspark.sql import functions as F

    aggregated = aggregated.withColumn("covered_duration_hours", F.lit(float(expected_hours)))

    result = with_completeness_status(aggregated, {"XX": tz_name}).collect()

    assert len(result) == 1
    assert result[0].completeness_status == "complete"


@pytest.mark.parametrize("tz_name, local_date, utc_start, expected_hours", DST_CASES)
def test_build_silver_energy_demand_daily_marks_full_dst_day_complete(
    spark_session, tz_name, local_date, utc_start, expected_hours
):
    # End-to-end through the real Silver builder (not just the shared
    # helpers in isolation): a fully-covered DST-transition day must
    # come out `complete`, with covered_duration_hours matching the
    # real 23/25-hour day length, not a hard-coded 24.
    country_code = "IE" if tz_name == "Europe/Dublin" else "DE"
    rows = _hourly_bronze_rows(country_code, "domain", utc_start, expected_hours)
    bronze_df = spark_session.createDataFrame(rows, schema=entsoe_bronze_schema())

    result = build_silver_energy_demand_daily(bronze_df, {country_code: tz_name}).collect()

    assert len(result) == 1
    row = result[0].asDict()
    assert row["local_date"] == local_date
    assert row["covered_duration_hours"] == pytest.approx(float(expected_hours))
    assert row["completeness_status"] == "complete"


@pytest.mark.parametrize("tz_name, local_date, utc_start, expected_hours", DST_CASES)
def test_build_silver_energy_demand_daily_marks_missing_hour_as_partial_on_dst_day(
    spark_session, tz_name, local_date, utc_start, expected_hours
):
    # One real observation missing from an already-short (spring) or
    # already-long (autumn) DST day must still be caught as `partial` —
    # the DST-aware expected duration must not be so lenient that a
    # genuine gap goes undetected.
    country_code = "IE" if tz_name == "Europe/Dublin" else "DE"
    rows = _hourly_bronze_rows(country_code, "domain", utc_start, expected_hours - 1)
    bronze_df = spark_session.createDataFrame(rows, schema=entsoe_bronze_schema())

    result = build_silver_energy_demand_daily(bronze_df, {country_code: tz_name}).collect()

    assert len(result) == 1
    row = result[0].asDict()
    assert row["covered_duration_hours"] == pytest.approx(float(expected_hours - 1))
    assert row["completeness_status"] == "partial"
