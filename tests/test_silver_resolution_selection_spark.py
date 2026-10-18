"""Databricks-runtime tests for mixed-resolution handling (Spec 003).

See `test_silver_weather_spark.py` for why these are skipped locally
(`pytest.importorskip("pyspark")`) and only ever run for real on a
machine/cluster with PySpark installed.

Since the day-ahead market moved to a 15-minute market time unit (SDAC,
30 September 2025), ENTSO-E publishes some series at two resolutions at
once, with different values for the same instant. These cover what
Silver does with that.
"""
import pytest

pytest.importorskip("pyspark")

from src.ingestion.delta_schema import entsoe_bronze_schema
from src.transformations.entsoe_silver_common import (
    interval_hours_udf,
    with_local_date,
    with_preferred_resolution,
)
from src.transformations.silver_energy_price import build_silver_energy_price_daily

pytestmark = pytest.mark.databricks

IE_TZ = {"IE": "Europe/Dublin"}


def _price_row(timestamp, resolution, value):
    return {
        "country_code": "IE", "domain": "10Y1001A1001A59C", "dataset_type": "price",
        "source_timestamp": timestamp, "source_resolution": resolution,
        "value": value, "unit": None, "production_type_raw": None,
        "business_type": "A62", "currency": "EUR", "source_document_mrid": "doc",
        "requested_start_date": "2026-01-01", "requested_end_date": "2026-01-01",
        "source_system": "entsoe", "ingestion_timestamp": "2026-01-02T00:00:00+00:00",
    }


def _hourly_day(day="2026-01-01", value=100.0):
    return [_price_row(f"{day}T{h:02d}:00:00Z", "PT60M", value) for h in range(24)]


def _quarter_hourly_day(day="2026-01-01", value=80.0):
    return [
        _price_row(f"{day}T{h:02d}:{m:02d}:00Z", "PT15M", value)
        for h in range(24) for m in (0, 15, 30, 45)
    ]


def _half_hourly_day(day="2026-01-01", value=90.0):
    return [
        _price_row(f"{day}T{h:02d}:{m:02d}:00Z", "PT30M", value)
        for h in range(24) for m in (0, 30)
    ]


@pytest.fixture
def spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.master("local[1]").appName("resolution").getOrCreate()


def _frame(spark_session, rows):
    return spark_session.createDataFrame(rows, schema=entsoe_bronze_schema())


def _prepared(spark_session, rows):
    from pyspark.sql import functions as F

    return with_local_date(_frame(spark_session, rows), IE_TZ).withColumn(
        "interval_hours", interval_hours_udf()(F.col("source_resolution"))
    )


def test_hourly_wins_when_both_are_published(spark_session):
    df = _frame(spark_session, _hourly_day() + _quarter_hourly_day())

    result = build_silver_energy_price_daily(df, IE_TZ).collect()

    assert len(result) == 1
    assert result[0]["source_interval_count"] == 24
    assert result[0]["avg_day_ahead_price_eur_mwh"] == pytest.approx(100.0)


def test_hourly_is_used_when_it_is_the_only_resolution(spark_session):
    df = _frame(spark_session, _hourly_day())

    result = build_silver_energy_price_daily(df, IE_TZ).collect()

    assert result[0]["source_interval_count"] == 24


def test_a_series_with_no_hourly_option_falls_back_to_what_exists(spark_session):
    # Not a nicety: most series have no hourly option at all (Irish load
    # and generation publish at PT30M, German at PT15M), so a strict
    # hourly filter would empty them rather than change their grain.
    df = _frame(spark_session, _half_hourly_day())

    result = build_silver_energy_price_daily(df, IE_TZ).collect()

    assert result[0]["source_interval_count"] == 48
    assert result[0]["avg_day_ahead_price_eur_mwh"] == pytest.approx(90.0)


def test_the_finest_is_taken_when_several_sub_hourly_grains_appear(spark_session):
    df = _frame(spark_session, _half_hourly_day() + _quarter_hourly_day())

    result = build_silver_energy_price_daily(df, IE_TZ).collect()

    assert result[0]["source_interval_count"] == 96


def test_a_mixed_day_is_not_double_counted(spark_session):
    # The failure this prevents: an hourly 22:00 contributing a full
    # hour alongside a 15-minute 22:00 contributing a quarter of one.
    df = _frame(spark_session, _hourly_day() + _quarter_hourly_day())

    result = build_silver_energy_price_daily(df, IE_TZ).collect()

    assert result[0]["covered_duration_hours"] == pytest.approx(24.0)


def test_resolution_is_chosen_per_country_day_not_globally(spark_session):
    df = _frame(
        spark_session,
        _hourly_day("2026-01-01") + _quarter_hourly_day("2026-01-02"),
    )

    rows = {
        r["local_date"].isoformat(): r
        for r in build_silver_energy_price_daily(df, IE_TZ).collect()
    }

    assert rows["2026-01-01"]["source_interval_count"] == 24
    assert rows["2026-01-02"]["source_interval_count"] == 96


def test_selection_keeps_every_row_when_one_resolution_is_present(spark_session):
    assert with_preferred_resolution(_prepared(spark_session, _hourly_day())).count() == 24


def test_selection_drops_the_sub_hourly_rows_when_hourly_exists(spark_session):
    kept = with_preferred_resolution(
        _prepared(spark_session, _hourly_day() + _quarter_hourly_day())
    )

    assert kept.count() == 24
    assert {r["source_resolution"] for r in kept.collect()} == {"PT60M"}
