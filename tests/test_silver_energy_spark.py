"""Databricks-runtime tests for the Silver demand/price transformations (Spec 003).

See `test_silver_weather_spark.py` for why these are skipped locally
(`pytest.importorskip("pyspark")`) and only ever run for real on a
machine/cluster with PySpark installed.
"""
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

from src.config.entsoe import EntsoeCountryDomain
from src.ingestion.entsoe_bronze import build_bronze_records
from src.ingestion.entsoe_datasets import LOAD, PRICE
from src.ingestion.entsoe_xml import parse_time_series
from src.transformations.silver_energy_demand import build_silver_energy_demand_daily
from src.transformations.silver_energy_price import build_silver_energy_price_daily
from tests.fixtures_entsoe_xml import LOAD_XML, PRICE_XML

pytestmark = pytest.mark.databricks

IE_DOMAIN = EntsoeCountryDomain("IE", "10Y1001A1001A59C", validated=False)
IE_TZ = {"IE": "Europe/Dublin"}


def _load_bronze_rows():
    from datetime import date

    parsed = parse_time_series(LOAD_XML, LOAD)
    return build_bronze_records(parsed, IE_DOMAIN, LOAD, date(2024, 1, 1), date(2024, 1, 1))


def _price_bronze_rows():
    from datetime import date

    parsed = parse_time_series(PRICE_XML, PRICE)
    return build_bronze_records(parsed, IE_DOMAIN, PRICE, date(2024, 1, 1), date(2024, 1, 1))


def _full_day_load_rows(value_mw: float = 3000.0):
    """24 hourly PT60M load rows covering all of 2024-01-01 UTC.

    2024-01-01 is not a Europe/Dublin DST transition day, so the expected
    local-day duration is exactly 24 hours here — this is a genuinely
    `complete` timeline, built directly as bronze-shaped rows rather than
    via a 24-point XML fixture.
    """
    rows = []
    for hour in range(24):
        rows.append(
            {
                "country_code": "IE",
                "domain": IE_DOMAIN.domain,
                "dataset_type": "load",
                "source_timestamp": f"2024-01-01T{hour:02d}:00:00",
                "source_resolution": "PT60M",
                "value": value_mw,
                "unit": "MAW",
                "production_type_raw": None,
                "currency": None,
                "source_document_mrid": "doc-load-full",
                "requested_start_date": "2024-01-01",
                "requested_end_date": "2024-01-01",
                "source_system": "entsoe",
                "ingestion_timestamp": datetime(2024, 1, 2, tzinfo=timezone.utc).isoformat(),
            }
        )
    return rows


def test_build_silver_energy_demand_daily_converts_power_to_energy(spark_session):
    # LOAD_XML: 3500.5 MW and 3400.2 MW, each over a 60-minute interval.
    bronze_df = spark_session.createDataFrame(_load_bronze_rows())

    result = build_silver_energy_demand_daily(bronze_df, IE_TZ).collect()

    assert len(result) == 1
    row = result[0].asDict()
    assert row["country_code"] == "IE"
    assert str(row["local_date"]) == "2024-01-01"
    assert row["daily_demand_mwh"] == pytest.approx(3500.5 + 3400.2)
    assert row["source_interval_count"] == 2


def test_build_silver_energy_demand_daily_excludes_negative_load(spark_session):
    rows = _load_bronze_rows()
    rows[0]["value"] = -50.0  # implausible negative load
    bronze_df = spark_session.createDataFrame(rows)

    result = build_silver_energy_demand_daily(bronze_df, IE_TZ).collect()

    assert len(result) == 1
    assert result[0].source_interval_count == 1  # the negative reading was dropped


def test_build_silver_energy_demand_daily_dedupes_reruns_to_latest_ingestion(spark_session):
    rows = _load_bronze_rows()
    stale = dict(rows[0])
    stale["value"] = 1.0
    stale["ingestion_timestamp"] = datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat()
    rows[0]["ingestion_timestamp"] = datetime(2024, 1, 9, tzinfo=timezone.utc).isoformat()
    bronze_df = spark_session.createDataFrame([stale] + rows)

    result = build_silver_energy_demand_daily(bronze_df, IE_TZ).collect()

    assert result[0].source_interval_count == 2  # not 3 — the stale duplicate is dropped
    assert result[0].daily_demand_mwh == pytest.approx(3500.5 + 3400.2)


def test_build_silver_energy_demand_daily_marks_short_timeline_as_partial(spark_session):
    # LOAD_XML only covers 2 of the ~24 expected hours for 2024-01-01 in
    # Europe/Dublin — a real partial day (Spec 003 "Demand Completeness"),
    # not proof of a complete one.
    bronze_df = spark_session.createDataFrame(_load_bronze_rows())

    row = build_silver_energy_demand_daily(bronze_df, IE_TZ).collect()[0].asDict()

    assert row["covered_duration_hours"] == pytest.approx(2.0)
    assert row["completeness_status"] == "partial"


def test_build_silver_energy_demand_daily_marks_full_timeline_as_complete(spark_session):
    bronze_df = spark_session.createDataFrame(_full_day_load_rows())

    row = build_silver_energy_demand_daily(bronze_df, IE_TZ).collect()[0].asDict()

    assert row["covered_duration_hours"] == pytest.approx(24.0)
    assert row["completeness_status"] == "complete"
    assert row["daily_demand_mwh"] == pytest.approx(3000.0 * 24)


def test_build_silver_energy_price_daily_computes_weighted_average_and_extremes(spark_session):
    # PRICE_XML: -5.32 and 45.10 EUR/MWh, each over a 60-minute interval.
    bronze_df = spark_session.createDataFrame(_price_bronze_rows())

    result = build_silver_energy_price_daily(bronze_df, IE_TZ).collect()

    assert len(result) == 1
    row = result[0].asDict()
    assert row["avg_day_ahead_price_eur_mwh"] == pytest.approx((-5.32 + 45.10) / 2)
    assert row["min_day_ahead_price_eur_mwh"] == pytest.approx(-5.32)
    assert row["max_day_ahead_price_eur_mwh"] == pytest.approx(45.10)


def test_build_silver_energy_price_daily_keeps_negative_prices(spark_session):
    bronze_df = spark_session.createDataFrame(_price_bronze_rows())

    result = build_silver_energy_price_daily(bronze_df, IE_TZ).collect()

    assert result[0].min_day_ahead_price_eur_mwh < 0


def test_build_silver_energy_price_daily_marks_short_timeline_as_partial(spark_session):
    bronze_df = spark_session.createDataFrame(_price_bronze_rows())

    row = build_silver_energy_price_daily(bronze_df, IE_TZ).collect()[0].asDict()

    assert row["covered_duration_hours"] == pytest.approx(2.0)
    assert row["completeness_status"] == "partial"
