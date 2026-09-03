"""Databricks-runtime tests for the Silver generation-mix transformation (Spec 003).

See `test_silver_weather_spark.py` for why these are skipped locally
(`pytest.importorskip("pyspark")`) and only ever run for real on a
machine/cluster with PySpark installed.
"""
from datetime import date

import pytest

pytest.importorskip("pyspark")

from src.config.entsoe import EntsoeCountryDomain
from src.ingestion.entsoe_bronze import build_bronze_records
from src.ingestion.entsoe_datasets import GENERATION
from src.ingestion.entsoe_xml import parse_time_series
from src.transformations.silver_generation_mix import build_silver_generation_mix_daily
from tests.fixtures_entsoe_xml import GENERATION_XML

pytestmark = pytest.mark.databricks

IE_DOMAIN = EntsoeCountryDomain("IE", "10Y1001A1001A59C", validated=False)
IE_TZ = {"IE": "Europe/Dublin"}


def _generation_bronze_rows(xml=GENERATION_XML):
    parsed = parse_time_series(xml, GENERATION)
    return build_bronze_records(parsed, IE_DOMAIN, GENERATION, date(2024, 1, 1), date(2024, 1, 1))


def test_build_silver_generation_mix_daily_normalizes_production_types(spark_session):
    # GENERATION_XML: B19 (Wind Onshore) = 120 MW, B16 (Solar) = 50 MW, both PT60M.
    bronze_df = spark_session.createDataFrame(_generation_bronze_rows())

    result = {row.normalized_production_type: row.asDict() for row in build_silver_generation_mix_daily(bronze_df, IE_TZ).collect()}

    assert set(result.keys()) == {"wind", "solar"}
    assert result["wind"]["generation_mwh"] == pytest.approx(120.0)
    assert result["wind"]["renewable_flag"] is True
    assert result["wind"]["production_type_raw_codes"] == ["B19"]
    assert result["solar"]["generation_mwh"] == pytest.approx(50.0)


def test_build_silver_generation_mix_daily_collapses_raw_types_into_one_category(spark_session):
    rows = _generation_bronze_rows()
    # Force both series onto the same normalized category (coal) via two
    # different raw ENTSO-E psrType codes, to verify they sum together
    # rather than staying as separate rows.
    rows[0]["production_type_raw"] = "B02"  # Fossil Brown coal/Lignite -> coal
    rows[1]["production_type_raw"] = "B05"  # Fossil Hard coal -> coal
    bronze_df = spark_session.createDataFrame(rows)

    result = build_silver_generation_mix_daily(bronze_df, IE_TZ).collect()

    assert len(result) == 1
    row = result[0].asDict()
    assert row["normalized_production_type"] == "coal"
    assert row["generation_mwh"] == pytest.approx(120.0 + 50.0)
    assert row["production_type_raw_codes"] == ["B02", "B05"]
    assert row["renewable_flag"] is False


def test_build_silver_generation_mix_daily_excludes_negative_generation(spark_session):
    rows = _generation_bronze_rows()
    rows[0]["value"] = -10.0
    bronze_df = spark_session.createDataFrame(rows)

    result = {row.normalized_production_type: row.asDict() for row in build_silver_generation_mix_daily(bronze_df, IE_TZ).collect()}

    assert "wind" not in result  # the only wind reading was negative and was dropped
    assert "solar" in result


def test_build_silver_generation_mix_daily_marks_short_timeline_as_partial(spark_session):
    # GENERATION_XML covers a single PT60M interval per production type —
    # 1 of the ~24 expected hours for 2024-01-01 in Europe/Dublin (Spec
    # 003 "Generation Completeness": evaluated per production-type series).
    bronze_df = spark_session.createDataFrame(_generation_bronze_rows())

    result = {row.normalized_production_type: row.asDict() for row in build_silver_generation_mix_daily(bronze_df, IE_TZ).collect()}

    assert result["wind"]["covered_duration_hours"] == pytest.approx(1.0)
    assert result["wind"]["completeness_status"] == "partial"
    assert result["solar"]["completeness_status"] == "partial"


def test_build_silver_generation_mix_daily_marks_full_timeline_as_complete(spark_session):
    rows = []
    for hour in range(24):
        row = dict(_generation_bronze_rows()[0])  # wind (B19) series
        row["source_timestamp"] = f"2024-01-01T{hour:02d}:00:00"
        row["value"] = 100.0
        rows.append(row)
    bronze_df = spark_session.createDataFrame(rows)

    result = build_silver_generation_mix_daily(bronze_df, IE_TZ).collect()

    assert len(result) == 1
    row = result[0].asDict()
    assert row["covered_duration_hours"] == pytest.approx(24.0)
    assert row["completeness_status"] == "complete"
    assert row["generation_mwh"] == pytest.approx(100.0 * 24)
