"""Databricks-runtime tests for the Silver World Bank transformation.

These exercise the REAL PySpark transformation logic in
`src/transformations/silver_worldbank.py` against small literal Spark
DataFrames. Skipped locally (no PySpark installation) via
`pytest.importorskip`, same convention as `test_silver_weather_spark.py`.
"""
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

from src.ingestion.worldbank_bronze import build_bronze_records
from src.transformations.silver_worldbank import build_silver_worldbank_annual

pytestmark = pytest.mark.databricks

WB_RECORDS_2023 = [
    {
        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
        "country": {"id": "IE", "value": "Ireland"},
        "date": "2023",
        "value": 5311538,
    },
    {
        "indicator": {"id": "NY.GDP.MKTP.KD", "value": "GDP (constant 2015 US$)"},
        "country": {"id": "IE", "value": "Ireland"},
        "date": "2023",
        "value": 496851698334.418,
    },
    {
        "indicator": {"id": "NY.GDP.PCAP.KD", "value": "GDP per capita (constant 2015 US$)"},
        "country": {"id": "IE", "value": "Ireland"},
        "date": "2023",
        "value": 93564.2,
    },
    {
        "indicator": {"id": "SP.URB.TOTL.IN.ZS", "value": "Urban population (% of total)"},
        "country": {"id": "IE", "value": "Ireland"},
        "date": "2023",
        "value": 64.227,
    },
]


def test_build_silver_worldbank_annual_pivots_one_row_per_country_year(spark_session):
    rows = build_bronze_records(WB_RECORDS_2023)
    bronze_df = spark_session.createDataFrame(rows)

    result = build_silver_worldbank_annual(bronze_df).collect()

    assert len(result) == 1
    row = result[0].asDict()
    assert row["country_code"] == "IE"
    assert row["year"] == 2023
    assert row["population"] == pytest.approx(5311538)
    assert row["gdp_constant_2015_usd"] == pytest.approx(496851698334.418)
    assert row["gdp_per_capita_constant_2015_usd"] == pytest.approx(93564.2)
    assert row["urban_population_pct"] == pytest.approx(64.227)
    assert row["country_name"] == "Ireland"
    assert row["source_system"] == "worldbank"


def test_build_silver_worldbank_annual_keeps_countries_and_years_separate(spark_session):
    de_2023 = [
        {**r, "country": {"id": "DE", "value": "Germany"}, "value": r["value"] * 10}
        for r in WB_RECORDS_2023
    ]
    ie_2022 = [{**r, "date": "2022", "value": r["value"] * 0.9} for r in WB_RECORDS_2023]
    rows = build_bronze_records(WB_RECORDS_2023) + build_bronze_records(de_2023) + build_bronze_records(ie_2022)
    bronze_df = spark_session.createDataFrame(rows)

    result = build_silver_worldbank_annual(bronze_df).collect()

    keys = {(row.country_code, row.year) for row in result}
    assert keys == {("IE", 2023), ("DE", 2023), ("IE", 2022)}


def test_build_silver_worldbank_annual_outer_joins_missing_indicators_as_null(spark_session):
    # Real case: an indicator can be missing for a country/year (e.g. not
    # yet published) while others already have data — the row must
    # still appear, with that one column null, not be dropped entirely.
    partial_records = [r for r in WB_RECORDS_2023 if r["indicator"]["id"] != "SP.URB.TOTL.IN.ZS"]
    rows = build_bronze_records(partial_records)
    bronze_df = spark_session.createDataFrame(rows)

    result = build_silver_worldbank_annual(bronze_df).collect()

    assert len(result) == 1
    row = result[0].asDict()
    assert row["population"] == pytest.approx(5311538)
    assert row["urban_population_pct"] is None


def test_build_silver_worldbank_annual_dedupes_reruns_to_latest_ingestion(spark_session):
    stale = dict(WB_RECORDS_2023[0])
    rows_stale = build_bronze_records([stale], ingestion_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc))
    fresh = dict(WB_RECORDS_2023[0], value=6000000)
    rows_fresh = build_bronze_records([fresh], ingestion_timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc))
    bronze_df = spark_session.createDataFrame(rows_stale + rows_fresh)

    result = build_silver_worldbank_annual(bronze_df).collect()

    assert len(result) == 1
    assert result[0].population == pytest.approx(6000000)  # not the stale 5311538
