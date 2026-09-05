"""Databricks-runtime tests for deterministic Bronze schema handling.

See `test_silver_weather_spark.py` for why these are skipped locally
(`pytest.importorskip("pyspark")`) and only ever run for real on a
machine/cluster with PySpark installed.
"""
import pytest

pytest.importorskip("pyspark")

from src.ingestion.delta_schema import (
    DuplicateKeyError,
    ENTSOE_BRONZE_KEY_COLS,
    SchemaMismatchError,
    assert_unique_keys,
    ensure_schema_compatible,
    entsoe_bronze_schema,
    open_meteo_bronze_schema,
)

pytestmark = pytest.mark.databricks


def test_ensure_schema_compatible_accepts_identical_schema():
    schema = entsoe_bronze_schema()
    ensure_schema_compatible(schema, schema, "bronze_entsoe_energy")  # must not raise


def test_ensure_schema_compatible_rejects_missing_column():
    from pyspark.sql.types import StructField, StructType

    # A table written before `business_type` existed.
    stale = StructType([f for f in entsoe_bronze_schema().fields if f.name != "business_type"])

    with pytest.raises(SchemaMismatchError) as exc_info:
        ensure_schema_compatible(stale, entsoe_bronze_schema(), "bronze_entsoe_energy")

    assert "business_type" in str(exc_info.value)


def test_ensure_schema_compatible_rejects_type_mismatch():
    from pyspark.sql.types import StringType, StructField, StructType

    expected = entsoe_bronze_schema()
    wrong_type = StructType(
        [StructField(f.name, StringType(), f.nullable) if f.name == "value" else f for f in expected.fields]
    )

    with pytest.raises(SchemaMismatchError) as exc_info:
        ensure_schema_compatible(wrong_type, expected, "bronze_entsoe_energy")

    assert "value" in str(exc_info.value)


def test_ensure_schema_compatible_ignores_nullability_only_differences():
    from pyspark.sql.types import StructField, StructType

    expected = open_meteo_bronze_schema()
    # Same names/types, every field forced non-nullable — not a real
    # incompatibility this guard should flag.
    flipped_nullability = StructType([StructField(f.name, f.dataType, False) for f in expected.fields])

    ensure_schema_compatible(flipped_nullability, expected, "bronze_open_meteo_weather")  # must not raise


def _entsoe_row(**overrides):
    row = {
        "country_code": "IE", "domain": "10Y1001A1001A59C", "dataset_type": "load",
        "source_timestamp": "2024-01-01T00:00:00", "source_resolution": "PT60M", "value": 100.0,
        "unit": "MAW", "production_type_raw": None, "business_type": "A04", "currency": None,
        "source_document_mrid": "doc-1", "requested_start_date": "2024-01-01",
        "requested_end_date": "2024-01-01", "source_system": "entsoe",
        "ingestion_timestamp": "2024-01-01T00:00:00+00:00",
    }
    row.update(overrides)
    return row


def test_assert_unique_keys_accepts_a_deduplicated_dataframe(spark_session):
    df = spark_session.createDataFrame([_entsoe_row(), _entsoe_row(source_timestamp="2024-01-01T01:00:00")], schema=entsoe_bronze_schema())

    assert_unique_keys(df, ENTSOE_BRONZE_KEY_COLS, "bronze_entsoe_energy")  # must not raise


def test_assert_unique_keys_rejects_duplicate_merge_keys(spark_session):
    # Two rows identical on every ENTSOE_BRONZE_KEY_COLS column — exactly
    # what would otherwise hit Delta's
    # DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE at .execute().
    df = spark_session.createDataFrame(
        [_entsoe_row(ingestion_timestamp="2024-01-01T00:00:00+00:00"),
         _entsoe_row(ingestion_timestamp="2024-01-01T00:05:00+00:00")],
        schema=entsoe_bronze_schema(),
    )

    with pytest.raises(DuplicateKeyError) as exc_info:
        assert_unique_keys(df, ENTSOE_BRONZE_KEY_COLS, "bronze_entsoe_energy")

    assert "bronze_entsoe_energy" in str(exc_info.value)
