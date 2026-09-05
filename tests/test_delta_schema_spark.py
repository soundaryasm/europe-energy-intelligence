"""Databricks-runtime tests for deterministic Bronze schema handling.

See `test_silver_weather_spark.py` for why these are skipped locally
(`pytest.importorskip("pyspark")`) and only ever run for real on a
machine/cluster with PySpark installed.
"""
import pytest

pytest.importorskip("pyspark")

from src.ingestion.delta_schema import (
    SchemaMismatchError,
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
