"""Databricks-runtime tests for deterministic Bronze schema handling.

See `test_silver_weather_spark.py` for why these are skipped locally
(`pytest.importorskip("pyspark")`) and only ever run for real on a
machine/cluster with PySpark installed.
"""
import pytest

pytest.importorskip("pyspark")

from src.ingestion.delta_schema import (
    DuplicateKeyError,
    ENTSOE_BRONZE_FIELDS,
    ENTSOE_BRONZE_KEY_COLS,
    OPEN_METEO_BRONZE_FIELDS,
    SchemaMismatchError,
    WORLDBANK_BRONZE_FIELDS,
    assert_unique_keys,
    ensure_schema_compatible,
    entsoe_bronze_schema,
    open_meteo_bronze_schema,
    worldbank_bronze_schema,
)

pytestmark = pytest.mark.databricks

_SCHEMA_CASES = (
    (entsoe_bronze_schema, ENTSOE_BRONZE_FIELDS),
    (open_meteo_bronze_schema, OPEN_METEO_BRONZE_FIELDS),
    (worldbank_bronze_schema, WORLDBANK_BRONZE_FIELDS),
)


@pytest.mark.parametrize("schema_fn,fields", _SCHEMA_CASES)
def test_spark_schema_matches_the_field_definitions(schema_fn, fields):
    """The Spark and Arrow schemas are derived from the same field
    definitions, so a drift between the definitions and what Spark
    actually produces would let a Parquet file be written that the
    Bronze merge then rejects on type."""
    schema = schema_fn()

    assert [f.name for f in schema.fields] == [name for name, _, _ in fields]
    assert [f.nullable for f in schema.fields] == [nullable for _, _, nullable in fields]


@pytest.mark.parametrize("schema_fn,fields", _SCHEMA_CASES)
def test_spark_types_match_the_declared_logical_types(schema_fn, fields):
    expected = {
        "string": "StringType", "double": "DoubleType",
        "int": "IntegerType", "long": "LongType",
    }
    schema = schema_fn()

    for field, (_, type_name, _) in zip(schema.fields, fields):
        assert type(field.dataType).__name__ == expected[type_name]


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
