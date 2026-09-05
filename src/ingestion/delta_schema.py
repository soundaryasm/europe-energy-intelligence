"""Deterministic, application-owned Bronze table schemas (Spec 002/001).

Databricks serverless (Standard environment v5) does not support Delta's
automatic MERGE schema evolution: neither
`spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")`
nor `DeltaTable.merge(...).withSchemaEvolution()` work on that environment
version (both raise/are unavailable there). Rather than route around that
by switching compute types, normal ingestion in this project does not rely
on automatic schema evolution at all, on any environment:

- Every Bronze table's schema is an explicit `StructType` defined in this
  module, not inferred from whatever keys happen to be present in a given
  run's records.
- A first-time write creates the table with exactly that schema.
- A write against an existing table validates the table's actual schema
  matches before merging; a mismatch fails loudly (`SchemaMismatchError`)
  instead of silently altering a production table.
- Adding/removing a column (e.g. `business_type`) is a deliberate,
  one-time migration applied out-of-band (see `docs/migrations/`), not
  something a daily/backfill/reprocess run does on the fly.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:  # pragma: no cover - typing only, no runtime import
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class SchemaMismatchError(Exception):
    """An existing Delta table's schema does not match this code's expected schema.

    Raised instead of silently evolving the table. Resolve by applying an
    explicit migration (see `docs/migrations/`) that brings the table's
    schema in line with the current `*_bronze_schema()` definition, then
    rerun.
    """


def entsoe_bronze_schema() -> "StructType":
    """Application-owned schema for `bronze_entsoe_energy` (Spec 002).

    Mirrors exactly what `entsoe_bronze.build_bronze_records` emits.
    `business_type` was added after this table was first created in some
    environments — see `docs/migrations/001_entsoe_bronze_add_business_type.sql`.
    """
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    return StructType(
        [
            StructField("country_code", StringType(), False),
            StructField("domain", StringType(), False),
            StructField("dataset_type", StringType(), False),
            StructField("source_timestamp", StringType(), False),
            StructField("source_resolution", StringType(), False),
            StructField("value", DoubleType(), False),
            StructField("unit", StringType(), True),
            StructField("production_type_raw", StringType(), True),
            StructField("business_type", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("source_document_mrid", StringType(), True),
            StructField("requested_start_date", StringType(), False),
            StructField("requested_end_date", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("ingestion_timestamp", StringType(), False),
        ]
    )


def open_meteo_bronze_schema() -> "StructType":
    """Application-owned schema for `bronze_open_meteo_weather` (Spec 001).

    Mirrors exactly what `open_meteo_bronze.build_bronze_records` emits.
    """
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("country_code", StringType(), False),
            StructField("country_name", StringType(), False),
            StructField("reference_location", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("timezone", StringType(), False),
            StructField("returned_latitude", DoubleType(), True),
            StructField("returned_longitude", DoubleType(), True),
            StructField("returned_timezone", StringType(), True),
            StructField("utc_offset_seconds", IntegerType(), True),
            StructField("observation_date", StringType(), False),
            StructField("source_variable", StringType(), False),
            StructField("source_value", DoubleType(), True),
            StructField("source_unit", StringType(), True),
            StructField("source_endpoint", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("ingestion_timestamp", StringType(), False),
        ]
    )


def _field_types(schema: "StructType") -> Dict[str, object]:
    return {field.name: field.dataType for field in schema.fields}


def ensure_schema_compatible(existing_schema: "StructType", expected_schema: "StructType", table_name: str) -> None:
    """Raise `SchemaMismatchError` unless `existing_schema` has exactly the
    columns and types this code expects.

    Nullability is deliberately not compared: a column Delta reports as
    non-nullable vs. nullable is not the kind of drift this guard exists
    to catch, and comparing it produces false positives unrelated to real
    incompatibility.
    """
    existing = _field_types(existing_schema)
    expected = _field_types(expected_schema)
    if existing == expected:
        return

    missing = sorted(set(expected) - set(existing))
    extra = sorted(set(existing) - set(expected))
    type_mismatches = sorted(
        f"{name} (table={existing[name]}, expected={expected[name]})"
        for name in set(existing) & set(expected)
        if existing[name] != expected[name]
    )
    raise SchemaMismatchError(
        f"'{table_name}' schema does not match the application-owned schema in code. "
        f"missing_columns={missing} extra_columns={extra} type_mismatches={type_mismatches}. "
        "Resolve with an explicit migration (see docs/migrations/), not automatic MERGE "
        "schema evolution."
    )


def write_with_deterministic_schema(
    spark,
    df: "DataFrame",
    table_name: str,
    expected_schema: "StructType",
    merge_condition: str,
) -> int:
    """Create-or-merge `df` into `table_name` without relying on Delta's
    automatic MERGE schema evolution.

    `df` must already have been built with `expected_schema` (e.g. via
    `spark.createDataFrame(records, schema=expected_schema)`), and already
    deduplicated on the merge key.
    """
    from delta.tables import DeltaTable

    if spark.catalog.tableExists(table_name):
        ensure_schema_compatible(spark.table(table_name).schema, expected_schema, table_name)
        target = DeltaTable.forName(spark, table_name)
        (
            target.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    return df.count()
