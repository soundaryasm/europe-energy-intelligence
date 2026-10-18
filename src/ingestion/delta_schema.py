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
  something a daily/reprocess run or the historical load does on the fly.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Sequence

if TYPE_CHECKING:  # pragma: no cover - typing only, no runtime import
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

# The merge/business key for each Bronze table, defined once here and
# reused by both the pre-merge dedupe (`dedupe_latest`) and the MERGE
# condition itself (`write_with_deterministic_schema`) — a single source
# of truth so the two can never drift apart (drift here is exactly what
# would make the post-dedupe uniqueness check in
# `write_with_deterministic_schema` meaningless).
# `source_resolution` is part of the key, not merely a payload column.
# Since the day-ahead market moved to a 15-minute market time unit
# (SDAC, 30 September 2025), ENTSO-E publishes the same instant at more
# than one resolution, with genuinely different values: an hourly price
# and the price for the first quarter of that hour are two distinct
# observations, not two copies of one. Without resolution in the key
# they collide on the MERGE and one is silently discarded, with nothing
# to break the tie — every row in a batch shares an
# `ingestion_timestamp`. Both are kept here, and the choice of which to
# use is made once in Silver (`entsoe_silver_common.with_preferred_resolution`)
# rather than left to whichever row happened to win a merge.
ENTSOE_BRONZE_KEY_COLS = (
    "country_code", "dataset_type", "source_timestamp", "source_resolution",
    "production_type_raw", "business_type",
)
OPEN_METEO_BRONZE_KEY_COLS = ("country_code", "source_variable", "observation_date")
WORLDBANK_BRONZE_KEY_COLS = ("country_code", "indicator_code", "year")


class SchemaMismatchError(Exception):
    """An existing Delta table's schema does not match this code's expected schema.

    Raised instead of silently evolving the table. Resolve by applying an
    explicit migration (see `docs/migrations/`) that brings the table's
    schema in line with the current `*_bronze_schema()` definition, then
    rerun.
    """


class DuplicateKeyError(Exception):
    """The source DataFrame has more than one row for the same merge key.

    Delta's MERGE raises an opaque `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`
    if this reaches `.execute()`. Raised earlier, with the offending keys,
    because `dedupe_latest` is expected to have already resolved this —
    seeing it here means `dedupe_latest`'s `key_cols` and the merge key
    used for this write have drifted apart from each other.
    """


# Bronze field definitions, declared without importing PySpark.
#
# Each Bronze table's columns are defined once, here, as (name, type,
# nullable) triples using the logical type names in `_LOGICAL_TYPES`.
# Both representations the project needs are derived from these same
# triples: the Spark `StructType` used for the Delta write
# (`*_bronze_schema()`) and the Arrow schema used for the Parquet write
# (`src.blob.writer.arrow_schema`). Declaring the columns in a
# PySpark-free form is what lets the Parquet path run where PySpark is
# not installed, while keeping one source of truth for the column set,
# ordering and nullability — the same reasoning that put the merge keys
# in this module rather than beside each writer.
ENTSOE_BRONZE_FIELDS = (
    ("country_code", "string", False),
    ("domain", "string", False),
    ("dataset_type", "string", False),
    ("source_timestamp", "string", False),
    ("source_resolution", "string", False),
    ("value", "double", False),
    ("unit", "string", True),
    ("production_type_raw", "string", True),
    ("business_type", "string", True),
    ("currency", "string", True),
    ("source_document_mrid", "string", True),
    ("requested_start_date", "string", False),
    ("requested_end_date", "string", False),
    ("source_system", "string", False),
    ("ingestion_timestamp", "string", False),
)

OPEN_METEO_BRONZE_FIELDS = (
    ("country_code", "string", False),
    ("country_name", "string", False),
    ("reference_location", "string", False),
    ("latitude", "double", False),
    ("longitude", "double", False),
    ("timezone", "string", False),
    ("returned_latitude", "double", True),
    ("returned_longitude", "double", True),
    ("returned_timezone", "string", True),
    # `long`, not `int`, to match what every real table already holds —
    # confirmed via `DESCRIBE TABLE bronze_open_meteo_weather` showing
    # `bigint`. Not a value-range requirement.
    ("utc_offset_seconds", "long", True),
    ("observation_date", "string", False),
    ("source_variable", "string", False),
    ("source_value", "double", True),
    ("source_unit", "string", True),
    ("source_endpoint", "string", False),
    ("source_system", "string", False),
    ("ingestion_timestamp", "string", False),
)

WORLDBANK_BRONZE_FIELDS = (
    ("country_code", "string", False),
    ("country_name", "string", False),
    ("indicator_code", "string", False),
    ("indicator_name", "string", False),
    ("year", "int", False),
    # Nullable — World Bank can list a country/indicator/year with a
    # `null` value.
    ("value", "double", True),
    ("source_system", "string", False),
    ("ingestion_timestamp", "string", False),
)


def _struct_type(fields: Sequence[tuple]) -> "StructType":
    """Build a Spark `StructType` from `(name, type, nullable)` triples."""
    from pyspark.sql.types import (
        DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    )

    spark_types = {
        "string": StringType, "double": DoubleType,
        "int": IntegerType, "long": LongType,
    }
    return StructType(
        [
            StructField(name, spark_types[type_name](), nullable)
            for name, type_name, nullable in fields
        ]
    )


def entsoe_bronze_schema() -> "StructType":
    """Application-owned schema for `bronze_entsoe_energy` (Spec 002).

    Mirrors exactly what `entsoe_bronze.build_bronze_records` emits.
    `business_type` was added after this table was first created in some
    environments — see `docs/migrations/001_entsoe_bronze_add_business_type.sql`.
    """
    return _struct_type(ENTSOE_BRONZE_FIELDS)


def open_meteo_bronze_schema() -> "StructType":
    """Application-owned schema for `bronze_open_meteo_weather` (Spec 001).

    Mirrors exactly what `open_meteo_bronze.build_bronze_records` emits.
    """
    return _struct_type(OPEN_METEO_BRONZE_FIELDS)


def worldbank_bronze_schema() -> "StructType":
    """Application-owned schema for `bronze_worldbank_indicators`.

    Mirrors exactly what `worldbank_bronze.build_bronze_records` emits.
    """
    return _struct_type(WORLDBANK_BRONZE_FIELDS)


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


def assert_unique_keys(df: "DataFrame", key_cols: Sequence[str], table_name: str, sample_limit: int = 5) -> None:
    """Raise `DuplicateKeyError` if `df` has more than one row for the same
    `key_cols` combination.

    Meant to run on the DataFrame actually about to be merged/written,
    after `dedupe_latest` — a safety net, not the primary dedupe
    mechanism. If this ever fires, `dedupe_latest`'s `key_cols` no longer
    matches the merge key for this table.
    """
    from pyspark.sql import functions as F

    duplicates = df.groupBy(*key_cols).count().filter(F.col("count") > 1).limit(sample_limit)
    sample = [row.asDict() for row in duplicates.collect()]
    if sample:
        raise DuplicateKeyError(
            f"'{table_name}': source DataFrame has multiple rows for the same merge key "
            f"{list(key_cols)} after dedupe_latest — this should be impossible. "
            f"Sample duplicate key(s): {sample}"
        )


def _merge_condition(key_cols: Sequence[str]) -> str:
    # coalesce(..., '') so a nullable key column (e.g. `production_type_raw`,
    # `business_type` for load/price rows) still matches NULL-to-NULL
    # instead of Delta's SQL NULL <> NULL never being true.
    return " AND ".join(f"coalesce(t.{col}, '') = coalesce(s.{col}, '')" for col in key_cols)


def write_with_deterministic_schema(
    spark,
    df: "DataFrame",
    table_name: str,
    expected_schema: "StructType",
    key_cols: Sequence[str],
) -> int:
    """Create-or-merge `df` into `table_name` without relying on Delta's
    automatic MERGE schema evolution.

    `df` must already have been built with `expected_schema` (e.g. via
    `spark.createDataFrame(records, schema=expected_schema)`), and already
    deduplicated on `key_cols` (e.g. via `dedupe_latest`). `key_cols` is
    the single definition of this table's merge/business key — reused
    here both to build the MERGE condition and to verify uniqueness,
    rather than a MERGE condition string maintained separately from the
    dedupe key.
    """
    from delta.tables import DeltaTable

    assert_unique_keys(df, key_cols, table_name)

    if spark.catalog.tableExists(table_name):
        ensure_schema_compatible(spark.table(table_name).schema, expected_schema, table_name)
        target = DeltaTable.forName(spark, table_name)
        (
            target.alias("t")
            .merge(df.alias("s"), _merge_condition(key_cols))
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    return df.count()
