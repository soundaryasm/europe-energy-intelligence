"""Spark-backed verification tests (Spec 010).

These exist because the fake-based tests in `test_blob_verify.py`
cannot model SQL NULL semantics, and that is precisely where the
original defect lived: an equi-join on the merge key reported every row
with a NULL key column as absent from Bronze.

Skipped where pyspark is unavailable.
"""
import pytest

pytest.importorskip("pyspark")

from src.blob import verify


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]").appName("verify-tests").getOrCreate()
    )
    yield session
    session.stop()


KEY_COLS = (
    "country_code", "dataset_type", "source_timestamp", "source_resolution",
    "production_type_raw", "business_type",
)

# A load row: `production_type_raw` and `business_type` are NULL, as
# they are for every load and price row in Bronze.
LOAD_ROW = ("IE", "load", "2026-11-01T00:00:00Z", "PT30M", None, None)
GEN_ROW = ("IE", "generation", "2026-11-01T00:00:00Z", "PT30M", "B16", "A01")
ABSENT_ROW = ("IE", "load", "2026-11-02T00:00:00Z", "PT30M", None, None)


def _frame(spark, rows):
    return spark.createDataFrame(list(rows), list(KEY_COLS))


def _missing(spark, staged_rows, bronze_rows):
    """Run the module's own join condition over two key frames."""
    from pyspark.sql import functions as F

    staged = _frame(spark, staged_rows).alias("s")
    bronze = _frame(spark, bronze_rows).alias("b")
    condition = None
    for col in KEY_COLS:
        same = F.coalesce(F.col(f"s.{col}").cast("string"), F.lit("")) == F.coalesce(
            F.col(f"b.{col}").cast("string"), F.lit("")
        )
        condition = same if condition is None else (condition & same)
    return staged.join(bronze, on=condition, how="left_anti").count()


def test_a_null_key_column_matches_its_counterpart(spark):
    # The defect: an equi-join makes this report 1 missing row.
    assert _missing(spark, [LOAD_ROW], [LOAD_ROW]) == 0


def test_a_genuinely_absent_row_is_still_reported(spark):
    assert _missing(spark, [LOAD_ROW, ABSENT_ROW], [LOAD_ROW]) == 1


def test_rows_with_and_without_null_keys_verify_together(spark):
    assert _missing(spark, [LOAD_ROW, GEN_ROW], [GEN_ROW, LOAD_ROW]) == 0


def test_bronze_holding_extra_rows_is_not_a_failure(spark):
    assert _missing(spark, [LOAD_ROW], [LOAD_ROW, GEN_ROW, ABSENT_ROW]) == 0


def test_verify_source_reports_no_missing_rows_for_a_complete_load(spark, tmp_path):
    # End to end through the real function, nullable keys included.
    root = tmp_path / "vol"
    (root / "entsoe").mkdir(parents=True)
    _frame(spark, [LOAD_ROW, GEN_ROW]).write.parquet(str(root / "entsoe" / "2026"))

    table = "verify_spark_bronze_entsoe_energy"
    _frame(spark, [LOAD_ROW, GEN_ROW]).write.mode("overwrite").saveAsTable(table)
    try:
        result = verify.verify_source(
            spark, "entsoe", str(root), table_prefix="verify_spark_",
        )
        assert result.missing_rows == 0
        assert result.ok
    finally:
        spark.sql(f"drop table if exists {table}")
