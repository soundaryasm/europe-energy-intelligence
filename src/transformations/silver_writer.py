"""Delta MERGE writer shared by the Silver builders (Spec 003 Idempotency).

Databricks-only: PySpark/Delta are imported lazily inside the function
body so this module stays importable without a PySpark installation.
Mirrors the same lazy-import, MERGE-on-business-key pattern used by the
Bronze ingestion writers in `src/ingestion/*_pipeline.py`.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame


def write_silver_table(spark, df: "DataFrame", table_name: str, key_cols: Sequence[str]) -> int:
    """Upsert `df` into a Silver Delta table, merging on `key_cols`.

    Reprocessing the same dates replaces the corresponding rows rather
    than appending duplicates (Spec 003 "Idempotency").
    """
    from delta.tables import DeltaTable

    if spark.catalog.tableExists(table_name):
        target = DeltaTable.forName(spark, table_name)
        condition = " AND ".join(f"t.{col} = s.{col}" for col in key_cols)
        (
            target.alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    return df.count()
