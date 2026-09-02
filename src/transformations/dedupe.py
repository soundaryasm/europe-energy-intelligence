"""Shared Bronze duplicate-handling for Silver builders (Spec 003).

Databricks-only: operates on a PySpark DataFrame. Keeps the most
recently ingested row per business key, which is Spec 003's documented
duplicate-resolution rule ("prefer the most recently ingested valid
source record").
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:  # pragma: no cover - typing only, no runtime import
    from pyspark.sql import DataFrame


def dedupe_latest(df: "DataFrame", key_cols: Sequence[str], order_col: str = "ingestion_timestamp") -> "DataFrame":
    """Keep exactly one row per `key_cols`, preferring the highest `order_col`."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
