"""Batch upsert publishing for the five Gold serving tables (Spec 005).

Row validation and upsert-SQL construction are plain Python business
logic and are exercised for real in tests. The database connection is
the external system: every function here takes a `connection` object
(a real psycopg connection in production, a test double in tests) rather
than opening one itself, so this module never touches a real database.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone as dt_timezone
from typing import Dict, List, Optional, Sequence

# Logical key per table (Spec 005 "Serving Tables" / dbt Spec 004 grains).
TABLE_KEYS: Dict[str, Sequence[str]] = {
    "dim_country": ("country_key",),
    "dim_date": ("date_key",),
    "fact_energy_daily": ("country_key", "date_key"),
    "fact_weather_daily": ("country_key", "date_key"),
    "fact_generation_mix_daily": ("country_key", "date_key", "production_type"),
}

# Dimensions before facts (Spec 005 "Dimensions must be loaded before
# dependent facts.").
DEFAULT_TABLE_ORDER: Sequence[str] = (
    "dim_country",
    "dim_date",
    "fact_energy_daily",
    "fact_weather_daily",
    "fact_generation_mix_daily",
)


class PublishValidationError(ValueError):
    """Raised when rows fail pre-publish validation (Spec 005 "Validation")."""


@dataclass
class PublishResult:
    started_at: str
    ended_at: Optional[str] = None
    tables_processed: List[str] = field(default_factory=list)
    rows_written: Dict[str, int] = field(default_factory=dict)
    failed_tables: List[str] = field(default_factory=list)
    errors: Dict[str, str] = field(default_factory=dict)

    @property
    def all_succeeded(self) -> bool:
        return not self.failed_tables


def validate_rows(table: str, rows: List[dict]) -> None:
    """Pre-publish validation: required keys present, logical keys unique in-batch.

    Spec 005 "Before publication, validate: required keys are present,
    logical keys are unique...".
    """
    if table not in TABLE_KEYS:
        raise PublishValidationError(f"Unknown serving table: {table}")

    key_cols = TABLE_KEYS[table]
    seen = set()
    for row in rows:
        missing = [c for c in key_cols if row.get(c) is None]
        if missing:
            raise PublishValidationError(
                f"{table}: row is missing required key column(s) {missing}: {row}"
            )
        key = tuple(row[c] for c in key_cols)
        if key in seen:
            raise PublishValidationError(f"{table}: duplicate logical key {key} within the batch")
        seen.add(key)


def build_upsert_sql(table: str, columns: Sequence[str]) -> str:
    """Build an INSERT ... ON CONFLICT (...) DO UPDATE statement.

    This is the idempotency mechanism (Spec 005 "Upsert Strategy"):
    record does not exist -> insert; record exists -> update; never a
    duplicate logical fact.
    """
    if table not in TABLE_KEYS:
        raise PublishValidationError(f"Unknown serving table: {table}")

    key_cols = TABLE_KEYS[table]
    placeholders = ", ".join(f"%({c})s" for c in columns)
    update_cols = [c for c in columns if c not in key_cols]

    if update_cols:
        update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        conflict_clause = f"DO UPDATE SET {update_clause}"
    else:
        conflict_clause = "DO NOTHING"

    return (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT ({', '.join(key_cols)}) {conflict_clause}"
    )


def publish_table(connection, table: str, rows: List[dict]) -> int:
    """Upsert one batch of rows into one serving table inside a transaction.

    Uses a single batched `executemany` (Spec 005 "Use batch-oriented
    writes... avoid opening one database connection per individual
    record.") rather than row-by-row execution.
    """
    if not rows:
        return 0

    validate_rows(table, rows)
    columns = list(rows[0].keys())
    sql = build_upsert_sql(table, columns)

    with connection.cursor() as cursor:
        cursor.executemany(sql, rows)
    connection.commit()
    return len(rows)


def publish(
    connection,
    tables: Dict[str, List[dict]],
    *,
    table_order: Sequence[str] = DEFAULT_TABLE_ORDER,
) -> PublishResult:
    """Publish multiple tables in dependency order (dimensions before facts).

    Each table is committed in its own transaction. One table's failure
    is rolled back, recorded, and logged, but does not stop the others
    from publishing (Spec 005 "the failure must be visible... the
    serving load must be safely rerunnable").
    """
    result = PublishResult(started_at=datetime.now(dt_timezone.utc).isoformat())

    for table in table_order:
        rows = tables.get(table)
        if not rows:
            continue

        result.tables_processed.append(table)
        try:
            written = publish_table(connection, table, rows)
            result.rows_written[table] = written
        except Exception as exc:  # noqa: BLE001 - a per-table failure must stay visible
            connection.rollback()
            result.failed_tables.append(table)
            result.errors[table] = str(exc)

    result.ended_at = datetime.now(dt_timezone.utc).isoformat()
    return result
