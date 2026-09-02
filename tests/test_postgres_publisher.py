"""Tests for PostgreSQL upsert publishing (Spec 005).

The database connection is the external system and is mocked throughout
— no real database is touched. Row validation and upsert SQL
construction are real business logic and are exercised directly.
"""
from unittest.mock import MagicMock

import pytest

from src.serving.postgres_publisher import (
    PublishValidationError,
    build_upsert_sql,
    publish,
    publish_table,
    validate_rows,
)


def _mock_connection():
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection, cursor


# --- validate_rows --------------------------------------------------------

def test_validate_rows_accepts_well_formed_batch():
    validate_rows("dim_country", [{"country_key": "IE"}, {"country_key": "DE"}])


def test_validate_rows_rejects_missing_key_column():
    with pytest.raises(PublishValidationError):
        validate_rows("fact_energy_daily", [{"country_key": "IE"}])  # missing date_key


def test_validate_rows_rejects_duplicate_logical_key_in_batch():
    rows = [
        {"country_key": "IE", "date_key": 20240101, "daily_demand_mwh": 100.0},
        {"country_key": "IE", "date_key": 20240101, "daily_demand_mwh": 999.0},
    ]
    with pytest.raises(PublishValidationError):
        validate_rows("fact_energy_daily", rows)


def test_validate_rows_rejects_unknown_table():
    with pytest.raises(PublishValidationError):
        validate_rows("not_a_real_table", [{"id": 1}])


def test_validate_rows_generation_mix_key_includes_production_type():
    rows = [
        {"country_key": "IE", "date_key": 20240101, "production_type": "wind"},
        {"country_key": "IE", "date_key": 20240101, "production_type": "solar"},
    ]
    validate_rows("fact_generation_mix_daily", rows)  # different production_type -> not a duplicate


# --- build_upsert_sql -------------------------------------------------------

def test_build_upsert_sql_dim_country_uses_country_key_conflict_target():
    sql = build_upsert_sql("dim_country", ["country_key", "country_code", "country_name"])
    assert "INSERT INTO dim_country" in sql
    assert "ON CONFLICT (country_key)" in sql
    assert "DO UPDATE SET country_code = EXCLUDED.country_code" in sql
    assert "country_key = EXCLUDED.country_key" not in sql  # never update the key itself


def test_build_upsert_sql_fact_generation_mix_uses_composite_conflict_target():
    sql = build_upsert_sql(
        "fact_generation_mix_daily",
        ["country_key", "date_key", "production_type", "generation_mwh"],
    )
    assert "ON CONFLICT (country_key, date_key, production_type)" in sql
    assert "generation_mwh = EXCLUDED.generation_mwh" in sql


def test_build_upsert_sql_with_only_key_columns_does_nothing_on_conflict():
    sql = build_upsert_sql("dim_country", ["country_key"])
    assert "DO NOTHING" in sql


# --- publish_table -----------------------------------------------------------

def test_publish_table_executes_one_batched_upsert_and_commits():
    connection, cursor = _mock_connection()
    rows = [{"country_key": "IE", "country_code": "IE", "country_name": "Ireland",
              "reference_location": "Dublin", "timezone": "Europe/Dublin", "entsoe_domain": "x"}]

    written = publish_table(connection, "dim_country", rows)

    assert written == 1
    cursor.executemany.assert_called_once()
    sql_arg, rows_arg = cursor.executemany.call_args[0]
    assert "ON CONFLICT (country_key)" in sql_arg
    assert rows_arg == rows
    connection.commit.assert_called_once()


def test_publish_table_with_empty_rows_touches_nothing():
    connection, cursor = _mock_connection()

    written = publish_table(connection, "dim_country", [])

    assert written == 0
    cursor.executemany.assert_not_called()
    connection.commit.assert_not_called()


def test_publish_table_propagates_validation_errors_without_writing():
    connection, cursor = _mock_connection()

    with pytest.raises(PublishValidationError):
        publish_table(connection, "fact_energy_daily", [{"country_key": "IE"}])  # missing date_key

    cursor.executemany.assert_not_called()


# --- publish (multi-table orchestration) --------------------------------------

def test_publish_writes_tables_in_dimension_before_fact_order():
    connection, cursor = _mock_connection()
    tables = {
        "fact_energy_daily": [{"country_key": "IE", "date_key": 20240101, "daily_demand_mwh": 100.0}],
        "dim_country": [{"country_key": "IE", "country_code": "IE", "country_name": "Ireland",
                          "reference_location": "Dublin", "timezone": "Europe/Dublin", "entsoe_domain": "x"}],
    }

    result = publish(connection, tables)

    assert result.tables_processed == ["dim_country", "fact_energy_daily"]
    assert result.all_succeeded is True
    assert result.rows_written == {"dim_country": 1, "fact_energy_daily": 1}


def test_publish_isolates_one_table_failure_and_rolls_it_back():
    connection, cursor = _mock_connection()
    tables = {
        "dim_country": [{"country_key": "IE", "country_code": "IE", "country_name": "Ireland",
                          "reference_location": "Dublin", "timezone": "Europe/Dublin", "entsoe_domain": "x"}],
        "fact_energy_daily": [{"country_key": "IE"}],  # invalid: missing date_key
    }

    result = publish(connection, tables)

    assert "fact_energy_daily" in result.failed_tables
    assert "dim_country" not in result.failed_tables
    assert result.all_succeeded is False
    connection.rollback.assert_called_once()


def test_publish_skips_tables_with_no_rows():
    connection, cursor = _mock_connection()

    result = publish(connection, {"dim_country": []})

    assert result.tables_processed == []
    cursor.executemany.assert_not_called()
