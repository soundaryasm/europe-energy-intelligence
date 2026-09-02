"""Tests for the PostgreSQL serving-layer DDL (Spec 005)."""
from src.serving.postgres_schema import ALL_TABLE_DDL, INDEX_DDL

EXPECTED_TABLES = {
    "dim_country",
    "dim_date",
    "fact_energy_daily",
    "fact_weather_daily",
    "fact_generation_mix_daily",
}


def test_all_table_ddl_covers_the_five_approved_serving_tables():
    joined = " ".join(ALL_TABLE_DDL)
    for table in EXPECTED_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table} " in joined


def test_dimension_ddl_precedes_fact_ddl_so_foreign_keys_resolve():
    ddl_text = "\n".join(ALL_TABLE_DDL)
    assert ddl_text.index("CREATE TABLE IF NOT EXISTS dim_country") < ddl_text.index(
        "CREATE TABLE IF NOT EXISTS fact_energy_daily"
    )
    assert ddl_text.index("CREATE TABLE IF NOT EXISTS dim_date") < ddl_text.index(
        "CREATE TABLE IF NOT EXISTS fact_weather_daily"
    )


def test_fact_tables_reference_both_dimensions():
    ddl_text = "\n".join(ALL_TABLE_DDL)
    for fact in ("fact_energy_daily", "fact_weather_daily", "fact_generation_mix_daily"):
        fact_ddl = next(ddl for ddl in ALL_TABLE_DDL if f"EXISTS {fact} " in ddl)
        assert "REFERENCES dim_country(country_key)" in fact_ddl
        assert "REFERENCES dim_date(date_key)" in fact_ddl


def test_index_ddl_uses_if_not_exists_and_is_idempotent_to_rerun():
    assert all("CREATE INDEX IF NOT EXISTS" in stmt for stmt in INDEX_DDL)
