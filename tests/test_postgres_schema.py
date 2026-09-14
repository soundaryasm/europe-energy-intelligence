"""Tests for the PostgreSQL serving-layer DDL (Spec 005)."""
from src.serving.postgres_schema import ALL_TABLE_DDL, FACT_ENERGY_DAILY_DDL, INDEX_DDL

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


def test_fact_energy_daily_has_worldbank_columns_in_the_create_and_an_idempotent_alter():
    # CREATE TABLE IF NOT EXISTS is a no-op on a table that already
    # exists from before these columns were added, so a matching
    # ADD COLUMN IF NOT EXISTS must also run every time to bring an
    # already-existing table up to date, not just fresh ones.
    for column in (
        "population", "gdp_constant_2015_usd", "gdp_per_capita_constant_2015_usd",
        "urban_population_pct", "demand_kwh_per_capita",
    ):
        assert column in FACT_ENERGY_DAILY_DDL

    alter_statements = [ddl for ddl in ALL_TABLE_DDL if ddl.strip().upper().startswith("ALTER TABLE FACT_ENERGY_DAILY")]
    assert len(alter_statements) == 1
    alter_ddl = alter_statements[0]
    for column in (
        "population", "gdp_constant_2015_usd", "gdp_per_capita_constant_2015_usd",
        "urban_population_pct", "demand_kwh_per_capita",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in alter_ddl

    # Must run after the CREATE (a table that doesn't exist yet can't be ALTERed).
    ddl_text = "\n".join(ALL_TABLE_DDL)
    assert ddl_text.index("CREATE TABLE IF NOT EXISTS fact_energy_daily") < ddl_text.index(alter_ddl)
