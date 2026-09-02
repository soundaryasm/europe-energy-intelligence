"""PostgreSQL serving-layer schema (Spec 005).

DDL for the five approved serving tables only. Mirrors the Spec 004 Gold
contract exactly: changing a column here is a serving-contract change.
Explicit PostgreSQL types throughout (no reliance on inferred types).
"""

DIM_COUNTRY_DDL = """
CREATE TABLE IF NOT EXISTS dim_country (
    country_key VARCHAR(8) PRIMARY KEY,
    country_code VARCHAR(8) NOT NULL UNIQUE,
    country_name VARCHAR(100) NOT NULL,
    reference_location VARCHAR(100) NOT NULL,
    timezone VARCHAR(64) NOT NULL,
    entsoe_domain VARCHAR(64) NOT NULL
)
"""

DIM_DATE_DDL = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
)
"""

# Fact tables reference dim_country/dim_date: dimensions must be loaded
# first (Spec 005 "Dimensions must be loaded before dependent facts.").
FACT_ENERGY_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS fact_energy_daily (
    country_key VARCHAR(8) NOT NULL REFERENCES dim_country(country_key),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    daily_demand_mwh NUMERIC(14, 3),
    avg_day_ahead_price_eur_mwh NUMERIC(10, 3),
    min_day_ahead_price_eur_mwh NUMERIC(10, 3),
    max_day_ahead_price_eur_mwh NUMERIC(10, 3),
    total_generation_mwh NUMERIC(14, 3),
    renewable_generation_mwh NUMERIC(14, 3),
    renewable_generation_pct NUMERIC(5, 2),
    PRIMARY KEY (country_key, date_key)
)
"""

FACT_WEATHER_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS fact_weather_daily (
    country_key VARCHAR(8) NOT NULL REFERENCES dim_country(country_key),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    avg_temperature_c NUMERIC(6, 2),
    avg_wind_speed_kmh NUMERIC(6, 2),
    solar_radiation_mj_m2 NUMERIC(8, 3),
    reference_location VARCHAR(100),
    PRIMARY KEY (country_key, date_key)
)
"""

FACT_GENERATION_MIX_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS fact_generation_mix_daily (
    country_key VARCHAR(8) NOT NULL REFERENCES dim_country(country_key),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    production_type VARCHAR(32) NOT NULL,
    generation_mwh NUMERIC(14, 3),
    renewable_flag BOOLEAN NOT NULL,
    generation_share_pct NUMERIC(5, 2),
    PRIMARY KEY (country_key, date_key, production_type)
)
"""

# Ordered so dimensions are created (and therefore loadable) before facts.
ALL_TABLE_DDL = (
    DIM_COUNTRY_DDL,
    DIM_DATE_DDL,
    FACT_ENERGY_DAILY_DDL,
    FACT_WEATHER_DAILY_DDL,
    FACT_GENERATION_MIX_DAILY_DDL,
)

# Spec 005 "Indexing": realistic access patterns only (country+date,
# date, production type) — the MVP dataset does not need more.
INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_fact_energy_daily_date ON fact_energy_daily (date_key)",
    "CREATE INDEX IF NOT EXISTS idx_fact_weather_daily_date ON fact_weather_daily (date_key)",
    "CREATE INDEX IF NOT EXISTS idx_fact_generation_mix_daily_date ON fact_generation_mix_daily (date_key)",
    "CREATE INDEX IF NOT EXISTS idx_fact_generation_mix_daily_type ON fact_generation_mix_daily (production_type)",
)
