"""Silver World Bank transformation.

Databricks-only: pivots Bronze World Bank observations (one row per
country/indicator/year — the schema produced by
`src/ingestion/worldbank_bronze.build_bronze_records`) into one row per
country/year, one column per indicator. PySpark is imported lazily
inside the function body so this module stays importable without a
PySpark installation.

World Bank data is annual at the source — this stays at that grain
rather than being exploded to a daily row per country. Joining a given
year's values onto every day of that year happens at Gold time (a join
on `country_code` + `YEAR(date)`), not here — storing the same value
365 times in Silver would be pure duplication for no benefit.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from src.transformations.dedupe import dedupe_latest

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

POPULATION_INDICATOR = "SP.POP.TOTL"
GDP_INDICATOR = "NY.GDP.MKTP.KD"
GDP_PER_CAPITA_INDICATOR = "NY.GDP.PCAP.KD"
URBAN_POPULATION_PCT_INDICATOR = "SP.URB.TOTL.IN.ZS"

SILVER_WORLDBANK_COLUMNS = (
    "country_code",
    "year",
    "population",
    "gdp_constant_2015_usd",
    "gdp_per_capita_constant_2015_usd",
    "urban_population_pct",
    "country_name",
    "source_system",
)


def build_silver_worldbank_annual(bronze_df: "DataFrame") -> "DataFrame":
    """Pivot Bronze World Bank indicators into `silver_worldbank_annual`."""
    from pyspark.sql import functions as F

    deduped = dedupe_latest(
        bronze_df, key_cols=["country_code", "indicator_code", "year"]
    )

    def _indicator(indicator_code: str, output_col: str):
        return deduped.filter(F.col("indicator_code") == indicator_code).select(
            "country_code", "year", F.col("value").alias(output_col)
        )

    population = _indicator(POPULATION_INDICATOR, "population")
    gdp = _indicator(GDP_INDICATOR, "gdp_constant_2015_usd")
    gdp_per_capita = _indicator(GDP_PER_CAPITA_INDICATOR, "gdp_per_capita_constant_2015_usd")
    urban_pct = _indicator(URBAN_POPULATION_PCT_INDICATOR, "urban_population_pct")

    reference = deduped.select(
        "country_code", "country_name", "source_system"
    ).dropDuplicates(["country_code"])

    result = (
        population.join(gdp, ["country_code", "year"], "outer")
        .join(gdp_per_capita, ["country_code", "year"], "outer")
        .join(urban_pct, ["country_code", "year"], "outer")
        .join(reference, ["country_code"], "left")
    )

    return result.select(*SILVER_WORLDBANK_COLUMNS)
