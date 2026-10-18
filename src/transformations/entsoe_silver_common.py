"""Shared ENTSO-E Silver helpers: local-date bucketing, interval-hours,
resolution selection, and daily-timeline completeness classification
(Spec 003).

Centralizes local-date bucketing, interval-duration conversion,
resolution selection, and completeness classification exactly once,
reused by the demand, price, and generation-mix Silver builders instead
of being copy-pasted three times. PySpark is imported lazily inside function bodies so this module
stays importable without a PySpark installation, per the project's
Databricks-only runtime rule.
"""
from __future__ import annotations

from datetime import date as _date, datetime, timedelta, timezone as dt_timezone
from typing import TYPE_CHECKING, Dict
from zoneinfo import ZoneInfo

from src.ingestion.entsoe_xml import parse_iso8601_duration_minutes

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

COMPLETE = "complete"
PARTIAL = "partial"

# Tolerance for floating-point interval-hours summation only — not a
# forgiving allowance for genuinely missing data (Spec 003 "Demand
# Completeness": do not hard-code a fixed point count as proof of
# completeness; base it on actual reconstructed timeline coverage).
_COMPLETENESS_TOLERANCE_HOURS = 1.0 / 60.0  # one minute


def interval_hours_udf():
    """UDF converting an ENTSO-E ISO-8601 resolution string to hours."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    def _hours(resolution: str) -> float:
        return parse_iso8601_duration_minutes(resolution) / 60.0

    return F.udf(_hours, DoubleType())


# ENTSO-E's hourly resolution code. Preferred where a country-day
# offers a choice — see `with_preferred_resolution`.
HOURLY_RESOLUTION = "PT60M"

# Hours per interval at the hourly resolution, used to identify those
# rows after `interval_hours_udf` has already been applied.
_HOURLY_INTERVAL_HOURS = 1.0


def with_preferred_resolution(df: "DataFrame", *, extra_partition_cols=()) -> "DataFrame":
    """Reduce each country-day to a single resolution.

    ENTSO-E publishes some series at more than one resolution at once,
    with genuinely different values for the same instant: an hourly
    price and the price for the first quarter of that hour are two
    distinct observations, not two copies of one. This follows the
    day-ahead market's move to a 15-minute market time unit (SDAC,
    30 September 2025). Bronze keeps every resolution, so the choice of
    which to model is made here rather than by whichever row won a
    merge.

    Hourly wins where a country-day offers it, by explicit decision: one
    grain across the whole history is easier to reason about than a
    series that changes grain partway through, and the hourly series
    covers every year back to the start of the historical range.

    Where a country-day has no hourly series it falls back to the finest
    resolution present. That fallback is not a nicety — most series have
    no hourly option at all (Irish load and generation publish at
    PT30M, German load and generation at PT15M), so a strict hourly
    filter would empty them entirely rather than switch their grain.

    Selection is per country-day, not per instant, because the
    aggregations downstream are interval-duration-weighted. Mixing
    resolutions inside one day would double-count the instants that
    exist at both — an hourly 22:00 contributing a full hour alongside a
    15-minute 22:00 contributing a quarter of one.

    Note that the hourly preference means a day-ahead price can diverge
    from the market's own settlement price once a zone has moved to
    15-minute clearing. That is accepted for now; Bronze retains the
    finer series, so revisiting the decision is a change here and a
    Silver rebuild, with no re-ingestion.

    Requires `local_date` and `interval_hours` to already be present.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    partition = Window.partitionBy("country_code", "local_date", *extra_partition_cols)
    is_hourly = F.col("interval_hours") == F.lit(_HOURLY_INTERVAL_HOURS)

    return (
        df.withColumn("_has_hourly", F.max(is_hourly.cast("int")).over(partition))
        .withColumn("_finest_interval_hours", F.min("interval_hours").over(partition))
        .filter(
            F.when(F.col("_has_hourly") == 1, is_hourly).otherwise(
                F.col("interval_hours") == F.col("_finest_interval_hours")
            )
        )
        .drop("_has_hourly", "_finest_interval_hours")
    )


def with_local_date(df: "DataFrame", country_timezones: Dict[str, str]) -> "DataFrame":
    """Bucket UTC source_timestamp into each country's local calendar date.

    `source_timestamp` carries an explicit "Z" suffix (see
    `entsoe_bronze.build_bronze_records`). Parsing it with no explicit
    format lets Spark's own ISO-8601 parser recognize that trailing zone
    marker and resolve an absolute instant directly — unlike an explicit
    no-offset pattern (e.g. `"yyyy-MM-dd'T'HH:mm:ss"`), which would parse
    the same digits as a *local* timestamp in whatever
    `spark.sql.session.timeZone` happens to be, silently depending on
    Databricks' current `Etc/UTC` default rather than being correct
    regardless of it.
    """
    from pyspark.sql import functions as F

    timezone_map = F.create_map([F.lit(x) for pair in country_timezones.items() for x in pair])
    return df.withColumn("_tz", timezone_map[F.col("country_code")]).withColumn(
        "local_date",
        F.to_date(F.from_utc_timestamp(F.to_timestamp(F.col("source_timestamp")), F.col("_tz"))),
    )


def _expected_day_duration_hours(local_date: _date, tz_name: str) -> float:
    """Expected duration (hours) of one local calendar day in one timezone.

    Computed as (next local midnight - this local midnight) converted to
    UTC, so DST transition days naturally resolve to 23 or 25 hours
    instead of a hard-coded 24 (Spec 003: "local DST days may legitimately
    represent different durations").
    """
    zone = ZoneInfo(tz_name)
    start = datetime(local_date.year, local_date.month, local_date.day, tzinfo=zone)
    end = start + timedelta(days=1)
    return (end.astimezone(dt_timezone.utc) - start.astimezone(dt_timezone.utc)).total_seconds() / 3600.0


def _expected_day_duration_hours_udf():
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    return F.udf(_expected_day_duration_hours, DoubleType())


def with_completeness_status(
    aggregated_df: "DataFrame",
    country_timezones: Dict[str, str],
    *,
    covered_duration_col: str = "covered_duration_hours",
) -> "DataFrame":
    """Classify each country/local_date row as `complete` or `partial`.

    `aggregated_df` must already carry `country_code`, `local_date`, and
    `covered_duration_col` (the summed interval-hours actually observed
    for that grain). A row is `complete` only when its covered duration
    reaches the expected duration of that local calendar day; otherwise
    it is `partial` (Spec 003 "Demand/Price/Generation Completeness": a
    partial timeline must never be published as a trusted complete daily
    metric). A country/local_date with zero observations produces no row
    at all here, and therefore is `unavailable` only implicitly — no
    Silver row is written for it (Spec 004/005 read that absence as
    null, not as a fabricated zero).
    """
    from pyspark.sql import functions as F

    timezone_map = F.create_map([F.lit(x) for pair in country_timezones.items() for x in pair])
    expected_udf = _expected_day_duration_hours_udf()

    with_expected = aggregated_df.withColumn(
        "_tz", timezone_map[F.col("country_code")]
    ).withColumn("_expected_duration_hours", expected_udf(F.col("local_date"), F.col("_tz")))

    classified = with_expected.withColumn(
        "completeness_status",
        F.when(
            F.col(covered_duration_col)
            >= F.col("_expected_duration_hours") - F.lit(_COMPLETENESS_TOLERANCE_HOURS),
            F.lit(COMPLETE),
        ).otherwise(F.lit(PARTIAL)),
    )

    return classified.drop("_tz", "_expected_duration_hours")
