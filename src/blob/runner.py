"""Year-at-a-time historical ingestion into blob storage (Spec 010).

One invocation resolves one year: the most recent year that still has an
unresolved month, working backwards towards `HISTORICAL_START`. Within
that year each month is fetched, classified, and written as Parquet, and
the manifest is saved after every month so an invocation that dies
part-way keeps the months it finished.

The fetch itself is the existing ingestion pipelines
(`src.ingestion.*_pipeline`), called with a Parquet writer in place of
their Delta writer. Their `spark` argument only ever reaches that
writer, so the HTTP layer, XML parsing, Bronze row construction, bounded
thread pool, circuit breaker and success/unavailable/failed
classification are all exercised exactly as they are on Databricks —
there is no second implementation of any of it here.

Only whole months are in scope, ending at the last complete month.
Partial months are not fetched: a month that cannot be complete would
never reach a done status and would hold the walk open indefinitely. The
current month-to-date is covered by running the daily pipeline in
`reprocess` mode once, after the historical load.
"""
from __future__ import annotations

import logging
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone as dt_timezone
from typing import Callable, Dict, List, Optional, Sequence

from src.blob import writer as blob_writer
from src.blob.completeness import (
    classify,
    covered_dates_from_records,
    evaluate_coverage,
    month_date_range,
)
from src.blob.manifest import (
    Manifest,
    STATUS_FAILED,
    combo_id,
    month_id,
)
from src.config.countries import CountryConfig, load_countries
from src.ingestion.entsoe_datasets import ALL_DATASETS

logger = logging.getLogger(__name__)

# The earliest month in scope. ENTSO-E's own published history does not
# extend meaningfully before this.
HISTORICAL_START = date(2015, 1, 1)

# ENTSO-E requests are padded this many days either side of the month so
# Silver can reconstruct complete local-timezone calendar dates at the
# edges. Only dates inside the plain month count toward completeness,
# and the overlap is harmless because the Bronze merge key makes the
# write idempotent.
ENTSOE_MONTH_BUFFER_DAYS = 2

# Smaller than the ingestion default: a month of a single country/dataset
# is still a large ENTSO-E response, and narrow windows keep an
# individual retry cheap.
ENTSOE_CHUNK_DAYS = 7

# Open-Meteo has no per-dataset split, so its combinations use this as
# the dataset segment and the manifest stays uniform across sources.
OPEN_METEO_DATASET = "weather"

ONE_DAY = timedelta(days=1)


@dataclass
class MonthResult:
    """What one month's attempt resolved, per combination."""

    month_start: date
    statuses: Dict[str, str] = field(default_factory=dict)
    entsoe_rows: int = 0
    open_meteo_rows: int = 0
    skipped: bool = False


@dataclass
class YearResult:
    """What one invocation resolved."""

    year: Optional[int]
    months: List[MonthResult] = field(default_factory=list)
    complete: bool = False
    started_at: str = ""
    ended_at: str = ""

    @property
    def unresolved(self) -> List[str]:
        """`month:combo` pairs this invocation left unresolved."""
        return [
            f"{month_id(m.month_start)}:{combo}"
            for m in self.months
            for combo, status in m.statuses.items()
            if status == STATUS_FAILED
        ]


def last_complete_month(reference_date: Optional[date] = None) -> date:
    """First day of the month before `reference_date`'s month."""
    today = reference_date or datetime.now(dt_timezone.utc).date()
    first_of_this_month = today.replace(day=1)
    return (first_of_this_month - ONE_DAY).replace(day=1)


def months_in_year(year: int, reference_date: Optional[date] = None) -> List[date]:
    """In-scope month starts for `year`, bounded by the historical window."""
    latest = last_complete_month(reference_date)
    months = []
    for month in range(1, 13):
        month_start = date(year, month, 1)
        if month_start < HISTORICAL_START or month_start > latest:
            continue
        months.append(month_start)
    return months


def buffered_entsoe_range(month_start: date, buffer_days: int = ENTSOE_MONTH_BUFFER_DAYS):
    """Date range to request from ENTSO-E for `month_start`, padded either side."""
    start, end = month_date_range(month_start)
    return start - timedelta(days=buffer_days), end + timedelta(days=buffer_days)


def expected_combos(countries: Sequence[CountryConfig]) -> List[str]:
    """Every combination a month must resolve before it counts as done."""
    combos = [
        combo_id(blob_writer.SOURCE_ENTSOE, country.country_code, dataset.name)
        for country in countries
        for dataset in ALL_DATASETS
    ]
    combos.extend(
        combo_id(blob_writer.SOURCE_OPEN_METEO, country.country_code, OPEN_METEO_DATASET)
        for country in countries
    )
    return combos


def next_year(
    manifest: Manifest,
    countries: Sequence[CountryConfig],
    reference_date: Optional[date] = None,
) -> Optional[int]:
    """Most recent year still holding an unresolved month, or `None` when done.

    Walks backwards from the last complete month's year so the most
    recent history lands first, which is the history most likely to be
    wanted while the rest is still filling.
    """
    latest_year = last_complete_month(reference_date).year
    for year in range(latest_year, HISTORICAL_START.year - 1, -1):
        for month_start in months_in_year(year, reference_date):
            if not manifest.month_is_done(month_start, expected_combos(countries)):
                return year
    return None


def _countries_with_pending(
    manifest: Manifest, month_start: date, countries: Sequence[CountryConfig], source: str,
) -> List[CountryConfig]:
    """Countries of `source` that still have an unresolved combination this month.

    Narrowing by country, not by country and dataset: failures
    concentrate per country, and the ingestion pipelines take a country
    list, so this keeps a re-attempt cheap without reaching inside them.
    """
    pending = []
    for country in countries:
        datasets = (
            [dataset.name for dataset in ALL_DATASETS]
            if source == blob_writer.SOURCE_ENTSOE
            else [OPEN_METEO_DATASET]
        )
        if any(
            not manifest.is_done(month_start, combo_id(source, country.country_code, name))
            for name in datasets
        ):
            pending.append(country)
    return pending


class _ParquetWriter:
    """A writer with the ingestion pipelines' `spark_writer` signature.

    The pipelines pass their `spark` argument straight through to this
    callable and use it for nothing else, so it is accepted and ignored.

    The records handed to the writer are also retained on `records`,
    because that call is the only place the full record set is exposed:
    `IngestionResult` reports counts and per-combination outcomes but
    not the rows themselves, and the coverage check needs the rows to
    establish which calendar dates a month actually got.
    """

    def __init__(self, store, source: str, month_start: date):
        self._store = store
        self._source = source
        self._month_start = month_start
        self.records: List[dict] = []

    def __call__(self, _spark, records: List[dict], _table_name: str) -> int:
        self.records = list(records)
        return blob_writer.write_month(
            self._store, self._source, self._month_start, records,
        )


def run_month(
    store,
    manifest: Manifest,
    month_start: date,
    *,
    token: str,
    countries: Sequence[CountryConfig],
    entsoe_ingestion_fn: Optional[Callable[..., object]] = None,
    open_meteo_ingestion_fn: Optional[Callable[..., object]] = None,
) -> MonthResult:
    """Fetch, classify and land one month's unresolved combinations."""
    combos = expected_combos(countries)
    if manifest.month_is_done(month_start, combos):
        logger.info("%s already resolved; skipping", month_id(month_start))
        return MonthResult(month_start=month_start, skipped=True)

    if entsoe_ingestion_fn is None:
        from src.ingestion.entsoe_pipeline import run_ingestion as entsoe_ingestion_fn
    if open_meteo_ingestion_fn is None:
        from src.ingestion.open_meteo_pipeline import run_ingestion as open_meteo_ingestion_fn

    result = MonthResult(month_start=month_start)

    entsoe_countries = _countries_with_pending(
        manifest, month_start, countries, blob_writer.SOURCE_ENTSOE,
    )
    if entsoe_countries:
        entsoe_start, entsoe_end = buffered_entsoe_range(month_start)
        entsoe_sink = _ParquetWriter(store, blob_writer.SOURCE_ENTSOE, month_start)
        entsoe_result = entsoe_ingestion_fn(
            entsoe_start, entsoe_end,
            token=token,
            countries=list(entsoe_countries),
            chunk_days=ENTSOE_CHUNK_DAYS,
            spark_writer=entsoe_sink,
        )
        result.entsoe_rows = entsoe_result.records_written
        _record_entsoe(
            manifest, month_start, entsoe_countries, entsoe_result,
            entsoe_sink.records, result,
        )

    open_meteo_countries = _countries_with_pending(
        manifest, month_start, countries, blob_writer.SOURCE_OPEN_METEO,
    )
    if open_meteo_countries:
        weather_start, weather_end = month_date_range(month_start)
        weather_sink = _ParquetWriter(store, blob_writer.SOURCE_OPEN_METEO, month_start)
        open_meteo_result = open_meteo_ingestion_fn(
            weather_start, weather_end,
            countries=list(open_meteo_countries),
            spark_writer=weather_sink,
        )
        result.open_meteo_rows = open_meteo_result.records_written
        _record_open_meteo(
            manifest, month_start, open_meteo_countries, open_meteo_result,
            weather_sink.records, result,
        )

    return result


def _record_entsoe(
    manifest, month_start, countries, ingestion_result, records, month_result,
) -> None:
    for country in countries:
        for dataset in ALL_DATASETS:
            attempt_key = f"{country.country_code}:{dataset.name}"
            covered = covered_dates_from_records(
                records,
                date_field="source_timestamp",
                country_code=country.country_code,
                dataset_field="dataset_type",
                dataset=dataset.name,
            )
            status = classify(
                evaluate_coverage(month_start, covered),
                source_reported_no_data=attempt_key in ingestion_result.unavailable,
            )
            combo = combo_id(blob_writer.SOURCE_ENTSOE, country.country_code, dataset.name)
            month_result.statuses[combo] = manifest.record(
                month_start, combo, status,
                error=ingestion_result.errors.get(attempt_key),
            )


def _record_open_meteo(
    manifest, month_start, countries, ingestion_result, records, month_result,
) -> None:
    for country in countries:
        covered = covered_dates_from_records(
            records, date_field="observation_date", country_code=country.country_code,
        )
        # Open-Meteo has no positive "no data" signal of its own, unlike
        # ENTSO-E's acknowledgement document, so an empty month is
        # always `failed` here. A whole month with no weather is not a
        # legitimate condition the way a missing market series can be.
        status = classify(
            evaluate_coverage(month_start, covered), source_reported_no_data=False,
        )
        combo = combo_id(
            blob_writer.SOURCE_OPEN_METEO, country.country_code, OPEN_METEO_DATASET,
        )
        month_result.statuses[combo] = manifest.record(
            month_start, combo, status,
            error=ingestion_result.errors.get(country.country_code),
        )


def run_year(
    store,
    *,
    token: str,
    countries: Optional[List[CountryConfig]] = None,
    reference_date: Optional[date] = None,
    manifest: Optional[Manifest] = None,
    worldbank_ingestion_fn: Optional[Callable[..., object]] = None,
    **month_kwargs,
) -> YearResult:
    """Resolve one year of history, saving progress after each month."""
    resolved_countries = countries if countries is not None else load_countries()
    resolved_manifest = manifest if manifest is not None else Manifest.load(store)

    started_at = datetime.now(dt_timezone.utc).isoformat()
    year = next_year(resolved_manifest, resolved_countries, reference_date)
    if year is None:
        logger.info("Every month back to %s is accounted for", HISTORICAL_START)
        return YearResult(
            year=None, complete=True, started_at=started_at,
            ended_at=datetime.now(dt_timezone.utc).isoformat(),
        )

    logger.info("Resolving %s", year)
    result = YearResult(year=year, started_at=started_at)

    for month_start in months_in_year(year, reference_date):
        month_result = run_month(
            store, resolved_manifest, month_start,
            token=token, countries=resolved_countries, **month_kwargs,
        )
        result.months.append(month_result)
        # Saved per month, not once at the end: an invocation killed by
        # a timeout mid-year keeps every month it already resolved.
        resolved_manifest.save(store)

    _run_worldbank(store, year, resolved_countries, worldbank_ingestion_fn)

    result.ended_at = datetime.now(dt_timezone.utc).isoformat()
    return result


def _run_worldbank(store, year: int, countries, worldbank_ingestion_fn) -> None:
    """Land this year's World Bank indicators, best effort.

    Annual rather than monthly, and outside the manifest entirely: the
    indicators are a small reference set with no per-month completeness
    to assess. A failure here must never affect the energy and weather
    months the invocation resolved, so it is contained rather than
    propagated.
    """
    if worldbank_ingestion_fn is None:
        from src.ingestion.worldbank_pipeline import run_ingestion as worldbank_ingestion_fn

    def write(_spark, records: List[dict], _table_name: str) -> int:
        return blob_writer.write_year(store, blob_writer.SOURCE_WORLDBANK, year, records)

    try:
        worldbank_ingestion_fn(year, countries=list(countries), spark_writer=write)
    except Exception as exc:  # noqa: BLE001 - must not fail the year's energy/weather months
        logger.error("World Bank fetch failed for %s: %s", year, exc)
