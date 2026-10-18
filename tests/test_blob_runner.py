"""Tests for the year-at-a-time historical runner (Spec 010).

The ingestion pipelines are injected, so these tests exercise the walk,
the narrowing of re-attempts and the classification wiring without
touching the network, Spark or Databricks.
"""
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List

import pytest

from src.blob import runner
from src.blob.manifest import (
    GIVE_UP_AFTER_ATTEMPTS,
    STATUS_FAILED,
    STATUS_GIVEN_UP,
    STATUS_SUCCESS,
    STATUS_UNAVAILABLE,
    Manifest,
    combo_id,
)
from src.blob.store import BlobStore
from src.config.countries import CountryConfig
from tests.test_blob_store import FakeS3

IRELAND = CountryConfig("IE", "Ireland", "Dublin", 53.3498, -6.2603, "Europe/Dublin")
GERMANY = CountryConfig("DE", "Germany", "Berlin", 52.52, 13.405, "Europe/Berlin")
COUNTRIES = [IRELAND, GERMANY]

# A run "today" of 2026-03-10, so the last complete month is 2026-02.
REFERENCE = date(2026, 3, 10)


def _store(objects=None):
    client = FakeS3(objects)
    return BlobStore(
        token="tok",
        credentials_fn=lambda _t: {
            "endpoint": "https://example.invalid", "bucket": "b",
            "accessKeyId": "ak", "secretAccessKey": "sk",
            "sessionToken": "st", "expiresAt": 10**12,
        },
        client_factory=lambda _c: client,
    )


@dataclass
class FakeResult:
    records_written: int = 0
    unavailable: List[str] = field(default_factory=list)
    errors: Dict[str, str] = field(default_factory=dict)


def _entsoe_fn(records_by_call=None, unavailable=None, errors=None, calls=None):
    def run(start_date, end_date, *, token, countries, chunk_days, spark_writer):
        if calls is not None:
            calls.append({"countries": [c.country_code for c in countries],
                          "start": start_date, "end": end_date})
        records = list(records_by_call or [])
        written = spark_writer(None, records, "table") if records else 0
        return FakeResult(written, list(unavailable or []), dict(errors or {}))
    return run


def _open_meteo_fn(records=None, errors=None, calls=None):
    def run(start_date, end_date, *, countries, spark_writer):
        if calls is not None:
            calls.append({"countries": [c.country_code for c in countries]})
        rows = list(records or [])
        written = spark_writer(None, rows, "table") if rows else 0
        return FakeResult(written, [], dict(errors or {}))
    return run


def _entsoe_rows(country, dataset, days, month=date(2026, 2, 1)):
    return [
        {
            "country_code": country, "domain": "d", "dataset_type": dataset,
            "source_timestamp": f"{month.year}-{month.month:02d}-{day:02d}T00:00:00Z",
            "source_resolution": "PT60M", "value": 1.0, "unit": "MW",
            "production_type_raw": None, "business_type": None, "currency": None,
            "source_document_mrid": None, "requested_start_date": "x",
            "requested_end_date": "y", "source_system": "entsoe",
            "ingestion_timestamp": "2026-03-01T00:00:00+00:00",
        }
        for day in days
    ]


def _all_days(month):
    from src.blob.completeness import month_dates
    return [d.day for d in month_dates(month)]


def test_last_complete_month_is_the_month_before_the_reference():
    assert runner.last_complete_month(REFERENCE) == date(2026, 2, 1)


def test_months_in_year_stops_at_the_last_complete_month():
    months = runner.months_in_year(2026, REFERENCE)

    assert months == [date(2026, 1, 1), date(2026, 2, 1)]


def test_months_in_year_excludes_months_before_the_historical_start():
    months = runner.months_in_year(runner.HISTORICAL_START.year - 1, REFERENCE)

    assert months == []


def test_a_partial_current_month_is_never_in_scope():
    # A month that cannot be complete would never reach a done status
    # and would hold the walk open indefinitely.
    assert date(2026, 3, 1) not in runner.months_in_year(2026, REFERENCE)


def test_buffered_range_pads_either_side_of_the_month():
    start, end = runner.buffered_entsoe_range(date(2026, 2, 1))

    assert start == date(2026, 1, 30)
    assert end == date(2026, 3, 2)


def test_expected_combos_covers_every_country_dataset_and_weather():
    combos = runner.expected_combos(COUNTRIES)

    assert combo_id("entsoe", "IE", "load") in combos
    assert combo_id("open_meteo", "DE", "weather") in combos
    # 2 countries x 3 ENTSO-E datasets, plus 2 weather combinations.
    assert len(combos) == 8


def test_next_year_starts_at_the_most_recent_year_with_work():
    assert runner.next_year(Manifest(), COUNTRIES, REFERENCE) == 2026


def test_next_year_walks_backwards_once_a_year_is_resolved():
    manifest = Manifest()
    for month in runner.months_in_year(2026, REFERENCE):
        for combo in runner.expected_combos(COUNTRIES):
            manifest.record(month, combo, STATUS_SUCCESS)

    assert runner.next_year(manifest, COUNTRIES, REFERENCE) == 2025


def test_next_year_is_none_once_every_month_is_accounted_for():
    manifest = Manifest()
    for year in range(runner.HISTORICAL_START.year, 2027):
        for month in runner.months_in_year(year, REFERENCE):
            for combo in runner.expected_combos(COUNTRIES):
                manifest.record(month, combo, STATUS_SUCCESS)

    assert runner.next_year(manifest, COUNTRIES, REFERENCE) is None


def test_a_fully_covered_month_is_recorded_as_success():
    month = date(2026, 2, 1)
    manifest = Manifest()
    rows = []
    for dataset in ("load", "generation", "price"):
        rows += _entsoe_rows("IE", dataset, _all_days(month), month)

    result = runner.run_month(
        _store(), manifest, month, token="t", countries=[IRELAND],
        entsoe_ingestion_fn=_entsoe_fn(rows),
        open_meteo_ingestion_fn=_open_meteo_fn([]),
    )

    assert result.statuses[combo_id("entsoe", "IE", "load")] == STATUS_SUCCESS


def test_a_partially_covered_month_is_recorded_as_failed():
    month = date(2026, 2, 1)
    manifest = Manifest()
    rows = _entsoe_rows("IE", "load", [1, 2, 3], month)

    result = runner.run_month(
        _store(), manifest, month, token="t", countries=[IRELAND],
        entsoe_ingestion_fn=_entsoe_fn(rows),
        open_meteo_ingestion_fn=_open_meteo_fn([]),
    )

    assert result.statuses[combo_id("entsoe", "IE", "load")] == STATUS_FAILED


def test_a_source_reported_no_data_month_is_recorded_as_unavailable():
    month = date(2026, 2, 1)
    manifest = Manifest()

    result = runner.run_month(
        _store(), manifest, month, token="t", countries=[IRELAND],
        entsoe_ingestion_fn=_entsoe_fn([], unavailable=["IE:load"]),
        open_meteo_ingestion_fn=_open_meteo_fn([]),
    )

    assert result.statuses[combo_id("entsoe", "IE", "load")] == STATUS_UNAVAILABLE
    # Without that signal, an empty dataset stays retryable.
    assert result.statuses[combo_id("entsoe", "IE", "price")] == STATUS_FAILED


def test_an_empty_weather_month_is_never_unavailable():
    # Open-Meteo has no positive "no data" signal, and a whole month
    # with no weather is not a legitimate condition.
    month = date(2026, 2, 1)
    manifest = Manifest()

    result = runner.run_month(
        _store(), manifest, month, token="t", countries=[IRELAND],
        entsoe_ingestion_fn=_entsoe_fn([]),
        open_meteo_ingestion_fn=_open_meteo_fn([]),
    )

    assert result.statuses[combo_id("open_meteo", "IE", "weather")] == STATUS_FAILED


def test_an_already_resolved_month_is_skipped_without_fetching():
    month = date(2026, 2, 1)
    manifest = Manifest()
    for combo in runner.expected_combos([IRELAND]):
        manifest.record(month, combo, STATUS_SUCCESS)
    calls = []

    result = runner.run_month(
        _store(), manifest, month, token="t", countries=[IRELAND],
        entsoe_ingestion_fn=_entsoe_fn(calls=calls),
        open_meteo_ingestion_fn=_open_meteo_fn(calls=calls),
    )

    assert result.skipped
    assert calls == []


def test_a_re_attempt_only_fetches_countries_that_are_unresolved():
    # What keeps a re-run cheap: a month sitting at all-but-one resolved
    # re-fetches the one country, not every country.
    month = date(2026, 2, 1)
    manifest = Manifest()
    for dataset in ("load", "generation", "price"):
        manifest.record(month, combo_id("entsoe", "DE", dataset), STATUS_SUCCESS)
    manifest.record(month, combo_id("open_meteo", "DE", "weather"), STATUS_SUCCESS)
    manifest.record(month, combo_id("open_meteo", "IE", "weather"), STATUS_SUCCESS)
    entsoe_calls = []

    runner.run_month(
        _store(), manifest, month, token="t", countries=COUNTRIES,
        entsoe_ingestion_fn=_entsoe_fn(calls=entsoe_calls),
        open_meteo_ingestion_fn=_open_meteo_fn(),
    )

    assert entsoe_calls[0]["countries"] == ["IE"]


def test_a_source_with_nothing_pending_is_not_called_at_all():
    month = date(2026, 2, 1)
    manifest = Manifest()
    for country in COUNTRIES:
        manifest.record(
            month, combo_id("open_meteo", country.country_code, "weather"), STATUS_SUCCESS,
        )
    weather_calls = []

    runner.run_month(
        _store(), manifest, month, token="t", countries=COUNTRIES,
        entsoe_ingestion_fn=_entsoe_fn(),
        open_meteo_ingestion_fn=_open_meteo_fn(calls=weather_calls),
    )

    assert weather_calls == []


def test_repeated_failures_escalate_a_combination_to_given_up():
    month = date(2026, 2, 1)
    manifest = Manifest()
    store = _store()

    for _ in range(GIVE_UP_AFTER_ATTEMPTS):
        result = runner.run_month(
            store, manifest, month, token="t", countries=[IRELAND],
            entsoe_ingestion_fn=_entsoe_fn([]),
            open_meteo_ingestion_fn=_open_meteo_fn([]),
        )

    assert result.statuses[combo_id("entsoe", "IE", "load")] == STATUS_GIVEN_UP


def test_run_year_reports_complete_when_there_is_nothing_left():
    manifest = Manifest()
    for year in range(runner.HISTORICAL_START.year, 2027):
        for month in runner.months_in_year(year, REFERENCE):
            for combo in runner.expected_combos(COUNTRIES):
                manifest.record(month, combo, STATUS_SUCCESS)

    result = runner.run_year(
        _store(), token="t", countries=COUNTRIES, reference_date=REFERENCE,
        manifest=manifest, worldbank_ingestion_fn=lambda *a, **k: None,
    )

    assert result.complete
    assert result.year is None


def test_run_year_saves_the_manifest_after_every_month():
    # An invocation killed by a timeout mid-year must keep the months it
    # already resolved.
    store = _store()
    manifest = Manifest()
    saves = []
    original_save = manifest.save
    manifest.save = lambda s: (saves.append(1), original_save(s))[1]

    runner.run_year(
        store, token="t", countries=[IRELAND], reference_date=REFERENCE,
        manifest=manifest,
        entsoe_ingestion_fn=_entsoe_fn([]),
        open_meteo_ingestion_fn=_open_meteo_fn([]),
        worldbank_ingestion_fn=lambda *a, **k: None,
    )

    assert len(saves) == len(runner.months_in_year(2026, REFERENCE))


def test_run_year_reports_unresolved_combinations():
    result = runner.run_year(
        _store(), token="t", countries=[IRELAND], reference_date=REFERENCE,
        manifest=Manifest(),
        entsoe_ingestion_fn=_entsoe_fn([]),
        open_meteo_ingestion_fn=_open_meteo_fn([]),
        worldbank_ingestion_fn=lambda *a, **k: None,
    )

    assert result.unresolved


def test_a_world_bank_failure_does_not_fail_the_year():
    # The indicators are a small reference set; the energy and weather
    # months an invocation resolved must survive a failure there.
    def boom(*_a, **_k):
        raise RuntimeError("world bank is down")

    result = runner.run_year(
        _store(), token="t", countries=[IRELAND], reference_date=REFERENCE,
        manifest=Manifest(),
        entsoe_ingestion_fn=_entsoe_fn([]),
        open_meteo_ingestion_fn=_open_meteo_fn([]),
        worldbank_ingestion_fn=boom,
    )

    assert result.year == 2026


def test_a_skipped_month_does_not_rewrite_the_manifest():
    # A year that is almost entirely done would otherwise rewrite the
    # whole manifest once per month for no reason.
    store = _store()
    manifest = Manifest()
    for month in runner.months_in_year(2026, REFERENCE):
        for combo in runner.expected_combos([IRELAND]):
            manifest.record(month, combo, STATUS_SUCCESS)
    # One month left open so the year is still selected for work.
    manifest.record(date(2026, 1, 1), combo_id("entsoe", "IE", "load"), STATUS_FAILED)

    saves = []
    original = manifest.save
    manifest.save = lambda s: (saves.append(1), original(s))[1]

    runner.run_year(
        store, token="t", countries=[IRELAND], reference_date=REFERENCE,
        manifest=manifest,
        entsoe_ingestion_fn=_entsoe_fn([]),
        open_meteo_ingestion_fn=_open_meteo_fn([]),
        worldbank_ingestion_fn=lambda *a, **k: None,
    )

    # Only the one month that actually ran.
    assert len(saves) == 1


def test_given_up_combinations_are_reported_separately_from_unresolved():
    # `given_up` is not `failed`, so a report that filters on `failed`
    # alone shows a permanently abandoned month as a clean run.
    month = date(2026, 2, 1)
    manifest = Manifest()
    store = _store()

    for _ in range(GIVE_UP_AFTER_ATTEMPTS):
        result = runner.run_year(
            store, token="t", countries=[IRELAND], reference_date=REFERENCE,
            manifest=manifest,
            entsoe_ingestion_fn=_entsoe_fn([]),
            open_meteo_ingestion_fn=_open_meteo_fn([]),
            worldbank_ingestion_fn=lambda *a, **k: None,
        )

    assert result.given_up
    assert not result.unresolved
