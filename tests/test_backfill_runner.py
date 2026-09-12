"""Tests for one backfill-job invocation's orchestration logic.

Every Spark-touching dependency (checkpoint read/write, Bronze coverage
lookups, the two ingestion pipelines) is injected as a mock — this
exercises only the orchestration/decision logic, no PySpark required.
"""
from datetime import date
from unittest.mock import MagicMock

from src.config.countries import CountryConfig
from src.ingestion.entsoe_pipeline import IngestionResult as EntsoeIngestionResult
from src.ingestion.open_meteo_pipeline import IngestionResult as OpenMeteoIngestionResult
from src.ingestion.worldbank_pipeline import IngestionResult as WorldBankIngestionResult
from src.orchestration.backfill_checkpoint import (
    CheckpointEntry,
    GIVE_UP_AFTER_ATTEMPTS,
    OPEN_METEO_DATASET,
    SOURCE_ENTSOE,
    SOURCE_OPEN_METEO,
    STATUS_FAILED,
    STATUS_GIVEN_UP,
    STATUS_SUCCESS,
    STATUS_UNAVAILABLE,
)
from src.orchestration.backfill_runner import determine_target_month, expected_combos, run_backfill_month

IRELAND = CountryConfig("IE", "Ireland", "Dublin", 53.3498, -6.2603, "Europe/Dublin")


def test_expected_combos_covers_every_entsoe_dataset_plus_one_weather_row_per_country():
    combos = expected_combos([IRELAND])

    assert (SOURCE_ENTSOE, "IE", "load") in combos
    assert (SOURCE_ENTSOE, "IE", "generation") in combos
    assert (SOURCE_ENTSOE, "IE", "price") in combos
    assert (SOURCE_OPEN_METEO, "IE", OPEN_METEO_DATASET) in combos
    assert len(combos) == 4


def test_determine_target_month_self_terminates_when_reader_reports_all_complete():
    def all_success_reader(spark, table_name):
        return {
            (SOURCE_ENTSOE, "IE", ds, date(2015, 1, 1)): CheckpointEntry(STATUS_SUCCESS, 1)
            for ds in ("load", "generation", "price")
        } | {(SOURCE_OPEN_METEO, "IE", OPEN_METEO_DATASET, date(2015, 1, 1)): CheckpointEntry(STATUS_SUCCESS, 1)}

    target = determine_target_month(
        MagicMock(), [IRELAND], reference_date=date(2015, 2, 15), checkpoint_reader=all_success_reader,
    )

    assert target is None


def test_determine_target_month_picks_the_only_month_when_checkpoint_is_empty():
    target = determine_target_month(
        MagicMock(), [IRELAND], reference_date=date(2015, 2, 15),
        checkpoint_reader=lambda spark, table_name: {},
    )

    assert target == date(2015, 1, 1)


def _entsoe_result(unavailable=(), errors=None, records_written=0):
    return EntsoeIngestionResult(
        started_at="2026-01-01T00:00:00+00:00",
        unavailable=list(unavailable),
        errors=errors or {},
        records_written=records_written,
    )


def _worldbank_result(errors=None, records_written=0):
    from datetime import datetime, timezone
    return WorldBankIngestionResult(
        started_at=datetime.now(timezone.utc),
        errors=errors or {},
        indicators_failed=list((errors or {}).keys()),
        records_written=records_written,
    )


def _open_meteo_result(errors=None, records_written=0):
    from datetime import datetime, timezone
    return OpenMeteoIngestionResult(
        started_at=datetime.now(timezone.utc),
        errors=errors or {},
        records_written=records_written,
    )


def test_run_backfill_month_marks_full_coverage_as_success():
    all_august_dates = frozenset(date(2026, 8, d) for d in range(1, 32))

    result = run_backfill_month(
        MagicMock(),
        token="tok",
        countries=[IRELAND],
        reference_date=date(2026, 9, 8),
        checkpoint_reader=lambda spark, table_name: {},
        checkpoint_writer=lambda spark, rows, table_name: len(rows),
        entsoe_covered_dates_fn=lambda spark, cc, ds, table: all_august_dates,
        open_meteo_covered_dates_fn=lambda spark, cc, table: all_august_dates,
        entsoe_ingestion_fn=lambda *a, **k: _entsoe_result(records_written=100),
        open_meteo_ingestion_fn=lambda *a, **k: _open_meteo_result(records_written=50),
        worldbank_ingestion_fn=lambda *a, **k: _worldbank_result(records_written=4),
    )

    assert result.target_month == date(2026, 8, 1)
    assert result.self_pause is False
    assert all(status == STATUS_SUCCESS for status in result.combo_statuses.values())
    assert result.checkpoint_rows_written == 4  # 3 entsoe datasets + 1 weather row
    assert result.worldbank_records_written == 4


def test_run_backfill_month_marks_confirmed_empty_entsoe_dataset_as_unavailable():
    result = run_backfill_month(
        MagicMock(),
        token="tok",
        countries=[IRELAND],
        reference_date=date(2026, 9, 8),
        checkpoint_reader=lambda spark, table_name: {},
        checkpoint_writer=lambda spark, rows, table_name: len(rows),
        entsoe_covered_dates_fn=lambda spark, cc, ds, table: frozenset(),
        open_meteo_covered_dates_fn=lambda spark, cc, table: frozenset(date(2026, 8, d) for d in range(1, 32)),
        entsoe_ingestion_fn=lambda *a, **k: _entsoe_result(
            unavailable=["IE:load", "IE:generation", "IE:price"],
        ),
        open_meteo_ingestion_fn=lambda *a, **k: _open_meteo_result(),
        worldbank_ingestion_fn=lambda *a, **k: _worldbank_result(),
    )

    assert result.combo_statuses[(SOURCE_ENTSOE, "IE", "load")] == STATUS_UNAVAILABLE


def test_run_backfill_month_marks_partial_coverage_as_failed_not_success():
    # The core rule under test: a partial month must never be waved
    # through, even if it's "mostly" there.
    partial = frozenset(date(2026, 8, d) for d in range(1, 31))  # missing Aug 31

    result = run_backfill_month(
        MagicMock(),
        token="tok",
        countries=[IRELAND],
        reference_date=date(2026, 9, 8),
        checkpoint_reader=lambda spark, table_name: {},
        checkpoint_writer=lambda spark, rows, table_name: len(rows),
        entsoe_covered_dates_fn=lambda spark, cc, ds, table: partial,
        open_meteo_covered_dates_fn=lambda spark, cc, table: partial,
        entsoe_ingestion_fn=lambda *a, **k: _entsoe_result(),
        open_meteo_ingestion_fn=lambda *a, **k: _open_meteo_result(),
        worldbank_ingestion_fn=lambda *a, **k: _worldbank_result(),
    )

    assert all(status == STATUS_FAILED for status in result.combo_statuses.values())


def test_run_backfill_month_escalates_to_given_up_after_threshold_attempts():
    # A combo that has already failed GIVE_UP_AFTER_ATTEMPTS - 1 times
    # before, and fails again now, must escalate to given_up rather than
    # staying failed forever — the exact scenario that would otherwise
    # block every older month indefinitely over one stuck combo.
    previous_attempts = GIVE_UP_AFTER_ATTEMPTS - 1
    existing = {
        (SOURCE_ENTSOE, "IE", "generation", date(2026, 8, 1)): CheckpointEntry(STATUS_FAILED, previous_attempts),
    }

    result = run_backfill_month(
        MagicMock(),
        token="tok",
        countries=[IRELAND],
        reference_date=date(2026, 9, 8),
        checkpoint_reader=lambda spark, table_name: existing,
        checkpoint_writer=lambda spark, rows, table_name: len(rows),
        entsoe_covered_dates_fn=lambda spark, cc, ds, table: (
            frozenset() if ds == "generation" else frozenset(date(2026, 8, d) for d in range(1, 32))
        ),
        open_meteo_covered_dates_fn=lambda spark, cc, table: frozenset(date(2026, 8, d) for d in range(1, 32)),
        entsoe_ingestion_fn=lambda *a, **k: _entsoe_result(),  # generation stays empty, not confirmed unavailable
        open_meteo_ingestion_fn=lambda *a, **k: _open_meteo_result(),
        worldbank_ingestion_fn=lambda *a, **k: _worldbank_result(),
    )

    assert result.combo_statuses[(SOURCE_ENTSOE, "IE", "generation")] == STATUS_GIVEN_UP
    # Everything else (full coverage) still succeeds normally.
    assert result.combo_statuses[(SOURCE_ENTSOE, "IE", "load")] == STATUS_SUCCESS


def test_run_backfill_month_does_not_escalate_before_threshold():
    existing = {
        (SOURCE_ENTSOE, "IE", "generation", date(2026, 8, 1)): CheckpointEntry(STATUS_FAILED, 1),
    }

    result = run_backfill_month(
        MagicMock(),
        token="tok",
        countries=[IRELAND],
        reference_date=date(2026, 9, 8),
        checkpoint_reader=lambda spark, table_name: existing,
        checkpoint_writer=lambda spark, rows, table_name: len(rows),
        entsoe_covered_dates_fn=lambda spark, cc, ds, table: (
            frozenset() if ds == "generation" else frozenset(date(2026, 8, d) for d in range(1, 32))
        ),
        open_meteo_covered_dates_fn=lambda spark, cc, table: frozenset(date(2026, 8, d) for d in range(1, 32)),
        entsoe_ingestion_fn=lambda *a, **k: _entsoe_result(),
        open_meteo_ingestion_fn=lambda *a, **k: _open_meteo_result(),
        worldbank_ingestion_fn=lambda *a, **k: _worldbank_result(),
    )

    # Second failure, still below GIVE_UP_AFTER_ATTEMPTS=3 — stays failed.
    assert result.combo_statuses[(SOURCE_ENTSOE, "IE", "generation")] == STATUS_FAILED


def test_given_up_lets_the_walker_advance_past_the_month():
    # Integration-level check that given_up genuinely unblocks
    # determine_target_month, not just that the status gets set.
    entries = {
        (SOURCE_ENTSOE, "IE", "load", date(2026, 8, 1)): CheckpointEntry(STATUS_SUCCESS, 1),
        (SOURCE_ENTSOE, "IE", "generation", date(2026, 8, 1)): CheckpointEntry(STATUS_GIVEN_UP, 3),
        (SOURCE_ENTSOE, "IE", "price", date(2026, 8, 1)): CheckpointEntry(STATUS_SUCCESS, 1),
        (SOURCE_OPEN_METEO, "IE", OPEN_METEO_DATASET, date(2026, 8, 1)): CheckpointEntry(STATUS_SUCCESS, 1),
    }

    target = determine_target_month(
        MagicMock(), [IRELAND], reference_date=date(2026, 9, 8),
        checkpoint_reader=lambda spark, table_name: entries,
    )

    assert target == date(2026, 7, 1)  # August is done (given_up counts), moved on to July


def test_run_backfill_month_self_pauses_when_nothing_left_to_backfill():
    result = run_backfill_month(
        MagicMock(),
        token="tok",
        countries=[IRELAND],
        reference_date=date(2015, 2, 15),
        checkpoint_reader=lambda spark, table_name: {
            (SOURCE_ENTSOE, "IE", ds, date(2015, 1, 1)): CheckpointEntry(STATUS_SUCCESS, 1)
            for ds in ("load", "generation", "price")
        } | {(SOURCE_OPEN_METEO, "IE", OPEN_METEO_DATASET, date(2015, 1, 1)): CheckpointEntry(STATUS_SUCCESS, 1)},
    )

    assert result.target_month is None
    assert result.self_pause is True
    assert result.checkpoint_rows_written == 0


def test_run_backfill_month_passes_buffered_range_to_entsoe_and_exact_month_to_open_meteo():
    captured = {}

    def capture_entsoe(start, end, **kwargs):
        captured["entsoe_start"], captured["entsoe_end"] = start, end
        return _entsoe_result()

    def capture_open_meteo(start, end, **kwargs):
        captured["om_start"], captured["om_end"] = start, end
        return _open_meteo_result()

    run_backfill_month(
        MagicMock(),
        token="tok",
        countries=[IRELAND],
        reference_date=date(2026, 9, 8),
        checkpoint_reader=lambda spark, table_name: {},
        checkpoint_writer=lambda spark, rows, table_name: len(rows),
        entsoe_covered_dates_fn=lambda spark, cc, ds, table: frozenset(),
        open_meteo_covered_dates_fn=lambda spark, cc, table: frozenset(),
        entsoe_ingestion_fn=capture_entsoe,
        open_meteo_ingestion_fn=capture_open_meteo,
        worldbank_ingestion_fn=lambda *a, **k: _worldbank_result(),
    )

    assert captured["entsoe_start"] == date(2026, 7, 30)  # August 1 minus 2-day buffer
    assert captured["entsoe_end"] == date(2026, 9, 2)     # August 31 plus 2-day buffer
    assert captured["om_start"] == date(2026, 8, 1)
    assert captured["om_end"] == date(2026, 8, 31)


def test_run_backfill_month_fetches_worldbank_for_the_target_months_year():
    # Agreed design: World Bank piggybacks on whichever year the target
    # month falls in — backfilling any month of 2019 re-fetches 2019's
    # indicators, not the target month's own year-independent logic.
    captured = {}

    def capture_worldbank(year, **kwargs):
        captured["year"] = year
        return _worldbank_result(records_written=4)

    result = run_backfill_month(
        MagicMock(),
        token="tok",
        countries=[IRELAND],
        reference_date=date(2019, 3, 15),  # target month: 2019-02
        checkpoint_reader=lambda spark, table_name: {},
        checkpoint_writer=lambda spark, rows, table_name: len(rows),
        entsoe_covered_dates_fn=lambda spark, cc, ds, table: frozenset(),
        open_meteo_covered_dates_fn=lambda spark, cc, table: frozenset(),
        entsoe_ingestion_fn=lambda *a, **k: _entsoe_result(),
        open_meteo_ingestion_fn=lambda *a, **k: _open_meteo_result(),
        worldbank_ingestion_fn=capture_worldbank,
    )

    assert captured["year"] == 2019
    assert result.worldbank_records_written == 4


def test_run_backfill_month_worldbank_failure_does_not_affect_entsoe_open_meteo_or_raise():
    # World Bank is best-effort here: a failure must never fail the
    # ENTSO-E/Open-Meteo month being processed, and must never affect
    # their checkpoint statuses.
    all_august_dates = frozenset(date(2026, 8, d) for d in range(1, 32))

    def failing_worldbank(*a, **k):
        raise RuntimeError("World Bank is down")

    result = run_backfill_month(
        MagicMock(),
        token="tok",
        countries=[IRELAND],
        reference_date=date(2026, 9, 8),
        checkpoint_reader=lambda spark, table_name: {},
        checkpoint_writer=lambda spark, rows, table_name: len(rows),
        entsoe_covered_dates_fn=lambda spark, cc, ds, table: all_august_dates,
        open_meteo_covered_dates_fn=lambda spark, cc, table: all_august_dates,
        entsoe_ingestion_fn=lambda *a, **k: _entsoe_result(records_written=100),
        open_meteo_ingestion_fn=lambda *a, **k: _open_meteo_result(records_written=50),
        worldbank_ingestion_fn=failing_worldbank,
    )

    assert all(status == STATUS_SUCCESS for status in result.combo_statuses.values())
    assert result.worldbank_records_written == 0
    assert "World Bank is down" in result.worldbank_error
