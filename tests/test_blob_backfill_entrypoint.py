"""Tests for the historical ingestion entry point's reporting (Spec 010).

The exit status is the only signal a scheduled run leaves behind, so
what it reports for each outcome is the contract these cover.
"""
from datetime import date

import pytest

import scripts.run_blob_backfill as entrypoint
from src.blob.manifest import (
    STATUS_FAILED,
    STATUS_GIVEN_UP,
    STATUS_SUCCESS,
    Manifest,
    combo_id,
)
from src.blob.runner import MonthResult, YearResult

IE_GEN = combo_id("entsoe", "IE", "generation")
IE_LOAD = combo_id("entsoe", "IE", "load")


def _result(**statuses):
    month = MonthResult(month_start=date(2026, 1, 1), statuses=dict(statuses))
    return YearResult(year=2026, months=[month])


@pytest.fixture
def stub(monkeypatch):
    """Run `main()` with credentials present and a stubbed year run."""
    monkeypatch.setenv("ENTSOE_API_TOKEN", "t")
    monkeypatch.setattr(
        entrypoint, "BlobStore",
        type("S", (), {"from_env": staticmethod(lambda **k: object())}),
    )
    monkeypatch.setattr(entrypoint, "Manifest", type("M", (), {
        "load": staticmethod(lambda _s: Manifest()),
    }))

    def _set(result):
        monkeypatch.setattr(entrypoint, "run_year", lambda *a, **k: result)

    return _set


def test_a_fully_resolved_year_exits_zero(stub):
    stub(_result(**{IE_GEN: STATUS_SUCCESS}))

    assert entrypoint.main() == 0


def test_an_unresolved_combination_exits_two(stub):
    # Expected: it is retried on the next invocation, but should not
    # read as a clean run.
    stub(_result(**{IE_GEN: STATUS_FAILED}))

    assert entrypoint.main() == 2


def test_an_abandoned_combination_exits_three_not_zero(stub):
    # The bug this covers: `given_up` is not `failed`, so a filter on
    # `failed` alone reported a permanently abandoned month as success.
    stub(_result(**{IE_GEN: STATUS_GIVEN_UP}))

    assert entrypoint.main() == 3


def test_abandonment_is_reported_even_alongside_a_success(stub):
    stub(_result(**{IE_GEN: STATUS_GIVEN_UP, IE_LOAD: STATUS_SUCCESS}))

    assert entrypoint.main() == 3


def test_abandonment_outranks_a_merely_unresolved_combination(stub):
    # The worse outcome must not be masked by the milder one.
    stub(_result(**{IE_GEN: STATUS_GIVEN_UP, IE_LOAD: STATUS_FAILED}))

    assert entrypoint.main() == 3


def test_abandonment_is_logged_at_error_with_the_recovery_command(stub, caplog):
    stub(_result(**{IE_GEN: STATUS_GIVEN_UP}))

    with caplog.at_level("ERROR"):
        entrypoint.main()

    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "ABANDONED" in logged
    assert "reset_given_up.py" in logged
    assert IE_GEN in logged


def test_a_completed_walk_exits_zero(stub):
    stub(YearResult(year=None, complete=True))

    assert entrypoint.main() == 0


def test_missing_credentials_exit_one(monkeypatch):
    monkeypatch.delenv("ENTSOE_API_TOKEN", raising=False)

    assert entrypoint.main() == 1
