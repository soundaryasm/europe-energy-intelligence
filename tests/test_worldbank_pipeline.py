"""Tests for World Bank ingestion orchestration.

Only the HTTP fetch and the Spark write are external systems here and are
mocked. Bronze record construction runs for real inside `run_ingestion`.
"""
from unittest.mock import MagicMock

from src.config.countries import CountryConfig
from src.ingestion.worldbank_client import WorldBankAPIError
from src.ingestion.worldbank_pipeline import run_ingestion

IRELAND = CountryConfig("IE", "Ireland", "Dublin", 53.3498, -6.2603, "Europe/Dublin")
GERMANY = CountryConfig("DE", "Germany", "Berlin", 52.5200, 13.4050, "Europe/Berlin")


def _wb_records_for(indicator_code):
    return [
        {
            "indicator": {"id": indicator_code, "value": "whatever"},
            "country": {"id": "IE", "value": "Ireland"},
            "date": "2026",
            "value": 1.0,
        }
    ]


def test_run_ingestion_writes_records_for_every_indicator():
    fetch_fn = MagicMock(side_effect=lambda request, **_: _wb_records_for(request.indicator_code))
    spark_writer = MagicMock(return_value=4)

    result = run_ingestion(
        2026, spark=MagicMock(), countries=[IRELAND], fetch_fn=fetch_fn, spark_writer=spark_writer,
        indicator_codes=("SP.POP.TOTL", "NY.GDP.MKTP.KD"),
    )

    assert result.year == 2026
    assert result.indicators_attempted == ["SP.POP.TOTL", "NY.GDP.MKTP.KD"]
    assert result.indicators_succeeded == ["SP.POP.TOTL", "NY.GDP.MKTP.KD"]
    assert result.indicators_failed == []
    assert result.succeeded is True
    assert result.records_written == 4

    spark_writer.assert_called_once()
    written_records = spark_writer.call_args[0][1]
    assert len(written_records) == 2  # one per indicator


def test_run_ingestion_isolates_failure_to_one_indicator():
    def flaky_fetch(request, **_):
        if request.indicator_code == "NY.GDP.MKTP.KD":
            raise WorldBankAPIError("boom")
        return _wb_records_for(request.indicator_code)

    spark_writer = MagicMock(return_value=1)

    result = run_ingestion(
        2026, spark=MagicMock(), countries=[IRELAND], fetch_fn=flaky_fetch, spark_writer=spark_writer,
        indicator_codes=("SP.POP.TOTL", "NY.GDP.MKTP.KD"),
    )

    assert result.indicators_succeeded == ["SP.POP.TOTL"]
    assert result.indicators_failed == ["NY.GDP.MKTP.KD"]
    assert "NY.GDP.MKTP.KD" in result.errors
    assert result.succeeded is False

    written_records = spark_writer.call_args[0][1]
    assert all(r["indicator_code"] != "NY.GDP.MKTP.KD" for r in written_records)


def test_run_ingestion_treats_empty_result_as_success_not_failure():
    # Real case: a genuinely empty indicator/year result must not fail
    # the run — it just contributes zero rows.
    fetch_fn = MagicMock(return_value=[])
    spark_writer = MagicMock()

    result = run_ingestion(
        2026, spark=MagicMock(), countries=[IRELAND], fetch_fn=fetch_fn, spark_writer=spark_writer,
        indicator_codes=("SP.POP.TOTL",),
    )

    assert result.indicators_succeeded == ["SP.POP.TOTL"]
    assert result.succeeded is True
    assert result.records_written == 0
    spark_writer.assert_not_called()  # no partial/empty write when nothing to write


def test_run_ingestion_does_not_write_when_everything_fails():
    fetch_fn = MagicMock(side_effect=WorldBankAPIError("down"))
    spark_writer = MagicMock()

    result = run_ingestion(
        2026, spark=MagicMock(), countries=[IRELAND], fetch_fn=fetch_fn, spark_writer=spark_writer,
        indicator_codes=("SP.POP.TOTL",),
    )

    assert result.succeeded is False
    assert result.records_written == 0
    spark_writer.assert_not_called()


def test_run_ingestion_passes_all_configured_country_codes_in_one_request():
    fetch_fn = MagicMock(side_effect=lambda request, **_: _wb_records_for(request.indicator_code))

    run_ingestion(
        2026, spark=MagicMock(), countries=[IRELAND, GERMANY], fetch_fn=fetch_fn,
        spark_writer=MagicMock(return_value=0), indicator_codes=("SP.POP.TOTL",),
    )

    request = fetch_fn.call_args[0][0]
    assert list(request.country_codes) == ["IE", "DE"]
    assert request.year == 2026
