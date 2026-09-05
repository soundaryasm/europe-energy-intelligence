"""Tests for ENTSO-E ingestion orchestration (Spec 002).

Only the HTTP fetch and the Spark write are external systems here and are
mocked. XML parsing and Bronze record construction run for real inside
`run_ingestion`, fed by fixture XML text returned from the mocked fetch —
this exercises the real business logic, not a stand-in for it.
"""
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.config.countries import CountryConfig
from src.config.entsoe import EntsoeCountryDomain
from src.ingestion.entsoe_client import EntsoeAPIError, EntsoeNoDataError
from src.ingestion.entsoe_datasets import GENERATION, LOAD, PRICE
from src.ingestion.entsoe_pipeline import _filter_points_within_window, resolve_country_domains, run_ingestion
from src.config.entsoe import EntsoeConfigError
from tests.fixtures_entsoe_xml import GENERATION_XML, LOAD_XML, PRICE_XML

IRELAND = CountryConfig("IE", "Ireland", "Dublin", 53.3498, -6.2603, "Europe/Dublin")
GERMANY = CountryConfig("DE", "Germany", "Berlin", 52.5200, 13.4050, "Europe/Berlin")

IE_DOMAIN = EntsoeCountryDomain("IE", "10Y1001A1001A59C", validated=False)
DE_DOMAIN = EntsoeCountryDomain("DE", "10Y1001A1001A82H", validated=False)

XML_BY_DATASET = {"load": LOAD_XML, "generation": GENERATION_XML, "price": PRICE_XML}
DATASETS = (LOAD, GENERATION, PRICE)


def _fetch_fn(request, **_):
    return XML_BY_DATASET[request.dataset.name]


def test_resolve_country_domains_raises_when_a_country_has_no_domain():
    with pytest.raises(EntsoeConfigError):
        resolve_country_domains([IRELAND, GERMANY], {"IE": IE_DOMAIN})


def test_resolve_country_domains_returns_matching_entries_in_country_order():
    resolved = resolve_country_domains([IRELAND, GERMANY], {"IE": IE_DOMAIN, "DE": DE_DOMAIN})
    assert [d.country_code for d in resolved] == ["IE", "DE"]


def test_run_ingestion_writes_records_for_every_country_and_dataset():
    spark_writer = MagicMock(return_value=42)

    result = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        token="tok",
        spark=MagicMock(),
        countries=[IRELAND],
        domains={"IE": IE_DOMAIN},
        datasets=DATASETS,
        fetch_fn=_fetch_fn,
        spark_writer=spark_writer,
    )

    assert result.countries_attempted == ["IE"]
    assert set(result.datasets_attempted) == {"load", "generation", "price"}
    assert result.succeeded == ["IE:load", "IE:generation", "IE:price"]
    assert result.failed == []
    assert result.all_succeeded is True
    assert result.records_written == 42

    spark_writer.assert_called_once()
    written_records = spark_writer.call_args[0][1]
    # load=2 points, generation=2 series x1 point, price=2 points -> 6 rows
    assert len(written_records) == 6


def test_run_ingestion_isolates_failure_to_one_dataset():
    def flaky_fetch(request, **_):
        if request.dataset.name == "price":
            raise EntsoeAPIError("price feed down")
        return XML_BY_DATASET[request.dataset.name]

    spark_writer = MagicMock(return_value=4)

    result = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        token="tok",
        spark=MagicMock(),
        countries=[IRELAND],
        domains={"IE": IE_DOMAIN},
        datasets=DATASETS,
        fetch_fn=flaky_fetch,
        spark_writer=spark_writer,
    )

    assert result.failed == ["IE:price"]
    assert set(result.succeeded) == {"IE:load", "IE:generation"}
    assert "IE:price" in result.errors
    assert result.all_succeeded is False

    written_records = spark_writer.call_args[0][1]
    assert all(r["dataset_type"] != "price" for r in written_records)


def test_run_ingestion_classifies_no_data_as_unavailable_not_failed():
    def fetch_with_one_unavailable(request, **_):
        if request.dataset.name == "generation":
            raise EntsoeNoDataError("no matching data found")
        return XML_BY_DATASET[request.dataset.name]

    spark_writer = MagicMock(return_value=4)

    result = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        token="tok",
        spark=MagicMock(),
        countries=[IRELAND],
        domains={"IE": IE_DOMAIN},
        datasets=DATASETS,
        fetch_fn=fetch_with_one_unavailable,
        spark_writer=spark_writer,
    )

    assert result.unavailable == ["IE:generation"]
    assert set(result.succeeded) == {"IE:load", "IE:price"}
    assert result.failed == []
    assert result.all_succeeded is True  # unavailable data must not fail the run
    assert "IE:generation" not in result.errors  # not an error, just unavailable

    written_records = spark_writer.call_args[0][1]
    assert all(r["dataset_type"] != "generation" for r in written_records)  # no synthesized rows


def test_run_ingestion_still_fails_when_a_real_failure_accompanies_unavailable_data():
    def fetch_fn(request, **_):
        if request.dataset.name == "generation":
            raise EntsoeNoDataError("no matching data found")
        if request.dataset.name == "price":
            raise EntsoeAPIError("price feed down")
        return XML_BY_DATASET[request.dataset.name]

    spark_writer = MagicMock(return_value=2)

    result = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        token="tok",
        spark=MagicMock(),
        countries=[IRELAND],
        domains={"IE": IE_DOMAIN},
        datasets=DATASETS,
        fetch_fn=fetch_fn,
        spark_writer=spark_writer,
    )

    assert result.unavailable == ["IE:generation"]
    assert result.failed == ["IE:price"]
    assert result.succeeded == ["IE:load"]
    assert result.all_succeeded is False  # a genuine failure still fails the run


def test_run_ingestion_isolates_failure_to_one_country():
    def flaky_fetch(request, **_):
        if request.country_code == "DE":
            raise EntsoeAPIError("DE outage")
        return XML_BY_DATASET[request.dataset.name]

    spark_writer = MagicMock(return_value=1)

    result = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        token="tok",
        spark=MagicMock(),
        countries=[IRELAND, GERMANY],
        domains={"IE": IE_DOMAIN, "DE": DE_DOMAIN},
        datasets=(LOAD,),
        fetch_fn=flaky_fetch,
        spark_writer=spark_writer,
    )

    assert result.succeeded == ["IE:load"]
    assert result.failed == ["DE:load"]


def test_run_ingestion_chunks_large_date_ranges_and_merges_records():
    call_count = {"n": 0}

    def counting_fetch(request, **_):
        call_count["n"] += 1
        return LOAD_XML

    spark_writer = MagicMock(return_value=99)

    run_ingestion(
        date(2024, 1, 1), date(2024, 4, 1),  # > 90 days
        token="tok",
        spark=MagicMock(),
        countries=[IRELAND],
        domains={"IE": IE_DOMAIN},
        datasets=(LOAD,),
        fetch_fn=counting_fetch,
        spark_writer=spark_writer,
        chunk_days=30,
    )

    assert call_count["n"] > 1  # more than one chunk was actually requested


def test_run_ingestion_does_not_write_when_everything_fails():
    fetch_fn = MagicMock(side_effect=EntsoeAPIError("down"))
    spark_writer = MagicMock()

    result = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 1),
        token="tok",
        spark=MagicMock(),
        countries=[IRELAND],
        domains={"IE": IE_DOMAIN},
        datasets=(LOAD,),
        fetch_fn=fetch_fn,
        spark_writer=spark_writer,
    )

    assert result.all_succeeded is False
    assert result.records_written == 0
    spark_writer.assert_not_called()


def test_run_ingestion_rejects_inverted_date_range():
    with pytest.raises(ValueError):
        run_ingestion(
            date(2024, 1, 5), date(2024, 1, 1),
            token="tok", countries=[IRELAND], domains={"IE": IE_DOMAIN},
        )


def test_filter_points_within_window_drops_points_outside_the_requested_range():
    # Real incident this guards against: ENTSO-E's day-ahead price (A44)
    # documents are published in whole local-calendar-day blocks, so a
    # request for a single UTC day can come back with points from the
    # adjacent day too (confirmed against a real response — see this
    # commit's history). A single-day reprocess must not silently persist
    # data for days nobody asked for.
    points = [
        {"source_timestamp": datetime(2024, 1, 1, 22, 0, tzinfo=timezone.utc)},  # before window
        {"source_timestamp": datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)},  # inside window
        {"source_timestamp": datetime(2024, 1, 3, 1, 0, tzinfo=timezone.utc)},  # after window
    ]

    filtered = _filter_points_within_window(points, date(2024, 1, 2), date(2024, 1, 2))

    assert filtered == [points[1]]


def test_run_ingestion_trims_dataset_response_to_the_requested_window():
    # GENERATION_XML's two points (wind + solar) always fall on
    # 2024-01-01, regardless of what window is requested (the mocked
    # fetch ignores request dates, like a real ENTSO-E day-ahead price
    # response returning a fixed local calendar day no matter the exact
    # UTC window asked for). Requesting a second day (chunked separately,
    # chunk_days=1) must not let those same 2024-01-01 points through a
    # second time under the 2024-01-02 window — without the trim, this
    # would silently double both records.
    def fetch_fn(request, **_):
        return XML_BY_DATASET[request.dataset.name]

    spark_writer = MagicMock(return_value=0)

    result = run_ingestion(
        date(2024, 1, 1), date(2024, 1, 2),
        token="tok", countries=[IRELAND], domains={"IE": IE_DOMAIN},
        datasets=(GENERATION,), fetch_fn=fetch_fn, spark_writer=spark_writer,
        chunk_days=1,
    )

    assert result.succeeded == ["IE:generation"]
    written_records = spark_writer.call_args[0][1]
    # Only the 2024-01-01 window's two points survive; the 2024-01-02
    # window's (identical, mocked) response gets fully filtered out.
    assert len(written_records) == 2
    assert all(r["source_timestamp"] == "2024-01-01T00:00:00Z" for r in written_records)
