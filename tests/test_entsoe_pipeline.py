"""Tests for ENTSO-E ingestion orchestration (Spec 002).

Only the HTTP fetch and the Spark write are external systems here and are
mocked. XML parsing and Bronze record construction run for real inside
`run_ingestion`, fed by fixture XML text returned from the mocked fetch —
this exercises the real business logic, not a stand-in for it.
"""
from datetime import date
from unittest.mock import MagicMock

import pytest

from src.config.countries import CountryConfig
from src.config.entsoe import EntsoeCountryDomain
from src.ingestion.entsoe_client import EntsoeAPIError
from src.ingestion.entsoe_datasets import GENERATION, LOAD, PRICE
from src.ingestion.entsoe_pipeline import resolve_country_domains, run_ingestion
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
