"""Tests for MVP country configuration loading (Spec 001)."""
import pytest

from src.config.countries import CountryConfigError, load_countries

EXPECTED_CODES = {
    "IE", "DE", "FR", "ES", "NL",
    "BE", "PT", "PL", "CZ", "FI", "HU", "RO", "SK", "SI", "EE",
}


def test_load_countries_returns_fifteen_mvp_countries():
    countries = load_countries()
    assert {c.country_code for c in countries} == EXPECTED_CODES


def test_load_countries_reference_locations_match_capitals():
    countries = {c.country_code: c for c in load_countries()}
    assert countries["IE"].reference_location == "Dublin"
    assert countries["DE"].reference_location == "Berlin"
    assert countries["FR"].reference_location == "Paris"
    assert countries["ES"].reference_location == "Madrid"
    assert countries["NL"].reference_location == "Amsterdam"


def test_load_countries_parses_coordinates_as_float():
    for country in load_countries():
        assert isinstance(country.latitude, float)
        assert isinstance(country.longitude, float)


def test_load_countries_rejects_missing_required_field(tmp_path):
    bad_config = tmp_path / "countries.yaml"
    bad_config.write_text(
        "countries:\n"
        "  - country_code: IE\n"
        "    country_name: Ireland\n"
        "    reference_location: Dublin\n"
        "    latitude: 53.3498\n"
        "    timezone: Europe/Dublin\n"  # longitude intentionally missing
    )
    with pytest.raises(CountryConfigError):
        load_countries(bad_config)


def test_load_countries_rejects_duplicate_country_code(tmp_path):
    dup_config = tmp_path / "countries.yaml"
    dup_config.write_text(
        "countries:\n"
        "  - country_code: IE\n"
        "    country_name: Ireland\n"
        "    reference_location: Dublin\n"
        "    latitude: 53.3498\n"
        "    longitude: -6.2603\n"
        "    timezone: Europe/Dublin\n"
        "  - country_code: IE\n"
        "    country_name: Ireland Duplicate\n"
        "    reference_location: Dublin\n"
        "    latitude: 53.3498\n"
        "    longitude: -6.2603\n"
        "    timezone: Europe/Dublin\n"
    )
    with pytest.raises(CountryConfigError):
        load_countries(dup_config)


def test_load_countries_rejects_out_of_range_latitude(tmp_path):
    bad_config = tmp_path / "countries.yaml"
    bad_config.write_text(
        "countries:\n"
        "  - country_code: XX\n"
        "    country_name: Nowhere\n"
        "    reference_location: Nowhere City\n"
        "    latitude: 200\n"
        "    longitude: 0\n"
        "    timezone: UTC\n"
    )
    with pytest.raises(CountryConfigError):
        load_countries(bad_config)


def test_load_countries_rejects_empty_country_list(tmp_path):
    empty_config = tmp_path / "countries.yaml"
    empty_config.write_text("countries: []\n")
    with pytest.raises(CountryConfigError):
        load_countries(empty_config)


def test_load_countries_rejects_missing_file(tmp_path):
    with pytest.raises(CountryConfigError):
        load_countries(tmp_path / "does-not-exist.yaml")
