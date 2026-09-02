"""Tests for centralized ENTSO-E production-type normalization (Spec 003)."""
from src.transformations.production_types import (
    KNOWN_CATEGORIES,
    UNKNOWN_CATEGORY,
    get_production_type_info,
)

EXPECTED_CATEGORIES = {
    "wind", "solar", "nuclear", "gas", "coal", "hydro", "biomass", "oil", "other",
}


def test_known_categories_match_spec_003_list():
    assert KNOWN_CATEGORIES <= EXPECTED_CATEGORIES


def test_wind_offshore_and_onshore_both_map_to_wind():
    assert get_production_type_info("B18").normalized_category == "wind"
    assert get_production_type_info("B19").normalized_category == "wind"


def test_solar_is_renewable():
    info = get_production_type_info("B16")
    assert info.normalized_category == "solar"
    assert info.renewable is True


def test_nuclear_is_not_renewable():
    info = get_production_type_info("B14")
    assert info.normalized_category == "nuclear"
    assert info.renewable is False


def test_fossil_fuels_are_not_renewable():
    for code in ("B02", "B03", "B04", "B05", "B06", "B07"):
        assert get_production_type_info(code).renewable is False


def test_unknown_code_is_surfaced_not_dropped():
    info = get_production_type_info("Z99")
    assert info.is_known is False
    assert info.normalized_category == UNKNOWN_CATEGORY


def test_missing_code_is_surfaced_not_dropped():
    info = get_production_type_info("")
    assert info.is_known is False


def test_lookup_is_case_and_whitespace_insensitive():
    assert get_production_type_info(" b16 ").normalized_category == "solar"


def test_every_mapped_entry_has_a_boolean_renewable_flag():
    for code in ("B0" + str(i) for i in range(1, 10)):
        info = get_production_type_info(code)
        assert isinstance(info.renewable, bool)
