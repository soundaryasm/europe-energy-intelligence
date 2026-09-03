"""Tests for ENTSO-E domain configuration loading (Spec 002)."""
import pytest

from src.config.entsoe import EntsoeConfigError, load_entsoe_domains

EXPECTED_CODES = {"IE", "DE", "FR", "ES", "NL"}


def test_load_entsoe_domains_covers_all_mvp_countries():
    domains = load_entsoe_domains()
    assert set(domains.keys()) == EXPECTED_CODES


def test_load_entsoe_domains_nl_is_validated_others_are_not():
    # Documents the open Spec 002 blocker: codes are sourced from public
    # docs, not yet confirmed against a live ENTSO-E account — except NL,
    # confirmed against a real tested request/response (see
    # tmp/entsoe.md, local-only reference notes).
    domains = load_entsoe_domains()
    assert domains["NL"].validated is True
    assert all(entry.validated is False for code, entry in domains.items() if code != "NL")


def test_load_entsoe_domains_rejects_duplicate_country(tmp_path):
    dup_config = tmp_path / "domains.yaml"
    dup_config.write_text(
        "domains:\n"
        "  - country_code: IE\n"
        "    domain: \"10Y1001A1001A59C\"\n"
        "  - country_code: IE\n"
        "    domain: \"10Y1001A1001A59C\"\n"
    )
    with pytest.raises(EntsoeConfigError):
        load_entsoe_domains(dup_config)


def test_load_entsoe_domains_rejects_missing_domain(tmp_path):
    bad_config = tmp_path / "domains.yaml"
    bad_config.write_text("domains:\n  - country_code: IE\n")
    with pytest.raises(EntsoeConfigError):
        load_entsoe_domains(bad_config)


def test_load_entsoe_domains_rejects_missing_file(tmp_path):
    with pytest.raises(EntsoeConfigError):
        load_entsoe_domains(tmp_path / "does-not-exist.yaml")
