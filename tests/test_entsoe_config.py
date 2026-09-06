"""Tests for ENTSO-E domain configuration loading (Spec 002)."""
import pytest

from src.config.entsoe import EntsoeConfigError, load_entsoe_domains

EXPECTED_CODES = {
    "IE", "DE", "FR", "ES", "NL",
    "BE", "PT", "PL", "CZ", "FI", "HU", "RO", "SK", "SI", "EE",
}


def test_load_entsoe_domains_covers_all_mvp_countries():
    domains = load_entsoe_domains()
    assert set(domains.keys()) == EXPECTED_CODES


def test_load_entsoe_domains_all_are_validated():
    # Every domain has now been empirically confirmed: IE/DE/FR/ES/NL via
    # extensive real production ingestion, and the 10 expansion countries
    # via a dedicated historical coverage probe (2026-09-06, 313/315 real
    # checks against live ENTSO-E) — see entsoe_domains.yaml's header and
    # project memory for the full evidence trail.
    domains = load_entsoe_domains()
    assert all(entry.validated is True for entry in domains.values())


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
