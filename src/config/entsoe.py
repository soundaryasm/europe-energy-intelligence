"""ENTSO-E bidding-zone/domain configuration (Spec 002).

Domain (EIC) codes live in `entsoe_domains.yaml`, separate from country
metadata in `countries.yaml`, so ingestion code never hard-codes them.
Every entry currently carries `validated: false` — see that file's header
for why, and treat it as an open blocker before production use.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Union

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).with_name("entsoe_domains.yaml")


class EntsoeConfigError(ValueError):
    """Raised when the ENTSO-E domain configuration is missing or invalid."""


# A country's configured domain is the one `entsoe_domains.yaml` holds.
# These are the second code to ask under when the configured one comes
# back with nothing — either because the country has changed bidding
# zone, or because it publishes different datasets under different codes
# at the same time.
#
# Kept here rather than in either pipeline because both need it: the
# historical walk retries a month the configured code left short of
# coverage, and the daily run retries a dataset the configured code
# reported no data for. One map, two callers.
ALTERNATIVE_DOMAINS: Dict[str, str] = {
    # DE-AT-LU, the zone Germany belonged to until the split on
    # 2018-10-01. Verified against live ENTSO-E on 2026-10-31, including
    # the handover falling mid-day on 2018-09-30.
    "DE": "10Y1001A1001A63L",
    # The EirGrid control area. Not a former zone: Ireland publishes
    # load only under this code and day-ahead price only under the SEM
    # bidding zone in the yaml, concurrently, with generation under
    # both. Verified against live ENTSO-E on 2026-11-01 for every month
    # the historical backfill had failed, 2016-04 through 2026-10, where
    # the SEM code returns four points a month or nothing at all.
    "IE": "10YIE-1001A00010",
}


@dataclass(frozen=True)
class EntsoeCountryDomain:
    country_code: str
    domain: str
    validated: bool


def load_entsoe_domains(path: Union[Path, str, None] = None) -> Dict[str, EntsoeCountryDomain]:
    """Load and validate the configured ENTSO-E country -> domain mapping."""
    config_path = Path(path) if path is not None else DEFAULT_CONFIG_PATH

    if not config_path.exists():
        raise EntsoeConfigError(f"ENTSO-E domain configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    entries = raw.get("domains")
    if not entries:
        raise EntsoeConfigError(f"No ENTSO-E domains configured in {config_path}")

    result: Dict[str, EntsoeCountryDomain] = {}
    for entry in entries:
        code = str(entry.get("country_code", "")).strip().upper()
        domain = str(entry.get("domain", "")).strip()
        if not code or not domain:
            raise EntsoeConfigError(f"Invalid ENTSO-E domain entry: {entry!r}")
        if code in result:
            raise EntsoeConfigError(f"Duplicate ENTSO-E domain entry for country: {code}")

        result[code] = EntsoeCountryDomain(
            country_code=code,
            domain=domain,
            validated=bool(entry.get("validated", False)),
        )

    return result
