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
