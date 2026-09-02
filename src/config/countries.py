"""MVP country reference configuration (Spec 001).

Country metadata is kept in a single YAML file so latitude/longitude and
other reference data are never duplicated throughout ingestion code.
Application logic must load and iterate over this configuration rather
than hard-code per-country branches.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Union

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).with_name("countries.yaml")

REQUIRED_FIELDS = (
    "country_code",
    "country_name",
    "reference_location",
    "latitude",
    "longitude",
    "timezone",
)


class CountryConfigError(ValueError):
    """Raised when the country configuration file is missing or invalid."""


@dataclass(frozen=True)
class CountryConfig:
    country_code: str
    country_name: str
    reference_location: str
    latitude: float
    longitude: float
    timezone: str


def load_countries(path: Union[Path, str, None] = None) -> List[CountryConfig]:
    """Load and validate the configured MVP countries."""
    config_path = Path(path) if path is not None else DEFAULT_CONFIG_PATH

    if not config_path.exists():
        raise CountryConfigError(f"Country configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    entries = raw.get("countries")
    if not entries:
        raise CountryConfigError(f"No countries configured in {config_path}")

    countries: List[CountryConfig] = []
    seen_codes = set()

    for entry in entries:
        missing = [field for field in REQUIRED_FIELDS if entry.get(field) in (None, "")]
        if missing:
            raise CountryConfigError(
                f"Country entry {entry!r} is missing required fields: {missing}"
            )

        code = str(entry["country_code"]).strip().upper()
        if code in seen_codes:
            raise CountryConfigError(f"Duplicate country_code in configuration: {code}")
        seen_codes.add(code)

        try:
            latitude = float(entry["latitude"])
            longitude = float(entry["longitude"])
        except (TypeError, ValueError) as exc:
            raise CountryConfigError(
                f"Invalid latitude/longitude for country {code}: {exc}"
            ) from exc

        if not (-90.0 <= latitude <= 90.0):
            raise CountryConfigError(f"Latitude out of range for country {code}: {latitude}")
        if not (-180.0 <= longitude <= 180.0):
            raise CountryConfigError(f"Longitude out of range for country {code}: {longitude}")

        countries.append(
            CountryConfig(
                country_code=code,
                country_name=str(entry["country_name"]).strip(),
                reference_location=str(entry["reference_location"]).strip(),
                latitude=latitude,
                longitude=longitude,
                timezone=str(entry["timezone"]).strip(),
            )
        )

    return countries
