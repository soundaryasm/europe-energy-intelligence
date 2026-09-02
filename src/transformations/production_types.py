"""Centralized ENTSO-E production-type normalization and renewable
classification (Spec 003).

A single source of truth for mapping ENTSO-E's raw `psrType` codes to the
MVP's normalized categories (wind, solar, nuclear, gas, coal, hydro,
biomass, oil, other) and their renewable/non-renewable classification.
Spec 003 explicitly requires this to be centralized, not scattered across
transformation code, and requires unknown codes to be surfaced rather
than silently dropped.

SOURCE: ENTSO-E's publicly documented PSR type code list (B01-B20). NOT
independently re-verified against a live ENTSO-E account in this
environment. The renewable/non-renewable call for a few ambiguous fuels
(biomass, waste, geothermal, marine, hydro pumped storage) follows common
industry convention; review before treating it as final for a portfolio
or production audience.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

UNKNOWN_CATEGORY = "other"


@dataclass(frozen=True)
class ProductionTypeInfo:
    normalized_category: str
    renewable: bool
    label: str
    is_known: bool = True


_PRODUCTION_TYPE_MAP: Dict[str, ProductionTypeInfo] = {
    "B01": ProductionTypeInfo("biomass", True, "Biomass"),
    "B02": ProductionTypeInfo("coal", False, "Fossil Brown coal/Lignite"),
    "B03": ProductionTypeInfo("gas", False, "Fossil Coal-derived gas"),
    "B04": ProductionTypeInfo("gas", False, "Fossil Gas"),
    "B05": ProductionTypeInfo("coal", False, "Fossil Hard coal"),
    "B06": ProductionTypeInfo("oil", False, "Fossil Oil"),
    "B07": ProductionTypeInfo("oil", False, "Fossil Oil shale"),
    "B08": ProductionTypeInfo(UNKNOWN_CATEGORY, False, "Fossil Peat"),
    "B09": ProductionTypeInfo(UNKNOWN_CATEGORY, True, "Geothermal"),
    "B10": ProductionTypeInfo("hydro", True, "Hydro Pumped Storage"),
    "B11": ProductionTypeInfo("hydro", True, "Hydro Run-of-river and poundage"),
    "B12": ProductionTypeInfo("hydro", True, "Hydro Water Reservoir"),
    "B13": ProductionTypeInfo(UNKNOWN_CATEGORY, True, "Marine"),
    "B14": ProductionTypeInfo("nuclear", False, "Nuclear"),
    "B15": ProductionTypeInfo(UNKNOWN_CATEGORY, True, "Other renewable"),
    "B16": ProductionTypeInfo("solar", True, "Solar"),
    "B17": ProductionTypeInfo(UNKNOWN_CATEGORY, False, "Waste"),
    "B18": ProductionTypeInfo("wind", True, "Wind Offshore"),
    "B19": ProductionTypeInfo("wind", True, "Wind Onshore"),
    "B20": ProductionTypeInfo(UNKNOWN_CATEGORY, False, "Other"),
}

KNOWN_CATEGORIES = frozenset(info.normalized_category for info in _PRODUCTION_TYPE_MAP.values())


def get_production_type_info(psr_code: str) -> ProductionTypeInfo:
    """Look up the normalized category/renewable flag for a raw ENTSO-E psrType code.

    Unknown codes are mapped to `other` / non-renewable with `is_known=False`
    rather than raising or being dropped, so they stay visible to
    downstream data-quality reporting (Spec 003/007: "Unknown production
    types should be surfaced... do not silently drop them.").
    """
    if not psr_code:
        return ProductionTypeInfo(UNKNOWN_CATEGORY, False, "Unknown", is_known=False)

    info = _PRODUCTION_TYPE_MAP.get(psr_code.strip().upper())
    if info is None:
        return ProductionTypeInfo(UNKNOWN_CATEGORY, False, f"Unmapped ({psr_code})", is_known=False)
    return info
