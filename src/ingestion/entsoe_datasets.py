"""ENTSO-E dataset type definitions (Spec 002).

Centralizes the ENTSO-E document/process type codes for the three
required MVP datasets, so they are not scattered through ingestion logic.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class EntsoeDataset:
    name: str
    document_type: str
    process_type: Optional[str]
    # Root element local-name(s) a successful response is expected to use.
    expected_root_tags: Tuple[str, ...]
    # XML tag holding the observation value inside each <Point>.
    value_tag: str


LOAD = EntsoeDataset(
    name="load",
    document_type="A65",
    process_type="A16",
    expected_root_tags=("GL_MarketDocument",),
    value_tag="quantity",
)

GENERATION = EntsoeDataset(
    name="generation",
    document_type="A75",
    process_type="A16",
    expected_root_tags=("GL_MarketDocument",),
    value_tag="quantity",
)

PRICE = EntsoeDataset(
    name="price",
    document_type="A44",
    process_type=None,
    expected_root_tags=("Publication_MarketDocument",),
    value_tag="price.amount",
)

ALL_DATASETS: Tuple[EntsoeDataset, ...] = (LOAD, GENERATION, PRICE)

DATASETS_BY_NAME = {dataset.name: dataset for dataset in ALL_DATASETS}
