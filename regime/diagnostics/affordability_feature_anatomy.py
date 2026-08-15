"""Affordability Phase-1 adapter for the canonical anatomy workflow."""
from __future__ import annotations

from pathlib import Path
import pandas as pd

from regime.diagnostics import price_feature_anatomy as canonical

DIMENSION = "affordability"
REVIEW_GEOS = canonical.REVIEW_GEOS
OUTPUTS = tuple(
    "affordability_dimension_statistics" if name == "price_dimension_statistics" else name
    for name in canonical.OUTPUTS
)


def resolve_contract(root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    return canonical.resolve_contract(root, DIMENSION)


def load_run(run: Path) -> dict[str, pd.DataFrame]:
    return canonical.load_run(run)


def build(artifacts: dict[str, pd.DataFrame], root: Path) -> dict[str, pd.DataFrame]:
    return canonical.build(artifacts, root, DIMENSION)


def write_review(tables: dict[str, pd.DataFrame], out: Path) -> None:
    canonical.write_review(tables, out, DIMENSION)
