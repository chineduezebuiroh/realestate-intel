"""Smoke 206: the governed FRED macro contract matches metric ownership."""
from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

from sources.fred_macro.ingest import FRED_SERIES, SOURCE_ID, SPREAD_SERIES_META


registry_path = Path("config/source_metric_registry.csv")
with registry_path.open(newline="") as handle:
    registry = list(csv.DictReader(handle))

governed_metrics = set(FRED_SERIES) | set(SPREAD_SERIES_META)
fred_macro_metrics = {
    row["metric_id"] for row in registry if row["source_id"] == SOURCE_ID
}
assert len(governed_metrics) == 14
assert fred_macro_metrics == governed_metrics, {
    "missing": sorted(governed_metrics - fred_macro_metrics),
    "unexpected": sorted(fred_macro_metrics - governed_metrics),
}

governed_rows = [row for row in registry if row["metric_id"] in governed_metrics]
ownership = {metric_id: set() for metric_id in governed_metrics}
for row in governed_rows:
    ownership[row["metric_id"]].add(row["source_id"])
assert all(owners == {SOURCE_ID} for owners in ownership.values()), ownership

row_counts = Counter(row["metric_id"] for row in governed_rows)
assert all(count == 1 for count in row_counts.values()), row_counts

print("Smoke 206 FRED metric registry passed")
