"""Governed contract helpers for the independent FRED unemployment source."""
from __future__ import annotations

import hashlib
from pathlib import Path

SOURCE_ID = "fred_unemp"
METRIC_ID = "fred_unemployment_rate_sa"
CONTRACT_VERSION = "fred_unemp_governed_source_v1"
GOVERNED_CONFIG_PATHS = (
    "config/geo_manifest.generated.csv",
    "config/source_metric_registry.csv",
    "config/metric_dimension_registry.csv",
    "config/source_refresh_revision_policy_v0_2.json",
    "config/monthly_refresh_policy.json",
)


def governed_config_hashes(repository_root: Path = Path(".")) -> dict[str, str]:
    result: dict[str, str] = {}
    for relative in GOVERNED_CONFIG_PATHS:
        path = repository_root / relative
        if not path.is_file():
            raise FileNotFoundError(f"missing governed FRED unemployment configuration: {relative}")
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(sorted(result.items()))
