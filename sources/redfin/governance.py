from __future__ import annotations

import json
from pathlib import Path

RAW_ROOT = Path("data/redfin/raw")
BASELINE_ID = "2026-07"
BASELINE_MANIFEST = Path("config/redfin_baseline_manifest.json")
METRIC_DOMAIN_CONTRACT = Path("config/redfin_metric_domain_contract.json")
FAMILIES = ("nation", "state", "metro", "county", "city", "neighborhood", "zip")
FAMILY_FILENAME_TOKENS = {"nation": ("country",), "state": ("states",), "metro": ("metros",), "county": ("counties",), "city": ("cities",), "neighborhood": ("neighborhoods",), "zip": ("zips",)}
FAMILY_LEVELS = {"nation": ("nation",), "state": ("state",), "metro": ("cbsa_metro", "metro_area"), "county": ("county",), "city": ("city", "place"), "neighborhood": ("neighborhood",), "zip": ("zip", "zip_code")}
METRICS = frozenset({"average_sale_to_list_ratio", "homes_sold", "inventory", "median_days_on_market_days", "median_sale_price_nsa", "median_sale_price_per_sqft", "months_of_supply", "new_listings", "pending_sales", "percent_off_market_in_two_weeks", "share_sold_above_original_list"})
GOVERNED_PERCENTAGE_METRICS = frozenset({"average_sale_to_list_ratio", "share_sold_above_original_list", "percent_off_market_in_two_weeks"})
PROTECTED = (RAW_ROOT, RAW_ROOT / "baseline", RAW_ROOT / "current", RAW_ROOT / "quarantine")


class GovernanceError(RuntimeError):
    pass


def bootstrap(root: Path = RAW_ROOT) -> None:
    for relative in ("baseline/2026-07", "drops", "current", "quarantine", "incoming"):
        (root / relative).mkdir(parents=True, exist_ok=True)


def load_baseline_manifest(path: Path = BASELINE_MANIFEST) -> dict:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise GovernanceError(f"governed baseline manifest unavailable: {path}") from exc
    if payload.get("manifest_version") != 1 or payload.get("baseline_id") != BASELINE_ID or payload.get("immutable") is not True:
        raise GovernanceError("baseline identity/immutability contract mismatch")
    files = payload.get("files", [])
    if {f.get("geography_family") for f in files} != set(FAMILIES):
        raise GovernanceError("baseline manifest must govern exactly seven geography families")
    for item in files:
        digest = item.get("sha256", "")
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise GovernanceError(f"invalid governed SHA-256 for {item.get('filename')}")
    return payload


def load_metric_domain_contract(path: Path = METRIC_DOMAIN_CONTRACT) -> dict:
    """Load and strictly validate the sole numeric percentage-domain contract."""
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise GovernanceError(f"governed metric-domain contract unavailable: {path}") from exc
    if payload.get("contract_version") != 1:
        raise GovernanceError("unsupported metric-domain contract version")
    if payload.get("baseline_id") != BASELINE_ID:
        raise GovernanceError("metric-domain contract baseline mismatch")
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict) or set(metrics) != GOVERNED_PERCENTAGE_METRICS:
        raise GovernanceError("metric-domain contract must contain exactly three governed percentage metrics")
    fields = ("observed_baseline_min", "observed_baseline_max", "expected_min", "expected_max", "source_min", "source_max")
    for metric, rule in metrics.items():
        if not isinstance(rule, dict) or rule.get("unit") != "percentage_points":
            raise GovernanceError(f"invalid unit for governed metric {metric}")
        if any(isinstance(rule.get(field), bool) or not isinstance(rule.get(field), (int, float)) for field in fields):
            raise GovernanceError(f"non-numeric metric-domain bound for {metric}")
        observed_low, observed_high = rule["observed_baseline_min"], rule["observed_baseline_max"]
        expected_low, expected_high = rule["expected_min"], rule["expected_max"]
        source_low, source_high = rule["source_min"], rule["source_max"]
        if not (source_low <= expected_low <= expected_high <= source_high):
            raise GovernanceError(f"expected range is not contained in source envelope for {metric}")
        if not (source_low <= observed_low <= observed_high <= source_high):
            raise GovernanceError(f"observed baseline extrema are not contained in source envelope for {metric}")
    return payload


def assert_safe_delete(path: Path, root: Path = RAW_ROOT) -> None:
    target = path.resolve()
    protected = {p.resolve() for p in (root, root / "baseline", root / "current", root / "quarantine")}
    if target in protected or any(target in p.parents for p in protected):
        raise GovernanceError(f"refusing deletion of protected Redfin path: {path}")
    allowed = ((root / "drops").resolve(), (root / "quarantine").resolve())
    if not any(parent in target.parents for parent in allowed):
        raise GovernanceError(f"deletion outside governed drop contents: {path}")
