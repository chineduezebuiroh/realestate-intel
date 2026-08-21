from __future__ import annotations

import json
from pathlib import Path

RAW_ROOT = Path("data/redfin/raw")
BASELINE_ID = "2026-07"
BASELINE_MANIFEST = Path("config/redfin_baseline_manifest.json")
FAMILIES = ("nation", "state", "metro", "county", "city", "neighborhood", "zip")
FAMILY_FILENAME_TOKENS = {"nation": ("country",), "state": ("states",), "metro": ("metros",), "county": ("counties",), "city": ("cities",), "neighborhood": ("neighborhoods",), "zip": ("zips",)}
FAMILY_LEVELS = {"nation": ("nation",), "state": ("state",), "metro": ("cbsa_metro", "metro_area"), "county": ("county",), "city": ("city", "place"), "neighborhood": ("neighborhood",), "zip": ("zip", "zip_code")}
METRICS = frozenset({"average_sale_to_list_ratio", "homes_sold", "inventory", "median_days_on_market_days", "median_sale_price_nsa", "median_sale_price_per_sqft", "months_of_supply", "new_listings", "pending_sales", "percent_off_market_in_two_weeks", "share_sold_above_original_list"})
PROTECTED = (RAW_ROOT, RAW_ROOT / "baseline", RAW_ROOT / "current", RAW_ROOT / "quarantine")


class GovernanceError(RuntimeError):
    pass


def bootstrap(root: Path = RAW_ROOT) -> None:
    for relative in ("baseline/2026-07", "drops", "current", "quarantine"):
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


def assert_safe_delete(path: Path, root: Path = RAW_ROOT) -> None:
    target = path.resolve()
    protected = {p.resolve() for p in (root, root / "baseline", root / "current", root / "quarantine")}
    if target in protected or any(target in p.parents for p in protected):
        raise GovernanceError(f"refusing deletion of protected Redfin path: {path}")
    allowed = ((root / "drops").resolve(), (root / "quarantine").resolve())
    if not any(parent in target.parents for parent in allowed):
        raise GovernanceError(f"deletion outside governed drop contents: {path}")
