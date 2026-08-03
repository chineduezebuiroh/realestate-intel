"""Smoke Test 92: focused Inventory causal-intervention contract.

Smoke 90 owns the real-constructor fixture and its fail-closed mutations. Run
that same public smoke entry point here so the corrective patch has an
explicitly named causal-intervention gate without a divergent fixture copy.
"""
from __future__ import annotations

import runpy
from pathlib import Path


def main() -> int:
    smoke = Path(__file__).with_name("90_inventory_challenger_completeness.py")
    namespace = runpy.run_path(str(smoke), run_name="inventory_completeness_contract")
    result = namespace["main"]()
    assert result == 0
    print("SMOKE TEST 92 — INVENTORY CAUSAL INTERVENTION: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
