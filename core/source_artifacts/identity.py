"""Deterministic, field-level diagnostics for source artifact identity."""
from __future__ import annotations

import json
import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from .hashing import sha256_file
from .models import CANONICAL_KEY


IDENTITY_MANIFEST_FIELDS = (
    "source_id", "provider_release_id", "target_month", "data_sha256",
    "lineage_sha256", "schema_version", "config_hashes",
)


def compare_artifact_identity(left: Path, right: Path, *, example_limit: int = 5) -> dict[str, Any]:
    """Explain an identity difference without relying on final hashes alone."""
    lm = json.loads((left / "manifest.json").read_text())
    rm = json.loads((right / "manifest.json").read_text())
    ll = pd.read_parquet(left / "lineage.parquet")
    rl = pd.read_parquet(right / "lineage.parquet")
    merged = ll.merge(rl, on=CANONICAL_KEY, how="outer", suffixes=("_left", "_right"), indicator=True)
    fields = sorted((set(ll.columns) | set(rl.columns)) - set(CANONICAL_KEY))
    masks: dict[str, pd.Series] = {}
    for field in fields:
        a = merged.get(field + "_left", pd.Series(index=merged.index, dtype="object"))
        b = merged.get(field + "_right", pd.Series(index=merged.index, dtype="object"))
        masks[field] = ~(a.eq(b) | (a.isna() & b.isna()))
    differing = (merged["_merge"] != "both")
    for mask in masks.values():
        differing |= mask
    examples = []
    for _, row in merged.loc[differing].head(example_limit).iterrows():
        item = {key: (None if pd.isna(row.get(key)) else str(row.get(key))) for key in CANONICAL_KEY}
        item["differences"] = {field: {
            "left": None if pd.isna(row.get(field + "_left")) else str(row.get(field + "_left")),
            "right": None if pd.isna(row.get(field + "_right")) else str(row.get(field + "_right")),
        } for field, mask in masks.items() if bool(mask.loc[row.name])}
        examples.append(item)
    semantic_equal = all(lm.get(field) == rm.get(field) for field in IDENTITY_MANIFEST_FIELDS)
    raw_left = sorted((x["filename"], x["sha256"]) for x in (lm.get("raw_source_lineage") or {}).get("files", []))
    raw_right = sorted((x["filename"], x["sha256"]) for x in (rm.get("raw_source_lineage") or {}).get("files", []))
    return {
        "schema_version": "artifact_identity_diff_v1",
        "same_data_sha256": lm.get("data_sha256") == rm.get("data_sha256"),
        "same_validation_sha256": lm.get("validation_sha256") == rm.get("validation_sha256"),
        "same_config_hashes": lm.get("config_hashes") == rm.get("config_hashes"),
        "same_provider_release": lm.get("provider_release_id") == rm.get("provider_release_id"),
        "lineage_hash_equal": lm.get("lineage_sha256") == rm.get("lineage_sha256"),
        "lineage_differing_rows": int(differing.sum()),
        "differing_lineage_fields": [field for field in fields if bool(masks[field].any())],
        "lineage_examples": examples,
        "raw_source_hashes_equal": raw_left == raw_right,
        "semantic_identity_inputs_equal": semantic_equal,
        "identity_input_differences": {field: {"left": lm.get(field), "right": rm.get(field)}
                                       for field in IDENTITY_MANIFEST_FIELDS if lm.get(field) != rm.get(field)},
        "identity_difference_reason": "lineage_ownership_changed" if lm.get("lineage_sha256") != rm.get("lineage_sha256") else
                                      ("governed_identity_input_changed" if not semantic_equal else "identity_equal"),
        "member_sha256": {"left": {name: sha256_file(left / name) for name in ("data.parquet", "lineage.parquet", "validation.json")},
                          "right": {name: sha256_file(right / name) for name in ("data.parquet", "lineage.parquet", "validation.json")}},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Explain two source artifact identities")
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--example-limit", type=int, default=5)
    args = parser.parse_args()
    report = compare_artifact_identity(args.left, args.right, example_limit=args.example_limit)
    rendered = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
