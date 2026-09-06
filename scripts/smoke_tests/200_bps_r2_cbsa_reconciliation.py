"""Smoke 200: freeze the immutable-r2 BPS CBSA reconciliation."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


COMPILED = "src__census_bps__2026-04__r2__993afaddb934ce4f"
PROVISIONAL = "src__census_bps_provisional__2026-07__r2__61c56540953237cb"
COMPILED_MISSING = {
    "15680", "15700", "17340", "18860", "20660", "31460", "39780",
    "43760", "45000", "46020", "46380",
}
PROVISIONAL_MISSING = {"15680", "31460", "36140"}

concept_path = Path("config/bps_cbsa_canonical_concepts_v1.csv")
assert hashlib.sha256(concept_path.read_bytes()).hexdigest() == (
    "76007778d36e44e00a7ac83310761ac99175c5ef7d97da7a29cff9c75d88b03c"
)
concepts = pd.read_csv(concept_path, dtype=str)
governed = set(concepts.loc[concepts.bps_compatibility.eq("compatible"), "census_code"])
assert len(governed) == 53
assert not set(concepts.loc[concepts.canonical_concept.eq("metropolitan_division"), "census_code"]) & governed

compiled = governed - COMPILED_MISSING
provisional = governed - PROVISIONAL_MISSING
shared = compiled & provisional
compiled_only = compiled - provisional
provisional_only = provisional - compiled
union = compiled | provisional
absent = governed - union

assert (len(compiled), len(provisional), len(shared), len(compiled_only),
        len(provisional_only), len(union), len(absent)) == (42, 50, 41, 1, 9, 51, 2)
assert shared | compiled_only == compiled
assert shared | provisional_only == provisional
assert len(shared) + len(compiled_only) + len(provisional_only) == len(union)
assert len(union) + len(absent) == 53
assert compiled_only == {"36140"}
assert provisional_only == {"15700", "17340", "18860", "20660", "39780",
                            "43760", "45000", "46020", "46380"}
assert absent == {"15680", "31460"}
assert "32300" in shared  # Martinsville is physically present in both r2 parents.
assert "09999" not in union

catalog = json.loads(Path("config/artifact_catalog.json").read_text())
records = {item["object_id"]: item for item in catalog["immutable_records"]}
assert records[COMPILED]["artifact_content_hash"] == (
    "993afaddb934ce4f8ea40e14a8e29ce63ddb6c1c743ba1e976b796b185dced4e"
)
assert records[PROVISIONAL]["artifact_content_hash"] == (
    "61c56540953237cb72cc2fec062e9aeb092de411153cd78a250994254004f7ab"
)
print("[smoke] immutable-r2 BPS CBSA reconciliation passed")
