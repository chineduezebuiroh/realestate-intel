"""Smoke 174: field-level identity diagnostics and equal-data lineage changes."""
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.identity import compare_artifact_identity
from core.source_artifacts.models import CANONICAL_KEY


data = pd.DataFrame([{"geo_id":"x","metric_id":"inventory","date":"2026-07-31",
    "property_type_id":"all","value":1.0,"source_id":"fixture","property_type":"all"}])
base = data[CANONICAL_KEY].assign(provider_release_id="2026-07", provider_vintage="2026-07",
    source_request_identity="baseline:2026-07", latest_source_hash_or_drop_id="a"*64,
    source_artifact_id="baseline:2026-07")
drop = base.assign(source_request_identity="redfin-drop:"+"b"*64,
    latest_source_hash_or_drop_id="c"*64, source_artifact_id="pending")
common = dict(source_id="fixture", source_family="fixture", source_type="snapshot",
    provider="fixture", distribution_channel="fixture", provider_release_id="2026-07",
    provider_release_timestamp_or_date=None, retrieved_at=None, target_month="2026-07",
    source_request_identity="fixture", source_urls_or_endpoint_identity=["fixture"],
    artifact_created_at="2026-07-31T00:00:00Z")
raw = {"kind":"fixture", "files":[{"filename":"same.csv","sha256":"d"*64}]}

with TemporaryDirectory() as td:
    root = Path(td)
    left = create_artifact(root/"baseline", data, lineage=base, raw_source_lineage=raw, **common)
    right = create_artifact(root/"drop", data, lineage=drop, raw_source_lineage=raw, **common)
    report = compare_artifact_identity(root/"baseline", root/"drop")
    assert left["data_sha256"] == right["data_sha256"]
    assert left["artifact_id"] != right["artifact_id"]
    assert report["lineage_differing_rows"] == 1
    assert report["differing_lineage_fields"] == [
        "latest_source_hash_or_drop_id", "source_artifact_id", "source_request_identity"]
    assert report["raw_source_hashes_equal"] and report["identity_difference_reason"] == "lineage_ownership_changed"

print("source artifact identity diagnostic smoke: ok")
