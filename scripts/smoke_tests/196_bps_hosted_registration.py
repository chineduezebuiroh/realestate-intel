"""Smoke 196: independent BPS hosted lifecycle, results, and cohort barrier."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from jobs.monthly_refresh.bps_hosted import execute_member
from jobs.monthly_refresh.cohort import required_sources, resume_plan
from jobs.monthly_refresh.source_inputs import FilePinStore, provider_pin


def pin(cycle: str, source: str, release: str, member_names: set[str]) -> dict:
    import hashlib
    return provider_pin(cycle_id=cycle, source_id=source, provider_release_id=release,
        members={name: {"url": f"https://provider.invalid/{release}/{name}",
            "retrieved_at": "2026-09-03T00:00:00Z", "sha256": hashlib.sha256(name.encode()).hexdigest()}
            for name in member_names})


def result(source: str, cycle: str, release: str) -> dict:
    return {"schema_version":"monthly_source_execution_result_v1", "source_id":source,
        "cycle_id":cycle, "status":"succeeded", "candidate_artifact_id":f"candidate-{source}",
        "artifact_content_hash":"a"*64, "package_sha256":"b"*64,
        "publication_state":"published_verified", "validation_status":"passed",
        "provider_release_id":release, "observation_max":"2026-07-01", "prior_artifact_id":None,
        "source_change_detected":True, "retryability":"not_applicable",
        "accepted_pointer_changed":False, "evidence_uri":f"artifact://source/{source}/candidate-{source}"}


with tempfile.TemporaryDirectory() as value:
    root = Path(value); cycle = "cycle_fixture"
    records, discoveries, executions = [], [], []
    specifications = (("census_bps", "compiled-release", {"compiled_zip"}),
                      ("census_bps_provisional", "provisional-release", {"state", "county", "cbsa_metro"}))
    for source, release, members in specifications:
        source_pin = pin(cycle, source, release, members)
        def discover(p=source_pin, s=source):
            discoveries.append(s); return p, {name: root / name for name in p["members"]}
        def build(*, pin, paths, output, cycle_id, s=source):
            assert FilePinStore(root).get(cycle, s) == pin  # durable before execution
            executions.append((s, pin["provider_release_id"])); output.mkdir(parents=True)
            return {"manifest": {"artifact_id": f"candidate-{s}"}}
        def publish(path, s, release=release):
            item = {"object_id":f"candidate-{s}", "artifact_content_hash":"a"*64,
                "package_sha256":"b"*64, "logical_artifact_uri":f"artifact://source/{s}/candidate-{s}",
                "metadata":{"provider_release_id":release, "observation_max":"2026-07-01"}}
            return {"record":item, "catalog":{}, "reused":False}
        execute_member(source_id=source, mode="normal", cycle_id=cycle, workspace=root/source,
            pin_store=FilePinStore(root), discover=discover,
            retrieve=lambda *_: (_ for _ in ()).throw(AssertionError("unexpected retrieval")),
            build=build, publish=publish, record=lambda r, _: records.append(r))
        # Resume and replay use the original pin even if discovery now advertises newer truth.
        for mode in ("resume", "replay"):
            execute_member(source_id=source, mode=mode, cycle_id=cycle, workspace=root/f"{source}-{mode}",
                pin_store=FilePinStore(root),
                discover=lambda: (_ for _ in ()).throw(AssertionError("rediscovered")),
                retrieve=lambda url, path: path.write_bytes(path.name.encode()), build=build,
                publish=publish, record=lambda *_: None)

assert discoveries == ["census_bps", "census_bps_provisional"]
assert {r["source_id"] for r in records} == {"census_bps", "census_bps_provisional"}
assert records[0]["provider_release_id"] != records[1]["provider_release_id"]
registry = json.loads(Path("config/monthly_source_execution_registry.json").read_text())
required = required_sources(registry)
assert required[-2:] == ("census_bps", "census_bps_provisional")
base = [result(source, cycle, "other") for source in required[:-2]]
one = resume_plan(required, [*base, result("census_bps", cycle, "compiled-release")], expected_cycle_id=cycle)
assert one["run"] == ["census_bps_provisional"]
both = resume_plan(required, [*base, result("census_bps", cycle, "compiled-release"),
    result("census_bps_provisional", cycle, "different-provisional-release")], expected_cycle_id=cycle)
assert both["run"] == []
print("Smoke 196 hosted BPS registration passed")
