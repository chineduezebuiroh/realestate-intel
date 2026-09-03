"""Smoke 195: durable pre-execution provider pins and policy-driven inventory."""
from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path

from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh.cohort import required_sources
from jobs.monthly_refresh.source_inputs import (FilePinStore, add_pin,
    discover_persist_execute, provider_pin, verify_member_bytes)


def make_pin(root: Path, release: str) -> tuple[dict, Path]:
    raw = root / f"provider-{release}.bin"; raw.write_bytes(release.encode())
    pin = provider_pin(cycle_id="cycle_fixture", source_id="fixture_source",
        provider_release_id=release, members={"raw": {
            "url": f"https://provider.invalid/{release}",
            "retrieved_at": "2026-09-02T00:00:00Z",
            "sha256": hashlib.sha256(raw.read_bytes()).hexdigest()}})
    return pin, raw


with tempfile.TemporaryDirectory() as value:
    root = Path(value); store = FilePinStore(root)
    pin_a, raw_a = make_pin(root, "A"); pin_b, _ = make_pin(root, "B")
    events = []
    result = discover_persist_execute(mode="normal", store=store,
        cycle_id="cycle_fixture", source_id="fixture_source", required_members={"raw"},
        discover_and_retrieve=lambda: events.append("discover-A") or pin_a,
        execute=lambda pin: events.append("execute-" + pin["provider_release_id"]) or "ok")
    assert result == "ok" and events == ["discover-A", "execute-A"]
    assert store.get("cycle_fixture", "fixture_source") == pin_a

    # A new store instance models a later job/process. Provider B may now be
    # latest, but both resume and replay must execute historical pin A.
    for mode in ("resume", "replay"):
        later = FilePinStore(root); calls = []
        discover_persist_execute(mode=mode, store=later, cycle_id="cycle_fixture",
            source_id="fixture_source", required_members={"raw"},
            discover_and_retrieve=lambda: calls.append("LATEST-B") or pin_b,
            execute=lambda pin: calls.append(pin["provider_release_id"]))
        assert calls == ["A"]

    # A failure after the durable commit leaves the pin available to resume.
    other_root = root / "failed"; failed_store = FilePinStore(other_root)
    try:
        discover_persist_execute(mode="normal", store=failed_store,
            cycle_id="cycle_fixture", source_id="fixture_source", required_members={"raw"},
            discover_and_retrieve=lambda: pin_a,
            execute=lambda _: (_ for _ in ()).throw(RuntimeError("candidate failed")))
    except RuntimeError as exc: assert str(exc) == "candidate failed"
    else: raise AssertionError("failed execution succeeded")
    calls = []
    discover_persist_execute(mode="resume", store=FilePinStore(other_root),
        cycle_id="cycle_fixture", source_id="fixture_source", required_members={"raw"},
        discover_and_retrieve=lambda: calls.append("rediscovered") or pin_b,
        execute=lambda pin: calls.append(pin["provider_release_id"]))
    assert calls == ["A"]

    try: add_pin(pin_a, pin_b)
    except IdentityCollisionError: pass
    else: raise AssertionError("contradictory immutable pin was accepted")
    raw_a.write_bytes(b"changed")
    try: verify_member_bytes(pin_a, {"raw": raw_a})
    except ValueError as exc: assert "mismatch" in str(exc)
    else: raise AssertionError("provider byte mismatch was accepted")

    class NonDurable:
        def get(self, *_): return None
        def put(self, pin): return pin, True
    try:
        discover_persist_execute(mode="normal", store=NonDurable(),
            cycle_id="cycle_fixture", source_id="fixture_source", required_members={"raw"},
            discover_and_retrieve=lambda: pin_a, execute=lambda _: None)
    except RuntimeError as exc: assert "before durable pin" in str(exc)
    else: raise AssertionError("execution began without durable persistence")

policy = json.loads(Path("config/monthly_source_execution_registry.json").read_text())
assert required_sources(policy) == ("redfin", "fred_macro", "ces", "laus")
by_id = {item["source_id"]: item for item in policy["members"]}
for source in ("fred_macro", "ces", "laus"):
    assert by_id[source]["provider_input_policy"] == "legacy_candidate_evidence"
for source in ("census_bps", "census_bps_provisional"):
    assert not by_id[source]["hosted_cohort_enabled"]
    assert by_id[source]["provider_input_policy"] == "durable_raw_input_pin"
assert by_id["census_bps_provisional"]["dependencies"] == []
print("Smoke 195 durable source-input lifecycle passed")
