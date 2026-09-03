"""Immutable provider-input pins shared by governed monthly source adapters."""
from __future__ import annotations

import hashlib
import base64
import json
import urllib.parse
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from core.source_artifacts.hashing import sha256_json, write_canonical_json
from core.source_artifacts.publication import (IdentityCollisionError, PublicationError,
                                               TransientPublicationError)
from core.source_artifacts.github_release import GitHubAPI

PIN_VERSION = "monthly_source_input_pin_v1"
PIN_ROOT = "config/monthly_source_input_pins"


def provider_pin(*, cycle_id: str, source_id: str, provider_release_id: str,
                 members: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """Build a complete, ordering-independent pin after provider bytes are retrieved."""
    if not cycle_id or not source_id or not provider_release_id or not members:
        raise ValueError("provider pin identity is incomplete")
    normalized: dict[str, dict[str, Any]] = {}
    for name, raw in sorted(members.items()):
        item = dict(raw)
        if set(item) < {"url", "retrieved_at", "sha256"}:
            raise ValueError(f"provider member pin is incomplete: {name}")
        if len(str(item["sha256"])) != 64:
            raise ValueError(f"provider member SHA-256 is invalid: {name}")
        normalized[name] = item
    semantic = {"cycle_id": cycle_id, "source_id": source_id,
                "provider_release_id": provider_release_id, "members": normalized}
    return {"schema_version": PIN_VERSION, **semantic,
            "pin_id": f"source_input__{source_id}__{sha256_json(semantic)[:20]}"}


def validate_pin(pin: Mapping[str, Any], *, cycle_id: str, source_id: str,
                 required_members: set[str]) -> dict[str, Any]:
    value = dict(pin)
    if value.get("schema_version") != PIN_VERSION:
        raise ValueError("source-input pin contract mismatch")
    if value.get("cycle_id") != cycle_id or value.get("source_id") != source_id:
        raise ValueError("source-input pin identity mismatch")
    if set(value.get("members", {})) != required_members:
        raise ValueError("source-input pin member inventory mismatch")
    rebuilt = provider_pin(cycle_id=cycle_id, source_id=source_id,
                           provider_release_id=value.get("provider_release_id"),
                           members=value["members"])
    if rebuilt != value:
        raise ValueError("source-input pin content identity mismatch")
    return value


def add_pin(existing: Mapping[str, Any] | None, proposed: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    """Create once; exact repeats are no-ops and contradictory bytes collide."""
    if existing is None:
        return dict(proposed), True
    if dict(existing) == dict(proposed):
        return dict(existing), False
    raise IdentityCollisionError(
        f"provider-input pin collision for ({proposed.get('cycle_id')}, {proposed.get('source_id')})")


def pinned_or_discover(*, mode: str, existing: Mapping[str, Any] | None,
                       discover_and_retrieve: Callable[[], Mapping[str, Any]],
                       cycle_id: str, source_id: str, required_members: set[str]) -> tuple[dict[str, Any], bool]:
    """Enforce discovery exactly once: resume/replay can only consume a recorded pin."""
    if mode not in {"normal", "resume", "replay"}:
        raise ValueError("mode must be normal, resume, or replay")
    if existing is not None:
        return validate_pin(existing, cycle_id=cycle_id, source_id=source_id,
                            required_members=required_members), False
    if mode != "normal":
        raise ValueError(f"{mode} requires an existing immutable provider-input pin")
    discovered = discover_and_retrieve()
    return validate_pin(discovered, cycle_id=cycle_id, source_id=source_id,
                        required_members=required_members), True


def verify_member_bytes(pin: Mapping[str, Any], paths: Mapping[str, Path]) -> None:
    if set(paths) != set(pin["members"]):
        raise ValueError("retrieved provider member inventory differs from pin")
    for name, path in paths.items():
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != pin["members"][name]["sha256"]:
            raise ValueError(f"raw/provider pin mismatch: {name}")


def write_pin(path: Path, pin: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        current = json.loads(path.read_text())
        value, changed = add_pin(current, pin)
        if not changed:
            return
    else:
        value, _ = add_pin(None, pin)
    write_canonical_json(path, value)


def pin_path(cycle_id: str, source_id: str) -> str:
    """Return the durable Contents path for one cycle/source authority."""
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    if not cycle_id or not source_id or set(cycle_id) - allowed or set(source_id) - allowed:
        raise ValueError("invalid provider-input pin path identity")
    return f"{PIN_ROOT}/{cycle_id}/{source_id}.json"


class FilePinStore:
    """Filesystem implementation used for cross-process contract tests."""
    def __init__(self, root: Path):
        self.root = root

    def get(self, cycle_id: str, source_id: str) -> dict[str, Any] | None:
        path = self.root / pin_path(cycle_id, source_id)
        return json.loads(path.read_text()) if path.is_file() else None

    def put(self, pin: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        path = self.root / pin_path(str(pin["cycle_id"]), str(pin["source_id"]))
        current = json.loads(path.read_text()) if path.is_file() else None
        value, changed = add_pin(current, pin)
        if changed:
            write_pin(path, value)
        return value, changed


class GitHubPinStore:
    """Immutable GitHub Contents storage, matching durable cycle-result state."""
    def __init__(self, api: GitHubAPI, branch: str, *, attempts: int = 4):
        self.api, self.branch, self.attempts = api, branch, attempts

    def _read(self, cycle_id: str, source_id: str) -> tuple[dict[str, Any] | None, str | None]:
        path = pin_path(cycle_id, source_id)
        encoded = urllib.parse.quote(path, safe="/")
        item, _ = self.api.request("GET", f"/contents/{encoded}?ref={urllib.parse.quote(self.branch)}",
                                   expected=(200, 404))
        if item is None:
            return None, None
        return json.loads(base64.b64decode(item["content"])), item["sha"]

    def get(self, cycle_id: str, source_id: str) -> dict[str, Any] | None:
        return self._read(cycle_id, source_id)[0]

    def put(self, pin: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        cycle_id, source_id = str(pin["cycle_id"]), str(pin["source_id"])
        for attempt in range(self.attempts):
            existing, oid = self._read(cycle_id, source_id)
            value, changed = add_pin(existing, pin)
            if not changed:
                return value, False
            payload = {"message": f"Pin {source_id} provider input for {cycle_id}",
                       "content": base64.b64encode(
                           json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
                       ).decode(), "branch": self.branch}
            if oid is not None:
                payload["sha"] = oid
            encoded = urllib.parse.quote(pin_path(cycle_id, source_id), safe="/")
            try:
                self.api.request("PUT", f"/contents/{encoded}", payload=payload, expected=(200, 201))
                persisted, _ = self._read(cycle_id, source_id)
                if persisted != value:
                    raise RuntimeError("durable provider-input pin read-after-write mismatch")
                return value, True
            except TransientPublicationError:
                if attempt + 1 == self.attempts:
                    raise
            except PublicationError:
                if attempt + 1 == self.attempts:
                    raise PublicationError("provider-input pin compare-and-swap retries exhausted")
            time.sleep(0.1 * (attempt + 1))
        raise AssertionError("unreachable")


def discover_persist_execute(*, mode: str, store: Any, cycle_id: str, source_id: str,
                             required_members: set[str], discover_and_retrieve: Callable[[], Mapping[str, Any]],
                             execute: Callable[[Mapping[str, Any]], Any]) -> Any:
    """Run the hosted lifecycle with a durable commit point before execution."""
    existing = store.get(cycle_id, source_id)
    pin, discovered = pinned_or_discover(mode=mode, existing=existing,
        discover_and_retrieve=discover_and_retrieve, cycle_id=cycle_id,
        source_id=source_id, required_members=required_members)
    if discovered:
        store.put(pin)
    durable = store.get(cycle_id, source_id)
    if durable is None:
        raise RuntimeError("provider-dependent execution attempted before durable pin persistence")
    durable = validate_pin(durable, cycle_id=cycle_id, source_id=source_id,
                           required_members=required_members)
    if durable != pin:
        raise IdentityCollisionError("durable provider-input pin contradicts selected input")
    return execute(durable)
