"""ACS governance boundary. Production authority is read from main, never a proposal.

No writer/activation adapter is provided here. The registry must be approved and
advanced by a separately governed control-plane operation.
"""
from __future__ import annotations

import base64
import json
import re
from copy import deepcopy
from pathlib import Path
from weakref import WeakKeyDictionary, WeakSet

from core.source_artifacts.github_release import GitHubAPI as _ProductionAPI

from core.source_artifacts.hashing import sha256_json
from sources.census_acs.artifact import PRODUCTS, governed_config_hashes

SCOPE_VERSION = "acs_historical_scope_authority_v1"
GAP_VERSION = "acs_governed_gap_evidence_v1"
REGISTRY_PATH = "config/acs_historical_governance.json"
_TOKEN = object()
_PRODUCTION = WeakKeyDictionary()
_HISTORICAL = WeakSet()
_READERS = WeakKeyDictionary()


def semantic_id(kind: str, value: dict, identity_field: str) -> str:
    return kind + "__" + sha256_json({k: v for k, v in value.items() if k != identity_field})


def identify(kind: str, value: dict, identity_field: str) -> dict:
    """Create a proposal identity; hashing does NOT confer governance approval."""
    return {**value, identity_field: semantic_id(kind, value, identity_field)}


def check_identity(kind: str, value: dict, field: str) -> None:
    if value.get(field) != semantic_id(kind, value, field):
        raise ValueError(f"{kind} semantic identity mismatch")


class ACSGovernance:
    """A control-plane snapshot, with explicitly separate fixture provenance."""
    def __init__(self, registry: dict, *, provenance: str, token: object):
        if token is not _TOKEN:
            raise ValueError("raw dictionaries are not trusted ACS governance")
        self._registry = deepcopy(registry)
        self.provenance = provenance
        self.namespace = registry.get("namespace")
        if registry.get("schema_version") != "acs_historical_governance_registry_v1":
            raise ValueError("ACS governance registry schema mismatch")
        if self.namespace not in {"production", "test_only"}:
            raise ValueError("ACS governance namespace mismatch")
        if (self.namespace == "production") != provenance.startswith("main:"):
            raise ValueError("ACS production governance must originate on main")
        self._validate_states()

    @classmethod
    def from_fixture(cls, registry: dict) -> ACSGovernance:
        if registry.get("namespace") != "test_only":
            raise ValueError("fixture authority must be explicitly test_only")
        return cls(registry, provenance="test_only", token=_TOKEN)

    @classmethod
    def from_main(cls, api) -> ACSGovernance:
        raise ValueError("injectable transports cannot establish production ACS governance; use load_production")

    @classmethod
    def load_production(cls, repository: str, token: str, *, snapshot: dict | None = None) -> ACSGovernance:
        if cls is not ACSGovernance or type(repository) is not str or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("production ACS governance requires a repository identity, not a transport")
        # Construct our own read-only transport: no caller-supplied opener/API.
        api = _ProductionAPI(repository, token, read_only=True)
        commit, _ = api.request("GET", "/commits/main")
        sha = commit.get("sha", "")
        if not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise ValueError("invalid main governance commit")
        if snapshot is not None:
            if snapshot.get("repository") != repository:
                raise ValueError("ACS governance repository mismatch")
            pinned = snapshot.get("commit", "")
            if not re.fullmatch(r"[0-9a-f]{40}", pinned):
                raise ValueError("invalid pinned ACS governance commit")
            comparison, _ = api.request("GET", f"/compare/{pinned}...{sha}")
            if comparison.get("merge_base_commit", {}).get("sha") != pinned:
                raise ValueError("ACS governance snapshot is not governed main history")
            sha = pinned
        item, _ = api.request("GET", f"/contents/{REGISTRY_PATH}?ref={sha}", expected=(200, 404))
        if item is None:
            raise ValueError("approved ACS historical governance is absent on main")
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("ambiguous ACS governance metadata")
                result[key] = value
            return result
        registry = json.loads(base64.b64decode(item["content"]), object_pairs_hook=unique)
        if registry.get("namespace") != "production":
            raise ValueError("remote ACS governance must be production")
        result = cls(registry, provenance="main:" + sha, token=_TOKEN)
        identity = {"repository": repository, "commit": sha, "registry_sha256": sha256_json(registry)}
        if snapshot is not None and snapshot != identity:
            raise ValueError("ACS governance snapshot content mismatch")
        _PRODUCTION[result] = identity
        _READERS[result] = api
        if snapshot is not None:
            _HISTORICAL.add(result)
        return result

    def production_snapshot(self) -> dict:
        self.require_production(_recovery=True)
        return deepcopy(_PRODUCTION[self])

    def _validate_states(self) -> None:
        states = self._registry.get("states", {})
        pointers = self._registry.get("current_state", {})
        if not isinstance(states, dict) or not isinstance(pointers, dict):
            raise ValueError("ambiguous ACS current state metadata")
        products = set()
        for key, state in states.items():
            check_identity("acs_physical_historical_state_v2", state, "state_id")
            if key != state.get("state_id"):
                raise ValueError("ACS state registry key differs from semantic state_id")
            source = state.get("inventory", {}).get("source_id")
            if source not in PRODUCTS:
                raise ValueError("governed historical state product mismatch")
            inventory = state.get("inventory", {})
            check_identity("acs_historical_inventory_v2", inventory, "inventory_id")
            if (state.get("schema_version") != "acs_physical_historical_state_v2"
                    or inventory.get("schema_version") != "acs_historical_inventory_v2"
                    or inventory.get("namespace") != self.namespace):
                raise ValueError("invalid governed ACS state/inventory metadata")
            parents = inventory.get("parents", [])
            if (not parents or len({p["vintage"] for p in parents}) != len(parents)
                    or any(p.get("source_id") != source for p in parents)):
                raise ValueError("ambiguous governed ACS state acquisition metadata")
            products.add(source)
        if products - set(pointers):
            raise ValueError("governed historical states require a current-state pointer")
        for source, key in pointers.items():
            if not isinstance(key, str) or key not in states:
                raise ValueError("current pointer references nonexistent or ambiguous state")
            if states[key].get("inventory", {}).get("source_id") != source:
                raise ValueError("governed current historical state product mismatch")
            current_parents = {p["vintage"]: p for p in states[key]["inventory"]["parents"]}
            for state in states.values():
                if state["inventory"]["source_id"] == source and any(
                        current_parents.get(p["vintage"]) != p for p in state["inventory"]["parents"]):
                    raise ValueError("current ACS pointer is stale or violates immutable history retention")

    def scope(self, source: str, authority_id: str, root: Path) -> dict:
        if source not in PRODUCTS or self._registry.get("current_scope", {}).get(source) != authority_id:
            raise ValueError("scope authority is absent, superseded or outdated")
        authority = deepcopy(self._registry.get("scopes", {}).get(authority_id, {}))
        check_identity(SCOPE_VERSION, authority, "authority_id")
        if (authority.get("schema_version") != SCOPE_VERSION or authority.get("source_id") != source
                or authority.get("product") != PRODUCTS[source] or authority.get("namespace") != self.namespace
                or not authority.get("approval_provenance")):
            raise ValueError("invalid approved ACS scope authority")
        if authority.get("config_hashes") != governed_config_hashes(root):
            raise ValueError("ACS scope geography/metric configuration drift")
        start, end = authority["historical_range"]
        available = authority["expected_available_vintages"]
        gaps = authority["governed_gaps"]
        if (not isinstance(start, int) or not isinstance(end, int) or start > end
                or available != sorted(set(available)) or not available):
            raise ValueError("invalid historical range/vintage inventory")
        gap_years = []
        for gap in gaps:
            evidence = deepcopy(self._registry.get("gap_evidence", {}).get(gap["evidence_id"], {}))
            # Membership in main's approved evidence registry, rather than a
            # self-consistent hash on caller data, is the trust boundary.
            check_identity(GAP_VERSION, evidence, "evidence_id")
            if (evidence.get("schema_version") != GAP_VERSION or evidence.get("source_id") != source
                    or evidence.get("vintage") != gap["vintage"] or evidence.get("namespace") != self.namespace
                    or not evidence.get("approval_provenance") or not evidence.get("provider_evidence")):
                raise ValueError("unapproved governed gap evidence")
            gap_years.append(gap["vintage"])
        if (len(set(gap_years)) != len(gap_years) or set(gap_years) & set(available)
                or set(available) | set(gap_years) != set(range(start, end + 1))):
            raise ValueError("scope does not exactly partition approved historical range")
        return authority

    def compatibility_authorization(self, authorization_id: str) -> dict:
        """Only current reviewed-main membership can confer compatibility authority."""
        self.require_production()
        if self._registry.get("current_compatibility_authorization") != authorization_id:
            raise ValueError("compatibility authorization absent or superseded")
        record = deepcopy(self._registry.get("compatibility_authorizations", {}).get(authorization_id, {}))
        check_identity("acs_2024_compatibility_authorization_v1", record, "authorization_id")
        if record.get("authorization_id") != authorization_id:
            raise ValueError("compatibility authorization registry key mismatch")
        return record

    def current_state(self, source: str) -> dict | None:
        self._validate_states()
        state_id = self._registry.get("current_state", {}).get(source)
        if state_id is None:
            return None
        state = deepcopy(self._registry.get("states", {}).get(state_id, {}))
        check_identity("acs_physical_historical_state_v2", state, "state_id")
        if state.get("inventory", {}).get("source_id") != source:
            raise ValueError("governed current historical state product mismatch")
        return state

    def require_production(self, *, _recovery=False) -> None:
        if (type(self) is not ACSGovernance or self not in _PRODUCTION or self.namespace != "production"
                or sha256_json(self._registry) != _PRODUCTION[self]["registry_sha256"]):
            raise ValueError("test_only ACS governance cannot authorize production")
        if self in _HISTORICAL and not _recovery:
            raise ValueError("pinned ACS governance is recovery-only; current main required")

    def require_destination(self, repository: str, *, recovery=False) -> None:
        self.require_production(_recovery=recovery)
        if repository != _PRODUCTION[self]["repository"]:
            raise ValueError("ACS governance destination repository mismatch")

    def current_commit(self, repository: str) -> str:
        """Read current main through the constrained loader transport.

        Unrelated commits are allowed; any governance-content advance requires
        reconstruction. The returned tip must be the parent of an atomic write.
        """
        self.require_destination(repository)
        api = _READERS[self]
        commit, _ = api.request("GET", "/commits/main")
        sha = commit.get("sha", "")
        if not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise ValueError("invalid current main governance commit")
        item, _ = api.request("GET", f"/contents/{REGISTRY_PATH}?ref={sha}", expected=(200, 404))
        if item is None or sha256_json(json.loads(base64.b64decode(item["content"]))) != _PRODUCTION[self]["registry_sha256"]:
            raise ValueError("ACS production governance superseded; reconstruct against current main")
        return sha
