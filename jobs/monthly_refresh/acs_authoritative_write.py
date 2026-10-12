"""ACS-facing writes fenced by governed main, not just a destination blob CAS.

Only trusted adapters call this after independent reconstruction. Immutable Git
objects may be orphaned on conflict; no authoritative ref is advanced on conflict.
Generic GitHub CAS and non-ACS publication behavior remain unchanged.
"""
import base64
import urllib.parse
from copy import deepcopy

from core.source_artifacts.hashing import canonical_json_bytes
from core.source_artifacts.publication import PublicationError


def write_current(*, api, branch, path, value, expected_sha, message, governance):
    governance.require_destination(api.repository)
    if branch != "main":
        raise ValueError("authoritative ACS writes require governed main")
    tip = governance.current_commit(api.repository)
    encoded = urllib.parse.quote(path, safe="/")
    item, _ = api.request("GET", f"/contents/{encoded}?ref={tip}", expected=(200, 404))
    if (item["sha"] if item else None) != expected_sha:
        raise PublicationError("ACS authoritative destination changed; reconstruct/retry")
    commit, _ = api.request("GET", f"/git/commits/{tip}")
    if commit.get("sha") != tip:
        raise PublicationError("ACS authoritative base commit mismatch")
    tree, _ = api.request("POST", "/git/trees", payload={"base_tree": commit["tree"]["sha"],
        "tree": [{"path": path, "mode": "100644", "type": "blob", "content": canonical_json_bytes(value).decode()}]}, expected=(201,))
    proposed, _ = api.request("POST", "/git/commits", payload={"message": message,
        "tree": tree["sha"], "parents": [tip]}, expected=(201,))
    # A concurrent governance OR unrelated main advance makes this sibling
    # commit non-fast-forward. Never rebase/retry without checking governance.
    api.request("PATCH", "/git/refs/heads/main", payload={"sha": proposed["sha"], "force": False})


class CurrentCatalog:
    """Fence ACS-related immutable catalog publication without changing generic CAS."""
    def __init__(self, store, governance):
        governance.require_destination(store.api.repository)
        if store.branch != "main":
            raise ValueError("ACS catalog publication requires main")
        self.store, self.governance = store, governance

    def read(self): return self.store.read()

    def add(self, record, receipt):
        from core.source_artifacts.catalog import add_record, validate_catalog_namespace
        catalog, oid = self.store.read()
        updated = add_record(catalog, record, receipt)
        self.governance.current_commit(self.store.api.repository)
        if updated == catalog: return catalog, False
        updated = deepcopy(updated)
        updated["compare_and_swap"]["expected_git_blob_sha"] = oid
        validate_catalog_namespace(updated, fixture=self.store.fixture)
        write_current(api=self.store.api, branch=self.store.branch, path=self.store.path,
            value=updated, expected_sha=oid, message=f"Catalog ACS-bound artifact {record['object_id']}",
            governance=self.governance)
        return updated, True
