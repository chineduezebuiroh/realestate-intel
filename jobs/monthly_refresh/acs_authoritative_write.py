"""ACS-facing writes fenced by governed main, not just a destination blob CAS.

Only trusted adapters call this after independent reconstruction. Immutable Git
objects may be orphaned on conflict; no authoritative ref is advanced on conflict.
Generic GitHub CAS and non-ACS publication behavior remain unchanged.
"""
import base64
import hashlib
import json
import re
import time
import urllib.parse
from copy import deepcopy

from core.source_artifacts.hashing import canonical_json_bytes
from core.source_artifacts.publication import PublicationError, TransientPublicationError


def commit_identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise PublicationError("ACS commit identity unavailable")
    return value


def read_at_commit(api, path, commit, *, allow_missing=False):
    """Bounded read retries only; a contradictory response is never retried."""
    commit_identity(commit)
    encoded = urllib.parse.quote(path, safe="/")
    for attempt in range(4):
        try:
            item, _ = api.request("GET", f"/contents/{encoded}?ref={commit}", expected=(200, 404))
        except TransientPublicationError:
            if attempt == 3:
                raise
        else:
            if item is not None:
                raw = base64.b64decode(item["content"], validate=False)
                blob = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
                if item.get("sha") != blob:
                    raise PublicationError("ACS commit-bound blob identity mismatch")
                return json.loads(raw), blob
            if attempt == 3:
                if allow_missing:
                    return None, None
                raise PublicationError("ACS commit-bound content unavailable")
        time.sleep(0.1 * 2 ** attempt)
    raise AssertionError("unreachable")


def verify_at_commit(api, path, commit, value):
    persisted, blob = read_at_commit(api, path, commit)
    raw = canonical_json_bytes(value)
    expected_blob = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
    if persisted != value or blob != expected_blob:
        raise PublicationError("ACS commit-bound content mismatch")
    return blob


def write_current(*, api, branch, path, value, expected_sha, message, governance):
    governance.require_destination(api.repository)
    if branch != "main":
        raise ValueError("authoritative ACS writes require governed main")
    tip = commit_identity(governance.current_commit(api.repository))
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
    written = commit_identity(proposed.get("sha"))
    event = {"path": path, "commit_sha": written, "state": "ref_update_uncertain"}
    if hasattr(governance, "write_events"):
        governance.write_events.append(event)
    try:
        response, _ = api.request("PATCH", "/git/refs/heads/main", payload={"sha": written, "force": False})
    except TransientPublicationError:
        # Never repeat a possibly successful mutation. Only an exact durable
        # head establishes this outcome; a further advance requires review.
        for attempt in range(4):
            try:
                response, _ = api.request("GET", "/git/ref/heads/main")
                break
            except TransientPublicationError:
                if attempt == 3:
                    raise
                time.sleep(0.1 * 2 ** attempt)
        if (response or {}).get("object", {}).get("sha") != written:
            raise PublicationError("ACS ref update outcome uncertain; reconcile before retry") from None
        verify_at_commit(api, path, written, value)
    else:
        if (response or {}).get("object", {}).get("sha") != written:
            raise PublicationError("ACS ref-update identity mismatch")
    event["state"] = "ref_update_confirmed"
    return written


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
