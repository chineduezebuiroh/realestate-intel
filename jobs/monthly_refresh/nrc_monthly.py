"""Direct Census NRC acquisition, exact-byte pinning, and candidate construction."""
from __future__ import annotations

import base64
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

import pandas as pd
import requests

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import sha256_file
from jobs.monthly_refresh.source_inputs import provider_pin, verify_member_bytes
from sources.census_nrc.parser import (CENSUS_INPUTS, PARSER_CONTRACT_VERSION, SOURCE_ID,
                                       parse_census_workbook)

MEMBERS = frozenset(CENSUS_INPUTS)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def retrieve(url: str, *, session: Any = None) -> tuple[bytes, dict[str, Any]]:
    response = (session or requests.Session()).get(url, timeout=180)
    response.raise_for_status(); payload = bytes(response.content)
    content_type = str(response.headers.get("content-type", "")).lower()
    if not payload.startswith(b"PK") or not ("spreadsheetml" in content_type or "octet-stream" in content_type):
        raise ValueError(f"Census NRC endpoint returned non-XLSX content: {content_type!r}")
    return payload, {"status_code": int(response.status_code), "content_type": content_type}


def discover_pin(*, cycle_id: str, workspace: Path,
                 acquire: Callable[[str], tuple[bytes, Mapping[str, Any]]] = retrieve,
                 retrieved_at: str | None = None) -> tuple[dict[str, Any], dict[str, Path]]:
    """Resolve both mutable routes once and embed their exact bytes in the durable pin."""
    workspace.mkdir(parents=True, exist_ok=True); members = {}; paths = {}; stamp = retrieved_at or _now()
    for kind, (url, _metric) in CENSUS_INPUTS.items():
        payload, transport = acquire(url)
        # Parse before pinning so malformed transport can never become governed input.
        _rows, diagnostics = parse_census_workbook(payload, kind)
        digest = hashlib.sha256(payload).hexdigest(); path = workspace / f"{kind}.xlsx"
        path.write_bytes(payload); paths[kind] = path
        members[kind] = {"url": url, "retrieved_at": stamp, "sha256": digest,
            "size_bytes": len(payload), "content_base64": base64.b64encode(payload).decode("ascii"),
            "transport": dict(transport), "evidence": diagnostics}
    release = "nrc-workbooks:" + hashlib.sha256(
        "\n".join(f"{kind}:{members[kind]['sha256']}" for kind in sorted(members)).encode()).hexdigest()
    return provider_pin(cycle_id=cycle_id, source_id=SOURCE_ID,
        provider_release_id=release, members=members), paths


def recover_pinned_workbooks(pin: Mapping[str, Any], workspace: Path) -> dict[str, Path]:
    if set(pin.get("members", {})) != MEMBERS: raise ValueError("NRC pin must contain both workbooks")
    workspace.mkdir(parents=True, exist_ok=True); paths = {}
    for kind in sorted(MEMBERS):
        member = pin["members"][kind]
        try: payload = base64.b64decode(member["content_base64"], validate=True)
        except Exception as exc: raise ValueError(f"NRC pinned workbook encoding is invalid: {kind}") from exc
        if len(payload) != member.get("size_bytes") or hashlib.sha256(payload).hexdigest() != member["sha256"]:
            raise ValueError(f"NRC durable pinned workbook hash mismatch: {kind}")
        path = workspace / f"{kind}.xlsx"; path.write_bytes(payload); paths[kind] = path
    return paths


def candidate(*, pin: Mapping[str, Any], paths: Mapping[str, Path], output: Path,
              cycle_id: str, git_sha: str = "unknown", repository_root: Path = Path(".")) -> dict[str, Any]:
    verify_member_bytes(pin, paths); rows = []; workbooks = {}
    for kind in sorted(MEMBERS):
        parsed, diagnostics = parse_census_workbook(paths[kind].read_bytes(), kind)
        rows.extend(parsed); workbooks[kind] = diagnostics
    frame = pd.DataFrame(rows).drop(columns=["provider", "native_id"]).assign(source_id=SOURCE_ID)
    if set(frame.geo_id) != {"us_nation", "us_region_northeast", "us_region_midwest",
                            "us_region_south", "us_region_west"}:
        raise ValueError("NRC candidate geography reconciliation failed")
    target = str(frame.date.max())[:7]
    semantic_input = hashlib.sha256("\n".join(
        f"{kind}:{pin['members'][kind]['sha256']}" for kind in sorted(MEMBERS)).encode()).hexdigest()
    evidence = {"schema_version": "census_nrc_candidate_evidence_v1", "cycle_id": cycle_id,
        "physical_source_id": SOURCE_ID, "provider_pin_id": pin["pin_id"],
        "input_members": [{"kind": k, "url": pin["members"][k]["url"],
                           "sha256": pin["members"][k]["sha256"],
                           "size_bytes": pin["members"][k]["size_bytes"]} for k in sorted(MEMBERS)],
        "parser_contract_version": PARSER_CONTRACT_VERSION, "workbooks": workbooks}
    parser_path = repository_root / "sources/census_nrc/parser.py"
    manifest = create_artifact(output, frame, source_id=SOURCE_ID, source_family=SOURCE_ID,
        source_type="government_survey", provider="U.S. Census Bureau",
        distribution_channel="census_nrc_historical_workbooks",
        provider_release_id=str(pin["provider_release_id"]), provider_release_timestamp_or_date=None,
        retrieved_at=max(pin["members"][k]["retrieved_at"] for k in MEMBERS), target_month=target,
        source_request_identity=semantic_input,
        source_urls_or_endpoint_identity=[CENSUS_INPUTS[k][0] for k in sorted(MEMBERS)],
        raw_source_lineage=evidence, config_hashes={"nrc_parser_sha256": sha256_file(parser_path)},
        git_sha=git_sha, source_contract_version=PARSER_CONTRACT_VERSION)
    return {"manifest": manifest, "evidence": evidence}
