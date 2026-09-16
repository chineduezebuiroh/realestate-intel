"""Hosted common-lifecycle runner for one independently governed BEA GDP source."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Mapping

from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS
from core.source_artifacts.hashing import write_canonical_json
from jobs.monthly_refresh.acs_hosted import execute_product
from jobs.monthly_refresh.bea_monthly import candidate, discover_pin, retrieve_pinned_snapshot
from jobs.monthly_refresh.bps_hosted import CATALOG_PATH, publish_candidate
from jobs.monthly_refresh.cycle_results import GitHubCycleResultStore, governed_record
from jobs.monthly_refresh.source_inputs import GitHubPinStore
from sources.bea.artifact import PRODUCTS


def main() -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--source-id", choices=sorted(PRODUCTS), required=True)
    parser.add_argument("--mode", choices=("normal", "resume", "replay"), required=True)
    parser.add_argument("--cycle-id", required=True); parser.add_argument("--repository", required=True)
    parser.add_argument("--branch", required=True); parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True); args = parser.parse_args()
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", "")); cas = GitHubCatalogCAS(api, CATALOG_PATH, args.branch)
    policy = json.loads(Path("config/monthly_refresh_policy.json").read_text()); results = GitHubCycleResultStore(api, args.branch)
    def publish(path: Path, source: str) -> Mapping[str, Any]:
        return publish_candidate(artifact=path, source_id=source, api=api, cas=cas,
            workspace=args.workspace / "publication", git_sha=os.environ.get("GITHUB_SHA", "unknown"), logical_source_id=source)
    value = execute_product(source_id=args.source_id, mode=args.mode, cycle_id=args.cycle_id,
        workspace=args.workspace, pin_store=GitHubPinStore(api, args.branch),
        discover=lambda: discover_pin(cycle_id=args.cycle_id, source_id=args.source_id, workspace=args.workspace / "discovery"),
        retrieve=retrieve_pinned_snapshot, build=candidate, publish=publish,
        record=lambda result, catalog: results.put(governed_record(result, policy, catalog)))
    write_canonical_json(args.output, value["result"]); print(json.dumps(value["result"], sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
