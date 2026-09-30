"""C2 cycle-scoped selection and post-success annual satisfaction commands."""
from __future__ import annotations

import argparse
import json
import os
import base64
from urllib.parse import quote
from datetime import date
from pathlib import Path

from core.source_artifacts.github_release import GitHubAPI
from core.source_artifacts.hashing import write_canonical_json
from jobs.monthly_refresh.laus_annual_processing import (
    GitHubAnnualSatisfactionStore, annual_vintage_id, satisfaction_path,
    satisfaction_record, select_routine, validate_satisfaction)
from jobs.monthly_refresh.cycle_results import _validate_record_invariants, record_path
from jobs.monthly_refresh.production import validate_source_result


def load_satisfactions(root: Path) -> list[dict]:
    return [json.loads(path.read_text()) for path in sorted(root.glob("*.json"))]


def cycle_date(target_month: str) -> date:
    try:
        year, month = map(int, target_month.split("-"))
        return date(year, month, 1)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid governed LAUS target month") from exc


def satisfaction_from_cycle_record(*, record: dict, target_month: str,
                                   invocation_mode: str,
                                   existing: dict | None = None) -> tuple[dict | None, bool]:
    """Derive satisfaction only from mode evidence bound into a durable cycle result."""
    if invocation_mode == "replay":
        raise ValueError("replay cannot reconcile LAUS annual satisfaction")
    when = cycle_date(target_month); vintage = annual_vintage_id(when.year)
    _validate_record_invariants(record)
    result = validate_source_result(dict(record["result"]), expected_cycle_id=record["cycle_id"])
    evidence = record.get("source_evidence")
    if record.get("source_id") != "laus" or not isinstance(evidence, dict):
        raise ValueError("durable LAUS cycle mode evidence is required")
    expected = {"schema_version":"laus_cycle_execution_evidence_v1", "cycle_id":record["cycle_id"],
                "target_month":target_month, "annual_reference_year":when.year,
                "annual_vintage_id":vintage, "acquisition_mode":evidence.get("acquisition_mode")}
    if evidence != expected:
        raise ValueError("durable LAUS cycle mode/vintage evidence contradiction")
    if existing is not None:
        validate_satisfaction(existing)
        if existing["annual_vintage_id"] != vintage:
            raise ValueError("annual satisfaction key mismatch")
        return existing, False
    if evidence["acquisition_mode"] != "annual_deep":
        return None, False
    decision=select_routine(cycle_date=when, annual_reference_year=when.year, satisfactions=[])
    proved={**result, "acquisition_mode":evidence["acquisition_mode"]}
    return satisfaction_record(decision=decision,result=proved,cycle_id=record["cycle_id"]), True


def _remote_json(api: GitHubAPI, path: str, branch: str) -> dict | None:
    item, _ = api.request("GET", f"/contents/{quote(path, safe='/')}?ref={quote(branch)}",
                          expected=(200, 404))
    return None if item is None else json.loads(base64.b64decode(item["content"]))


def main() -> int:
    parser=argparse.ArgumentParser(); sub=parser.add_subparsers(dest="command",required=True)
    select=sub.add_parser("select"); select.add_argument("--target-month",required=True)
    select.add_argument("--satisfactions",type=Path,default=Path("config/laus_annual_deep_satisfactions")); select.add_argument("--output",type=Path,required=True)
    satisfy=sub.add_parser("satisfy"); satisfy.add_argument("--target-month",required=True)
    satisfy.add_argument("--cycle-id",required=True)
    satisfy.add_argument("--repository",required=True); satisfy.add_argument("--branch",required=True)
    satisfy.add_argument("--invocation-mode",choices=("normal","resume","replay"),required=True)
    args=parser.parse_args(); when=cycle_date(args.target_month)
    if args.command == "select":
        decision=select_routine(cycle_date=when,annual_reference_year=when.year,
                                satisfactions=load_satisfactions(args.satisfactions))
        value={"acquisition_mode":decision.acquisition_mode,"annual_reference_year":decision.annual_reference_year,
               "annual_vintage_id":decision.annual_vintage_id,"state":decision.state.value}
        write_canonical_json(args.output,value)
    else:
        api=GitHubAPI(args.repository,os.environ.get("GITHUB_TOKEN",""))
        durable=_remote_json(api,record_path(args.cycle_id,"laus"),args.branch)
        if durable is None: raise ValueError("durable LAUS cycle result is required")
        store=GitHubAnnualSatisfactionStore(api,args.branch)
        existing,_=store._read(satisfaction_path(when.year))
        proposed,due=satisfaction_from_cycle_record(record=durable,target_month=args.target_month,
            invocation_mode=args.invocation_mode,existing=existing)
        if not due:
            value={"record":proposed,"changed":False}
        else:
            stored,changed=store.put(proposed); value={"record":stored,"changed":changed}
    print(json.dumps(value,sort_keys=True)); return 0

if __name__ == "__main__": raise SystemExit(main())
