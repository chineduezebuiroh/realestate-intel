"""Evidence-only explanation of the shared Structural turn detector semantics."""
from __future__ import annotations

import json
from pathlib import Path
import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import (
    TURN_FIXED_PROMINENCE, TURN_PERSISTENCE, TURN_PROMINENCE_MULTIPLIER,
    calendar_delta, detect_turning_points, direction,
)
from regime.experiments.demand_signal_attenuation import GEOS, RUN_ID
from regime.experiments.structural_turn_lineage import governed_detector_input

OUTPUT_FILES = ("detector_input.csv", "candidate_turns.csv", "candidate_criteria.csv",
                "county_rejection_summary.csv", "pooled_rejection_summary.csv",
                "representative_candidate_traces.csv", "detector_parity.csv")


def analyze_county(frame: pd.DataFrame, geo_id: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Enumerate adjacent-sign reversals, then transparently replay the helper."""
    work = frame[["date", "structural_score"]].dropna().sort_values("date").copy()
    work["date"] = pd.to_datetime(work.date)
    changes = calendar_delta(work, "structural_score", 1)
    changes["direction"] = changes.delta.map(direction)
    material = changes.delta.abs().dropna()
    dynamic = TURN_PROMINENCE_MULTIPLIER * (float(material.median()) if len(material) else 0.0)
    threshold = max(TURN_FIXED_PROMINENCE, dynamic)
    rows = []
    # A simple candidate is a point whose immediately incoming and outgoing
    # calendar-month deltas are both non-zero and have opposing signs.
    for k in range(len(changes) - 1):
        incoming, outgoing = changes.iloc[k], changes.iloc[k + 1]
        if incoming.direction not in {"positive", "negative"} or outgoing.direction not in {"positive", "negative"}:
            continue
        if incoming.direction == outgoing.direction:
            continue
        i = k + 1
        pre = changes.iloc[max(0, i-TURN_PERSISTENCE):i]
        post = changes.iloc[i:i+TURN_PERSISTENCE]
        full_windows = len(pre) == len(post) == TURN_PERSISTENCE
        contiguous = bool(full_windows and pre.lag_value.notna().all() and post.lag_value.notna().all())
        pdirs, ndirs = set(pre.direction), set(post.direction)
        persistence_pass = bool(contiguous and len(pdirs) == len(ndirs) == 1)
        direction_pass = bool(persistence_pass and
                              next(iter(pdirs)) in {"positive", "negative"} and
                              next(iter(ndirs)) in {"positive", "negative"} and pdirs != ndirs)
        edge_condition_pass = bool(TURN_PERSISTENCE <= i < len(changes)-TURN_PERSISTENCE)
        detector_accept = bool(persistence_pass and direction_pass and edge_condition_pass)
        prominence = float(abs(pre.delta.sum()) + abs(post.delta.sum())) if full_windows else np.nan
        prominence_pass = bool(np.isfinite(prominence) and prominence > threshold)
        failed = []
        if not persistence_pass: failed.append("INSUFFICIENT_PERSISTENCE")
        if not direction_pass: failed.append("DIRECTION_FAILURE")
        if not edge_condition_pass: failed.append("EDGE_CONDITION")
        if not prominence_pass: failed.append("INSUFFICIENT_PROMINENCE")
        primary = failed[0] if failed else "NONE"
        rows.append({"geo_id":geo_id, "candidate_date":incoming.date,
            "candidate_type":"peak" if incoming.direction == "positive" else "trough",
            "candidate_value":incoming.structural_score, "prior_value":incoming.lag_value,
            "next_value":outgoing.structural_score, "incoming_direction":incoming.direction,
            "outgoing_direction":outgoing.direction, "prominence":prominence,
            "fixed_prominence_threshold":TURN_FIXED_PROMINENCE,
            "dynamic_prominence_threshold":dynamic, "effective_prominence_threshold":threshold,
            "prominence_pass":prominence_pass, "persistence_observations":min(len(pre),len(post)),
            "required_persistence":TURN_PERSISTENCE, "persistence_pass":persistence_pass,
            "direction":f"{incoming.direction}_to_{outgoing.direction}",
            "direction_pass":direction_pass, "qualification_pass":bool(detector_accept and prominence_pass),
            "edge_condition_pass":edge_condition_pass, "detector_accept":detector_accept, "primary_rejection_reason":primary,
            "failed_criteria":"|".join(failed) if failed else "NONE", "change_index":i})
    criteria = pd.DataFrame(rows)
    shared = detect_turning_points(work, "structural_score")
    reconstructed = criteria.loc[criteria.detector_accept] if len(criteria) else criteria
    shared_keys = {(pd.Timestamp(r.turning_point_date), r.turning_point_type) for r in shared.itertuples()}
    reconstructed_keys = {(pd.Timestamp(r.candidate_date), r.candidate_type) for r in reconstructed.itertuples()}
    parity = {"geo_id":geo_id, "reconstructed_accepted_turn_count":len(reconstructed),
              "shared_helper_detected_turn_count":len(shared),
              "shared_helper_qualified_turn_count":int(shared.qualified.sum()) if len(shared) else 0,
              "parity_pass":shared_keys == reconstructed_keys}
    if not parity["parity_pass"]: raise AssertionError(f"detector reconstruction mismatch: {geo_id}")
    return changes.assign(geo_id=geo_id), criteria, parity


def build_tables(detector_input: pd.DataFrame) -> dict[str, pd.DataFrame | dict]:
    inputs, candidates, parity_rows = [], [], []
    for geo in GEOS:
        raw = detector_input.loc[detector_input.geo_id.eq(geo)]
        changes, criteria, parity = analyze_county(raw, geo)
        inputs.append(changes.rename(columns={"date":"period", "delta":"calendar_month_delta",
                                              "lag_value":"calendar_month_lag_value"}))
        candidates.append(criteria); parity_rows.append(parity)
    criteria = pd.concat(candidates, ignore_index=True) if candidates else pd.DataFrame()
    parity = pd.DataFrame(parity_rows)
    summaries = []
    for geo in GEOS:
        raw = detector_input.loc[detector_input.geo_id.eq(geo)].sort_values("date")
        q = criteria.loc[criteria.geo_id.eq(geo)]
        rejected = q.loc[~q.detector_accept]
        summaries.append({"geo_id":geo, "candidate_turn_count":len(q),
            "accepted_turn_count":int(q.detector_accept.sum()), "rejected_turn_count":len(rejected),
            "rejected_prominence_count":int((~q.prominence_pass).sum()),
            "rejected_persistence_count":int((~q.persistence_pass).sum()),
            "rejected_direction_count":int((~q.direction_pass).sum()),
            "rejected_qualification_count":int((q.detector_accept & ~q.qualification_pass).sum()),
            "rejected_other_count":int(rejected.primary_rejection_reason.isin(["EDGE_CONDITION","OTHER"]).sum()),
            "structural_mean":raw.structural_score.mean(), "structural_std":raw.structural_score.std(),
            "structural_min":raw.structural_score.min(), "structural_max":raw.structural_score.max(),
            "structural_range":raw.structural_score.max()-raw.structural_score.min(),
            "observation_count":len(raw), "start_date":raw.date.min(), "end_date":raw.date.max()})
    county = pd.DataFrame(summaries)
    total = len(criteria); accepted = int(criteria.detector_accept.sum())
    metrics = {
        "total_simple_candidates":total, "total_accepted":accepted, "total_rejected":total-accepted,
        "counties_with_simple_candidate":int(criteria.geo_id.nunique()),
        "counties_with_governed_accepted_turn":int(criteria.loc[criteria.detector_accept,"geo_id"].nunique()),
        "prominence_rejection_count":int((~criteria.prominence_pass).sum()),
        "prominence_rejection_share":float((~criteria.prominence_pass).mean()) if total else 0,
        "persistence_rejection_count":int((~criteria.persistence_pass).sum()),
        "persistence_rejection_share":float((~criteria.persistence_pass).mean()) if total else 0,
        "direction_rejection_count":int((~criteria.direction_pass).sum()),
        "direction_rejection_share":float((~criteria.direction_pass).mean()) if total else 0,
        "prominence_min":criteria.prominence.min(), "prominence_median":criteria.prominence.median(),
        "prominence_max":criteria.prominence.max(),
        "effective_threshold_min":criteria.effective_prominence_threshold.min(),
        "effective_threshold_median":criteria.effective_prominence_threshold.median(),
        "effective_threshold_max":criteria.effective_prominence_threshold.max(),
        "persistence_pass_count":int(criteria.persistence_pass.sum()),
        "qualification_pass_count":int(criteria.qualification_pass.sum()),
    }
    pooled = pd.DataFrame([{"metric":k,"value":v} for k,v in metrics.items()])
    rejected = criteria.loc[~criteria.detector_accept].copy()
    selected = []
    if len(rejected):
        choices = {"highest_prominence":rejected.prominence.idxmax(), "lowest_prominence":rejected.prominence.idxmin(),
                   "closest_prominence":(rejected.effective_prominence_threshold-rejected.prominence).abs().idxmin(),
                   "closest_persistence":rejected.persistence_observations.idxmax()}
        for label, idx in choices.items():
            row = rejected.loc[idx]; raw = detector_input.loc[detector_input.geo_id.eq(row.geo_id)].sort_values("date").reset_index(drop=True)
            pos = raw.index[raw.date.eq(row.candidate_date)][0]
            window = raw.iloc[max(0,pos-TURN_PERSISTENCE):pos+TURN_PERSISTENCE+1].copy()
            window["selection_reason"], window["candidate_date"] = label, row.candidate_date
            window["relative_observation"] = range(window.index[0]-pos, window.index[-1]-pos+1)
            selected.append(window)
    traces = pd.concat(selected, ignore_index=True).drop_duplicates(["selection_reason","geo_id","date"]) if selected else pd.DataFrame()
    if total == 0: root = "NO_SIMPLE_STRUCTURAL_TURNS"
    elif not parity.parity_pass.all(): root = "DETECTOR_RECONSTRUCTION_MISMATCH"
    else:
        p, s = metrics["prominence_rejection_count"], metrics["persistence_rejection_count"]
        root = "MULTI_CRITERIA_REJECTION" if p and s else "PROMINENCE_REJECTION" if p else "PERSISTENCE_REJECTION" if s else "OTHER"
    summary = {**metrics, "root_cause_classification":root, "detector_parity_pass":bool(parity.parity_pass.all()),
        "recommendation_state":"none", "promotion_state":"none", "human_decision":"pending",
        "automated_winner":False, "production_policy_changed":False}
    return {"detector_input":pd.concat(inputs, ignore_index=True), "candidate_turns":criteria[["geo_id","candidate_date","candidate_type","candidate_value"]],
            "candidate_criteria":criteria, "county_rejection_summary":county,
            "pooled_rejection_summary":pooled, "representative_candidate_traces":traces,
            "detector_parity":parity, "diagnostic_summary":summary}


def build_review(run: Path, output: Path, root: Path | None = None) -> Path:
    run = run.resolve(); root = (root or Path(__file__).resolve().parents[2]).resolve()
    if run.name != RUN_ID: raise ValueError(f"authoritative run identity must be {RUN_ID}")
    if not run.is_dir(): raise FileNotFoundError(f"authoritative run absent: {run}")
    detector_input, _, _, _ = governed_detector_input(run, root)
    tables = build_tables(detector_input)
    output.mkdir(parents=True, exist_ok=False)
    for name in OUTPUT_FILES: tables[name.removesuffix(".csv")].to_csv(output/name, index=False, date_format="%Y-%m-%d", float_format="%.15g")
    summary = tables["diagnostic_summary"]
    (output/"diagnostic_summary.json").write_text(json.dumps(
        summary, indent=2, default=lambda x: None if pd.isna(x) else x.item())+"\n")
    (output/"README.md").write_text(
        "# Structural turn detector semantics\n\n"
        "Simple candidates are adjacent, non-zero calendar-month deltas with opposite signs. "
        "The shared helper instead requires three contiguous incoming deltas of one non-zero direction and "
        "three contiguous outgoing deltas of the opposite direction. It emits such a turn, then qualifies it "
        "only when six-window prominence (absolute incoming sum plus absolute outgoing sum) is strictly greater "
        "than max(0.05, twice the median absolute monthly delta).\n\n"
        f"Across seven counties there are {summary['total_simple_candidates']} simple candidates and "
        f"{summary['total_accepted']} helper-detected turns. Persistence rejects "
        f"{summary['persistence_rejection_count']} and prominence fails for {summary['prominence_rejection_count']} "
        "(criterion counts can overlap). Detector reconstruction matches the shared helper in every county. "
        "Thus zero detected turns is a faithful consequence of the helper semantics. The evidence may indicate "
        "a semantic mismatch with slow Structural composites, but this diagnostic makes no parameter or policy recommendation.\n\n"
        f"Root-cause classification: **{summary['root_cause_classification']}**. Production policy is unchanged.\n")
    return output
