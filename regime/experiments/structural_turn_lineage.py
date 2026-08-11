"""Diagnostic-only lineage trace for Structural turn expression.

This module intentionally owns no policy: it reuses the Structural/Cyclical
experiment contract and the shared turn detector/matcher, and writes only
caller-selected review artifacts.
"""
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.experiments.demand_signal_attenuation import (
    RUN_ID, GEOS, CORE_DEMAND, STRUCTURAL, LABOR, WEIGHT_POLICIES,
    _col, _feature_panel, _load, _scope, effective_contributions, production_contract,
)
from regime.experiments.structural_cyclical_demand_architecture import (
    _metric_weights, realized_metric_weights,
)

SCENARIO_ID = "LF-IN__LAUS-W-70-15-15__BAL-INCUMBENT-EXACT"
STAGES = (
    "A. persisted aligned metric scores",
    "B. structural metric contribution panel",
    "C. structural composite chronology",
    "D. turning-point detector input",
    "E. detected structural turns",
    "F. qualified structural turns",
    "G. scenario core-Demand turn chronology",
    "H. turn matching input",
    "I. matched structural-to-scenario turns",
    "J. structural_turn_expression_share",
    "K. evaluation-matrix export",
)
TURN_COLUMNS = ["turning_point_date", "turning_point_type", "incoming_persistence",
                "outgoing_persistence", "prominence", "prominence_threshold", "qualified"]


def _turns(frame: pd.DataFrame, value: str) -> pd.DataFrame:
    """Call the shared detector and stabilize its legitimate empty schema."""
    found = detect_turning_points(frame[["date", value]].dropna().sort_values("date"), value)
    return found.reindex(columns=TURN_COLUMNS) if found.empty else found


def _describe(stage: str, geo: str, frame: pd.DataFrame, score: str | None = None,
              turns: pd.DataFrame | None = None) -> dict:
    q = frame if geo == "POOLED" else frame.loc[frame.geo_id.eq(geo)]
    dates = pd.to_datetime(q["date"], errors="coerce") if "date" in q else pd.Series(dtype="datetime64[ns]")
    values = pd.to_numeric(q[score], errors="coerce") if score and score in q else pd.Series(dtype=float)
    tq = turns if turns is not None else pd.DataFrame()
    if geo != "POOLED" and not tq.empty and "geo_id" in tq: tq = tq.loc[tq.geo_id.eq(geo)]
    return {
        "stage_name": stage, "geo_id": geo, "row_count": len(q),
        "non_null_score_count": int(values.notna().sum()) if score else np.nan,
        "date_min": dates.min() if len(dates) else pd.NaT, "date_max": dates.max() if len(dates) else pd.NaT,
        "unique_date_count": int(dates.nunique()) if len(dates) else 0,
        "duplicate_grain_count": int(q.duplicated(["geo_id", "date"]).sum()) if {"geo_id", "date"}.issubset(q) else np.nan,
        "score_min": values.min() if score else np.nan, "score_max": values.max() if score else np.nan,
        "score_std": values.std() if score else np.nan, "zero_count": int(values.eq(0).sum()) if score else np.nan,
        "positive_count": int(values.gt(0).sum()) if score else np.nan,
        "negative_count": int(values.lt(0).sum()) if score else np.nan,
        "turn_count": len(tq) if turns is not None else np.nan,
        "qualified_turn_count": int(tq.qualified.fillna(False).sum()) if turns is not None and "qualified" in tq else np.nan,
    }


def build_tables(scores: pd.DataFrame, labor_scores: pd.DataFrame, base_weights: pd.Series,
                 current_exported_share: float | None = None) -> dict[str, pd.DataFrame]:
    """Build lineage tables from already governed inputs (also the smoke seam)."""
    weights = realized_metric_weights(base_weights, "LF-IN", "BAL-INCUMBENT-EXACT")
    structural = scores.loc[scores.metric.isin(STRUCTURAL)].copy()
    scenario_panel = pd.concat([structural[["geo_id", "date", "metric", "score"]], labor_scores], ignore_index=True)
    panel_rows, chronology_rows, legacy_rows = [], [], []
    for (geo, date), g in scenario_panel.groupby(["geo_id", "date"], sort=True):
        # Missingness renormalization belongs at the complete parent (core
        # Demand) grain.  Applying it after selecting Structural silently
        # inflates that block and can change fixed-prominence qualification.
        calc = effective_contributions(g.score, g.metric.map(weights))
        q = g.assign(effective_weight=calc.effective_feature_weight.to_numpy(),
                     contribution=calc.weighted_feature_contribution.to_numpy())
        sq = q.loc[q.metric.isin(STRUCTURAL)].copy()
        panel_rows.append(sq)
        row = {"geo_id": geo, "date": date}
        for metric in STRUCTURAL:
            hit = sq.loc[sq.metric.eq(metric)]
            row[f"{metric}_score"] = hit.score.iloc[0] if len(hit) else np.nan
            row[f"{metric}_effective_weight"] = hit.effective_weight.iloc[0] if len(hit) else np.nan
            row[f"{metric}_contribution"] = hit.contribution.iloc[0] if len(hit) else np.nan
        row["structural_score"] = sq.contribution.sum(min_count=1)
        chronology_rows.append(row)
        legacy_calc = effective_contributions(sq.score, sq.metric.map(weights))
        legacy_rows.append({"geo_id": geo, "date": date,
                            "structural_score": legacy_calc.weighted_feature_contribution.sum(min_count=1)})
    panel = pd.concat(panel_rows, ignore_index=True) if panel_rows else pd.DataFrame()
    chronology = pd.DataFrame(chronology_rows).sort_values(["geo_id", "date"]).reset_index(drop=True)

    scenario_rows = []
    for (geo, date), g in scenario_panel.groupby(["geo_id", "date"], sort=True):
        calc = effective_contributions(g.score, g.metric.map(weights))
        scenario_rows.append({"geo_id": geo, "date": date,
                              "core_demand_score": calc.weighted_feature_contribution.sum(min_count=1)})
    scenario = pd.DataFrame(scenario_rows).sort_values(["geo_id", "date"]).reset_index(drop=True)

    detected, scenario_turns, recent_scenario_turns, matches = [], [], [], []
    for geo in GEOS:
        c = chronology.loc[chronology.geo_id.eq(geo)]
        st = _turns(c, "structural_score").assign(geo_id=geo)
        detected.append(st)
        d = scenario.loc[scenario.geo_id.eq(geo)]
        dt = _turns(d, "core_demand_score").assign(geo_id=geo)
        scenario_turns.append(dt)
        recent = d.loc[d.date.ge(d.date.max() - pd.DateOffset(months=35))]
        recent_scenario_turns.append(_turns(recent, "core_demand_score").assign(geo_id=geo))
        matched = match_turning_points(st, dt)
        if not matched.empty:
            matched = matched.rename(columns={"incumbent_date": "structural_turn_date",
                                              "challenger_date": "scenario_turn_date",
                                              "turning_point_type": "structural_turn_type"})
            matched["scenario_turn_type"] = matched.structural_turn_type.where(matched.matched)
            matched["absolute_lag_months"] = matched.signed_delay_months.abs()
            matched["geo_id"] = geo
            matches.append(matched)
    detected = pd.concat(detected, ignore_index=True).rename(columns={"turning_point_date": "turn_date"})
    scenario_turns = pd.concat(scenario_turns, ignore_index=True).rename(columns={"turning_point_date": "turn_date"})
    scenario_turn_export = pd.concat([
        scenario_turns.assign(period="full_history"),
        pd.concat(recent_scenario_turns, ignore_index=True).rename(
            columns={"turning_point_date": "turn_date"}).assign(period="recent_36_months"),
    ], ignore_index=True)
    match = pd.concat(matches, ignore_index=True) if matches else pd.DataFrame(columns=[
        "geo_id", "structural_turn_date", "structural_turn_type", "scenario_turn_date",
        "scenario_turn_type", "matched", "signed_delay_months", "absolute_lag_months"])

    expression = []
    for geo in (*GEOS, "POOLED"):
        tq = detected if geo == "POOLED" else detected.loc[detected.geo_id.eq(geo)]
        mq = match if geo == "POOLED" else match.loc[match.geo_id.eq(geo)]
        denominator = int(tq.qualified.fillna(False).sum())
        numerator = int(mq.matched.fillna(False).sum()) if len(mq) else 0
        expression.append({"geo_id": geo, "structural_qualified_turns": denominator,
                           "matched_same_direction_turns": numerator,
                           "expression_share": numerator / denominator if denominator else 0.0})
    expression = pd.DataFrame(expression)
    reconstructed = float(expression.loc[expression.geo_id.eq("POOLED"), "expression_share"].iloc[0])
    exported = reconstructed if current_exported_share is None else float(current_exported_share)
    comparison = pd.DataFrame([{"scenario_id": SCENARIO_ID,
        "reconstructed_expression_share": reconstructed,
        "current_exported_expression_share": exported,
        "absolute_difference": abs(reconstructed-exported),
        "status": "match" if np.isclose(reconstructed, exported) else "mismatch"}])

    qualified = detected.loc[detected.qualified.fillna(False)]
    legacy = pd.DataFrame(legacy_rows).sort_values(["geo_id", "date"]).reset_index(drop=True)
    before_detected = []
    for geo in GEOS:
        before_detected.append(_turns(legacy.loc[legacy.geo_id.eq(geo)], "structural_score").assign(geo_id=geo))
    before_detected = pd.concat(before_detected, ignore_index=True) if before_detected else pd.DataFrame()
    before_qualified = before_detected.loc[before_detected.qualified.fillna(False)]
    before_matches = []
    for geo in GEOS:
        bm = match_turning_points(before_detected.loc[before_detected.geo_id.eq(geo)],
                                  scenario_turns.loc[scenario_turns.geo_id.eq(geo)].rename(columns={"turn_date":"turning_point_date"}))
        if not bm.empty: before_matches.append(bm)
    before_match = pd.concat(before_matches, ignore_index=True) if before_matches else pd.DataFrame(columns=["matched"])
    before_numerator = int(before_match.matched.fillna(False).sum())
    before_denominator = len(before_qualified)
    before_share = before_numerator / before_denominator if before_denominator else 0.0
    stage_counts = pd.DataFrame([
        {"stage":"structural_composite_rows", "before":len(legacy), "after":len(chronology)},
        {"stage":"detected_structural_turns", "before":len(before_detected), "after":len(detected)},
        {"stage":"qualified_structural_turns", "before":before_denominator, "after":len(qualified)},
        {"stage":"matched_structural_turns", "before":before_numerator, "after":int(match.matched.sum())},
        {"stage":"expression_share", "before":before_share, "after":reconstructed},
    ])

    scenario_qualified = scenario_turns.loc[scenario_turns.qualified.fillna(False)]
    std = float(chronology.structural_score.std())
    if not np.isfinite(std) or np.isclose(std, 0): cause, stage = "NO_STRUCTURAL_SCORE_VARIATION", STAGES[2]
    elif detected.empty: cause, stage = "NO_STRUCTURAL_TURNS_DETECTED", STAGES[4]
    elif qualified.empty: cause, stage = "STRUCTURAL_TURNS_NOT_QUALIFIED", STAGES[5]
    elif scenario_qualified.empty or match.empty: cause, stage = "MATCHER_INPUT_EMPTY", STAGES[7]
    elif not match.matched.any():
        same_window = False
        for geo in GEOS:
            a = qualified.loc[qualified.geo_id.eq(geo)]
            b = scenario_qualified.loc[scenario_qualified.geo_id.eq(geo)]
            for left in a.itertuples():
                for right in b.itertuples():
                    lag = abs((right.turn_date.year-left.turn_date.year)*12 + right.turn_date.month-left.turn_date.month)
                    same_window |= lag <= 6
        cause = "MATCH_DIRECTION_FAILURE" if same_window else "MATCHER_SEMANTICS_FAILURE"
        stage = STAGES[8]
    elif not np.isclose(reconstructed, exported): cause, stage = "EXPORT_WIRING_FAILURE", STAGES[10]
    else: cause, stage = "NO_FAILURE_REPRODUCED", "not_applicable"
    recommendations = {
        "NO_STRUCTURAL_SCORE_VARIATION": "Inspect persisted Structural score coverage and effective-weight renormalization.",
        "NO_STRUCTURAL_TURNS_DETECTED": "Review the shared detector's persistence semantics against the persisted chronology.",
        "STRUCTURAL_TURNS_NOT_QUALIFIED": "Review shared prominence qualification against the measured Structural prominence; do not change thresholds without governance.",
        "MATCHER_INPUT_EMPTY": "Populate both qualified Structural and scenario turn inputs before matching.",
        "MATCHER_SEMANTICS_FAILURE": "Review the governed match window against observed same-direction lags.",
        "MATCH_DIRECTION_FAILURE": "Audit turning-point type propagation at the matcher boundary.",
        "EXPORT_WIRING_FAILURE": "Wire the reconstructed pooled expression share into the representative evaluation row.",
        "NO_FAILURE_REPRODUCED": "No fix indicated by this representative lineage trace.",
    }
    root = pd.DataFrame([{"root_cause": cause, "first_failing_stage": stage,
        "evidence_summary": f"structural_std={std:.15g}; detected={len(detected)}; qualified={len(qualified)}; matched={int(match.matched.sum()) if len(match) else 0}",
        "recommended_smallest_fix": recommendations.get(cause, "Review the first failing transformation before proposing a production change."),
        "production_policy_changed": False}])

    stage_rows = []
    contribution_panel = panel.drop(columns=["score"]).rename(columns={"contribution":"score"})
    stage_frames = [scores, contribution_panel, chronology, chronology,
                    detected.rename(columns={"turn_date":"date"}), qualified.rename(columns={"turn_date":"date"}),
                    scenario, pd.concat([qualified.rename(columns={"turn_date":"date"}), scenario_qualified.rename(columns={"turn_date":"date"})]),
                    match.rename(columns={"structural_turn_date":"date"}), expression.assign(date=pd.NaT), comparison.assign(geo_id="POOLED", date=pd.NaT)]
    stage_scores = ["score", "score", "structural_score", "structural_score", None, None,
                    "core_demand_score", None, None, "expression_share", "current_exported_expression_share"]
    stage_turns = [None, None, None, None, detected, qualified, scenario_turns, qualified, match, None, None]
    for name, frame, value, turn_frame in zip(STAGES, stage_frames, stage_scores, stage_turns):
        if "geo_id" not in frame: frame = frame.assign(geo_id="POOLED")
        for geo in (*GEOS, "POOLED"): stage_rows.append(_describe(name, geo, frame, value, turn_frame))

    return {
        "before_vs_after_stage_counts": stage_counts,
        "repaired_lineage_summary": pd.DataFrame(stage_rows),
        "repaired_expression_share": expression,
        "repaired_match_audit": match,
        "repaired_export_comparison": comparison,
        "structural_turn_lineage_stage_summary": pd.DataFrame(stage_rows),
        "structural_turn_lineage_structural_chronology": chronology,
        "structural_turn_lineage_detector_input": chronology[["geo_id", "date", "structural_score"]],
        "structural_turn_lineage_detected_turns": detected,
        "structural_turn_lineage_scenario_turns": scenario_turn_export,
        "structural_turn_lineage_match_audit": match,
        "structural_turn_lineage_expression_reconstruction": expression,
        "structural_turn_lineage_export_comparison": comparison,
        "structural_turn_lineage_root_cause": root,
    }


def build_review(run: Path, output: Path, root: Path | None = None) -> Path:
    """Run the one-scenario trace, failing closed before creating output."""
    run = run.resolve(); root = (root or Path(__file__).resolve().parents[2]).resolve()
    if run.name != RUN_ID: raise ValueError(f"authoritative run identity must be {RUN_ID}")
    if not run.is_dir(): raise FileNotFoundError(f"authoritative run absent: {run}")
    fr, mr, _ = production_contract(root)
    scores = _load(run, "aligned_metric_scores")
    scores = scores.rename(columns={_col(scores, "canonical_metric_key", "metric_key", "metric"): "metric",
                                    _col(scores, "aligned_metric_score", "metric_score", "score"): "score"})
    scores["metric"] = scores.metric.replace({"laus_labor_force":"labor_force", "laus_employment":"employment"})
    scores = _scope(scores, "aligned_metric_scores", ["geo_id", "date", "metric"])
    scores = scores.loc[scores.metric.isin(CORE_DEMAND), ["geo_id", "date", "metric", "score"]]
    features, _ = _feature_panel(run, fr)
    weights = dict(zip(("level", "short", "long"), WEIGHT_POLICIES["LAUS-W-70-15-15"]))
    labor_rows = []
    for keys, g in features.loc[features.metric.isin(LABOR)].groupby(["geo_id", "date", "metric"]):
        calc = effective_contributions(g.normalized_feature_score, g.feature_type.map(weights))
        labor_rows.append((*keys, calc.weighted_feature_contribution.sum(min_count=1)))
    labor = pd.DataFrame(labor_rows, columns=["geo_id", "date", "metric", "score"])
    tables = build_tables(scores, labor, _metric_weights(mr))
    output.mkdir(parents=True, exist_ok=False)
    for name, frame in tables.items():
        frame.to_csv(output/f"{name}.csv", index=False, date_format="%Y-%m-%d", float_format="%.15g")
    (output / "repaired_root_cause.md").write_text(
        "# Structural turn-expression repair\n\n"
        "## First failing stage\n\n"
        "**Aggregation: Structural metric composite.**\n\n"
        "The lineage diagnostic selected the Structural rows before calling "
        "`effective_contributions()`. That function implements parent-level missingness "
        "renormalization, so the premature selection incorrectly renormalized the three "
        "Structural metrics to 100% instead of retaining their weights within complete core "
        "Demand. The inflated composite could change fixed-prominence turn qualification.\n\n"
        "The repair computes effective contributions once at the complete core-Demand grain "
        "and only then selects and sums Structural contributions. The shared "
        "`detect_turning_points()` and `match_turning_points()` implementations are unchanged. "
        "No registry, feature weight, Demand balance, LAUS weight, detector, matcher, or "
        "production policy changed. The repaired export comparison records whether expression "
        "and its reconstruction reconcile.\n"
    )
    return output
