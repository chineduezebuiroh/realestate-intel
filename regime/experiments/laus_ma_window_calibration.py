"""Diagnostic-only LAUS moving-average window calibration.

The experiment deliberately has no registry write path.  It reconstructs only
the governed 2 x 2 x 4 factorial from the authoritative source chronology and
uses the shared smoothing implementation (and therefore its calendar coverage
and exact calendar-lag contracts).
"""
from __future__ import annotations

from pathlib import Path
import html

import numpy as np
import pandas as pd

from regime._04_asof_aligner import align_metric_scores_asof
from regime.diagnostics.capital_markets_ma import detect_turning_points
from regime.experiments import laus_feature_architecture as laus
from regime.experiments.demand_signal_attenuation import (
    CORE_DEMAND, GEOS, LABOR, RUN_ID, STRUCTURAL, _col, _contribution_layer,
    _load, _reversal_rate, _scope, cancellation, effective_contributions,
    recent_36,
)
from regime.experiments.structural_cyclical_demand_architecture import (
    _metric_weights, conflict_month, realized_metric_weights,
)

MA_WINDOWS = (3, 6, 9, 12)
LAUS_WEIGHTS = {
    "LAUS-W-70-15-15": (.70, .15, .15),
    "LAUS-W-80-10-10": (.80, .10, .10),
}
BALANCES = ("BAL-S25-C75", "BAL-S35-C65")
GOVERNANCE = {
    "recommendation_state": "none", "promotion_state": "none",
    "human_decision": "pending", "automated_winner": False,
    "production_policy_changed": False,
}


def scenario_grid() -> pd.DataFrame:
    rows = []
    for weight in LAUS_WEIGHTS:
        for balance in BALANCES:
            for window in MA_WINDOWS:
                rows.append({
                    "scenario_id": f"LF-IN__{weight}__{balance}__MA{window}",
                    "labor_force_membership": "LF-IN",
                    "laus_weight_policy": weight, "balance_policy": balance,
                    "ma_window": f"MA{window}", "ma_months": window,
                    **GOVERNANCE,
                })
    out = pd.DataFrame(rows)
    if len(out) != 16 or out.scenario_id.duplicated().any():
        raise AssertionError("governed factorial must contain exactly 16 unique scenarios")
    return out


def construct_laus_features(source: pd.DataFrame, window: int) -> pd.DataFrame:
    """Public testable boundary around the shared governed MA constructor."""
    if window not in MA_WINDOWS:
        raise ValueError(f"window must be one of {MA_WINDOWS}")
    return laus._features(source, window)


def align_challenger_laus_scores(
    challenger: pd.DataFrame,
    persisted_aligned: pd.DataFrame,
) -> pd.DataFrame:
    """Apply production as-of semantics to challenger LAUS metric scores.

    Persisted non-LAUS rows supply the authoritative production evaluation
    calendar.  They are calendar carriers only: the returned frame contains
    exclusively challenger LAUS rows aligned by the production aligner.
    """
    candidate = challenger.copy()
    candidate["date"] = pd.to_datetime(candidate["date"])
    candidate["canonical_metric_key"] = candidate["metric"]
    candidate["metric_score"] = pd.to_numeric(candidate["metric_score"])
    available = candidate[["level_score", "short_score", "long_score"]].notna()
    candidate["feature_count"] = available.sum(axis=1)
    candidate["feature_weight_sum"] = 1.0
    candidate["min_feature_score"] = candidate[
        ["level_score", "short_score", "long_score"]
    ].min(axis=1)
    candidate["max_feature_score"] = candidate[
        ["level_score", "short_score", "long_score"]
    ].max(axis=1)
    metric_columns = [
        "geo_id", "date", "canonical_metric_key", "metric_score",
        "feature_count", "feature_weight_sum", "min_feature_score",
        "max_feature_score",
    ]

    carriers = persisted_aligned.copy()
    evaluation = _col(carriers, "evaluation_date", "date")
    carriers = carriers.rename(columns={evaluation: "date"})
    carriers = carriers.loc[
        ~carriers[_col(carriers, "canonical_metric_key", "metric_key", "metric")]
        .isin(LABOR)
    ].copy()
    carriers = carriers.rename(columns={
        _col(carriers, "canonical_metric_key", "metric_key", "metric"):
            "canonical_metric_key",
        _col(carriers, "aligned_metric_score", "metric_score", "score"):
            "metric_score",
    })
    # Aligner metadata is immaterial for calendar carriers, but its production
    # input contract remains explicit and complete.
    for column, default in (
        ("feature_count", 1), ("feature_weight_sum", 1.0),
        ("min_feature_score", carriers["metric_score"]),
        ("max_feature_score", carriers["metric_score"]),
    ):
        if column not in carriers:
            carriers[column] = default

    aligned = align_metric_scores_asof(pd.concat(
        [carriers[metric_columns], candidate[metric_columns]],
        ignore_index=True,
    ))
    aligned = aligned.loc[
        aligned["canonical_metric_key"].isin(LABOR)
    ].rename(columns={
        "evaluation_date": "date", "canonical_metric_key": "metric",
        "metric_score": "score",
    })
    return aligned[["geo_id", "date", "metric_date", "metric", "score", "metric_age_days"]]


def _turn_dates(frame: pd.DataFrame, value: str) -> pd.DataFrame:
    found = detect_turning_points(frame[["date", value]].dropna().sort_values("date"), value)
    if found.empty:
        return found
    return found.loc[found["qualified"].eq(True)].copy()


def _nearest_month_distance(left: pd.Series, right: pd.Series) -> float:
    if left.empty or right.empty:
        return np.nan
    a = pd.PeriodIndex(pd.to_datetime(left), freq="M").astype(int).to_numpy()
    b = pd.PeriodIndex(pd.to_datetime(right), freq="M").astype(int).to_numpy()
    return float(np.mean([np.min(np.abs(b - item)) for item in a]))


def _crossings(g: pd.DataFrame, value: str) -> pd.Series:
    q = g[["date", value]].dropna().sort_values("date")
    signs = np.sign(q[value])
    return q.loc[signs.ne(signs.shift()) & signs.shift().notna(), "date"]


def _comparison(candidate: pd.DataFrame, reference: pd.DataFrame) -> dict[str, float]:
    q = candidate[["date", "core_demand_score"]].merge(
        reference[["date", "core_demand_score"]], on="date", suffixes=("", "_reference")
    ).dropna()
    if q.empty:
        return {k: np.nan for k in ("monthly_correlation", "direction_agreement_share",
            "sign_agreement_share", "mean_absolute_score_difference",
            "median_absolute_score_difference", "zero_crossing_count_difference",
            "zero_crossing_timing_difference_months", "major_peak_timing_difference_months",
            "major_trough_timing_difference_months")}
    delta = q.core_demand_score.diff(); ref_delta = q.core_demand_score_reference.diff()
    c_cross = _crossings(q, "core_demand_score")
    r_cross = _crossings(q, "core_demand_score_reference")
    ct = _turn_dates(q, "core_demand_score"); rt = _turn_dates(q, "core_demand_score_reference")
    def turns(frame: pd.DataFrame, kind: str) -> pd.Series:
        if frame.empty: return pd.Series(dtype="datetime64[ns]")
        return frame.loc[frame.turning_point_type.eq(kind), "turning_point_date"]
    difference = (q.core_demand_score - q.core_demand_score_reference).abs()
    return {
        "monthly_correlation": q.core_demand_score.corr(q.core_demand_score_reference),
        "direction_agreement_share": float(np.sign(delta).eq(np.sign(ref_delta)).iloc[1:].mean()),
        "sign_agreement_share": float(np.sign(q.core_demand_score).eq(np.sign(q.core_demand_score_reference)).mean()),
        "mean_absolute_score_difference": difference.mean(),
        "median_absolute_score_difference": difference.median(),
        "zero_crossing_count_difference": int(len(c_cross) - len(r_cross)),
        "zero_crossing_timing_difference_months": _nearest_month_distance(c_cross, r_cross),
        "major_peak_timing_difference_months": _nearest_month_distance(turns(ct,"peak"), turns(rt,"peak")),
        "major_trough_timing_difference_months": _nearest_month_distance(turns(ct,"trough"), turns(rt,"trough")),
    }


def _calibration_contract(root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate production membership/axis semantics without freezing LAUS candidates.

    This diagnostic deliberately varies LAUS MA windows and feature weights, so
    it must not reuse the historical production_contract() gate that requires
    the former MA6 / 25-35-40 LAUS feature contract.
    """
    mr = pd.read_csv(root / "config/metric_dimension_registry.csv")
    ar = pd.read_csv(root / "config/axis_registry.csv")

    active_metric = mr["enabled"].astype(str).str.lower().isin(
        {"true", "1", "yes", "y"}
    )
    active = mr.loc[active_metric].copy()

    demand = active.loc[
        active["dimension"].astype(str).str.lower().eq("demand")
    ].copy()

    canonical = set(
        demand["canonical_metric_key"]
        .dropna()
        .astype(str)
        .str.strip()
    )

    if canonical != set(CORE_DEMAND):
        raise ValueError(
            "active Core Demand membership drift: "
            f"expected={sorted(CORE_DEMAND)} actual={sorted(canonical)}"
        )

    demand_axis = ar.loc[
        ar["enabled"].astype(str).str.lower().isin({"true", "1", "yes", "y"})
        & ar["axis"].astype(str).str.lower().eq("demand")
    ].copy()

    expected_dimensions = {
        "demand",
        "price",
        "affordability",
        "capital_markets",
    }
    actual_dimensions = set(
        demand_axis["dimension"].astype(str).str.lower()
    )

    if actual_dimensions != expected_dimensions:
        raise ValueError(
            "Demand-axis membership drift: "
            f"expected={sorted(expected_dimensions)} "
            f"actual={sorted(actual_dimensions)}"
        )

    if demand_axis["dimension"].astype(str).str.lower().duplicated().any():
        raise ValueError("Demand-axis dimensions must be unique")

    demand_axis["dimension_weight"] = pd.to_numeric(
        demand_axis["dimension_weight"],
        errors="raise",
    )

    if (demand_axis["dimension_weight"] <= 0).any():
        raise ValueError("Demand-axis weights must be positive")

    if not np.isclose(
        demand_axis["dimension_weight"].sum(),
        1.0,
        atol=1e-12,
        rtol=0,
    ):
        raise ValueError("Demand-axis weights must sum to 1.0")

    enabled_axis = ar.loc[
        ar["enabled"].astype(str).str.lower().isin({"true", "1", "yes", "y"})
    ].copy()

    return active, enabled_axis


def _build_chronology(run: Path, root: Path, registry: pd.DataFrame):
    mr, ar = _calibration_contract(root)
    source = laus._source(run)
    base = _metric_weights(mr)
    persisted = _load(run, "aligned_metric_scores")
    persisted = persisted.rename(columns={
        _col(persisted,"canonical_metric_key","metric_key","metric"): "metric",
        _col(persisted,"aligned_metric_score","metric_score","score"): "score",
    })
    persisted["metric"] = persisted.metric.replace({"laus_labor_force":"labor_force", "laus_employment":"employment"})
    persisted = _scope(persisted, "aligned_metric_scores", ["geo_id","date","metric"])
    structural = persisted.loc[persisted.metric.isin(STRUCTURAL), ["geo_id","date","metric","score"]]
    feature_cache = {n: laus._features(source, n) for n in MA_WINDOWS}
    metric_cache = {}
    for n in MA_WINDOWS:
        features = feature_cache[n]
        # _chronology delegates normalization and missing-feature renormalization
        # to the same shared production-safe implementations used by the earlier
        # LAUS architecture diagnostic.
        for policy, weights in LAUS_WEIGHTS.items():
            metric_cache[n, policy] = laus._chronology(source, policy, n, weights)

    aligned_metric_cache = {
        key: align_challenger_laus_scores(value, persisted)
        for key, value in metric_cache.items()
    }

    rows=[]; detail_rows=[]
    for sc in registry.itertuples():
        weights = realized_metric_weights(base, "LF-IN", sc.balance_policy)
        labor = aligned_metric_cache[sc.ma_months, sc.laus_weight_policy]
        panel = pd.concat([structural, labor[["geo_id","date","metric","score"]]], ignore_index=True)
        for (geo,date), g in panel.groupby(["geo_id","date"]):
            calc=effective_contributions(g.score,g.metric.map(weights)); q=g.assign(contribution=calc.weighted_feature_contribution.to_numpy())
            s=q.loc[q.metric.isin(STRUCTURAL),"contribution"]; c=q.loc[q.metric.isin(LABOR),"contribution"]
            sg,_,_=cancellation(s); cg,_,cc=cancellation(c); gross,_,cancel=cancellation(q.contribution)
            ss=s.sum(min_count=1); cs=c.sum(min_count=1)
            rows.append({"scenario_id":sc.scenario_id,"geo_id":geo,"date":date,
                "core_demand_score":q.contribution.sum(min_count=1),"structural_score":ss,
                "cyclical_score":cs,"core_demand_cancellation":cancel,
                "cyclical_cancellation":cc,"conflict_neutralization":bool(conflict_month(pd.Series([ss]),pd.Series([cs])).fillna(False).iloc[0]),
                "combined_gross":gross,"structural_gross":sg,"cyclical_gross":cg})
            detail_rows.extend(q.assign(scenario_id=sc.scenario_id,geo_id=geo,date=date).to_dict("records"))
    chronology=pd.DataFrame(rows).merge(registry[["scenario_id","laus_weight_policy","balance_policy","ma_window","ma_months"]],on="scenario_id")

    # Rebuild only the Demand axis, retaining persisted non-core dimensions.
    dims=_load(run,"dimension_scores").rename(columns={_col(_load(run,"dimension_scores"),"dimension_score","score"):"score"})
    dims=_scope(dims,"dimension_scores",["geo_id","date","dimension"])
    fixed=dims.loc[dims.dimension.str.lower().isin(["price","affordability","capital_markets"])]
    axis=[]
    for sid,g in chronology.groupby("scenario_id"):
        inputs=pd.concat([g[["geo_id","date","core_demand_score"]].rename(columns={"core_demand_score":"score"}).assign(dimension="demand"),fixed],ignore_index=True)
        _, monthly=_contribution_layer(inputs,ar.loc[ar.axis.str.lower().eq("demand")],"axis","dimension","score",_col(ar,"dimension_weight","weight"))
        axis.append(monthly.loc[monthly.axis.str.lower().eq("demand"),["geo_id","date","net_score"]].assign(scenario_id=sid))
    axis=pd.concat(axis,ignore_index=True).rename(columns={"net_score":"demand_axis_score"})
    return chronology.merge(axis,on=["scenario_id","geo_id","date"],how="left",validate="one_to_one"), pd.DataFrame(detail_rows)


def _summaries(chron: pd.DataFrame):
    county=[]
    for (sid,geo),g in chron.groupby(["scenario_id","geo_id"]):
        recent=recent_36(g)
        row={"scenario_id":sid,"geo_id":geo,"observations":g.core_demand_score.notna().sum(),
            "core_std":g.core_demand_score.std(),"recent_core_std":recent.core_demand_score.std(),
            "median_abs_core_score":g.core_demand_score.abs().median(),
            "recent_median_abs_core_score":recent.core_demand_score.abs().median(),
            "median_abs_demand_axis":g.demand_axis_score.abs().median(),
            "median_core_cancellation":g.core_demand_cancellation.median(),
            "recent_core_cancellation":recent.core_demand_cancellation.median(),
            "median_cyclical_cancellation":g.cyclical_cancellation.median(),
            "conflict_neutralization_share":g.conflict_neutralization.mean(),
            "reversal_1m":_reversal_rate(g,"core_demand_score",1),
            "reversal_3m":_reversal_rate(g,"core_demand_score",3),
            "reversal_6m":_reversal_rate(g,"core_demand_score",6)}
        signs=np.sign(g.sort_values("date").core_demand_score.dropna())
        row["zero_crossings"]=int(max(0,signs.ne(signs.shift()).sum()-1)); row["same_sign_persistence"]=1-row["reversal_1m"]
        row["turn_count"]=len(_turn_dates(g,"core_demand_score")); county.append(row)
    county=pd.DataFrame(county).merge(scenario_grid(),on="scenario_id")
    keys=["ma_window","ma_months","laus_weight_policy","balance_policy"]
    numeric=[c for c in county.select_dtypes("number").columns if c not in keys]
    main=county.groupby(keys,as_index=False)[numeric].mean()
    return county,main


def _story(chron: pd.DataFrame):
    pooled=chron.groupby(["scenario_id","date"],as_index=False).mean(numeric_only=True)
    meta=scenario_grid(); pooled=pooled.merge(meta[["scenario_id","laus_weight_policy","balance_policy","ma_window","ma_months"]])
    rows=[]
    for (weight,balance),family in pooled.groupby(["laus_weight_policy","balance_policy"]):
        for candidate in MA_WINDOWS:
            c=family.loc[family.ma_months.eq(candidate)]
            for reference in (6,3):
                stats=_comparison(c,family.loc[family.ma_months.eq(reference)])
                turn_lags=[stats["major_peak_timing_difference_months"],stats["major_trough_timing_difference_months"]]
                matched_lag=float(np.mean([x for x in turn_lags if pd.notna(x)])) if any(pd.notna(x) for x in turn_lags) else np.nan
                rows.append({"laus_weight_policy":weight,"balance_policy":balance,
                    "candidate_ma":f"MA{candidate}","reference_ma":f"MA{reference}",**stats,
                    "sign_disagreement_share":1-stats["sign_agreement_share"],
                    "matched_turn_lag_months":matched_lag})
    story=pd.DataFrame(rows)
    responsiveness=story.loc[story.reference_ma.isin(["MA6","MA3"])].copy()
    return pooled,story,responsiveness


def _plots(chron: pd.DataFrame, output: Path):
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    visual=output/"visual_review"; visual.mkdir(parents=True,exist_ok=True)
    pooled=chron.groupby(["scenario_id","date"],as_index=False).mean(numeric_only=True)
    pooled=pooled.merge(
        scenario_grid()[["scenario_id","laus_weight_policy","balance_policy","ma_window"]],
        on="scenario_id",
        how="left",
        validate="many_to_one",
    )
    views=[(g,chron.loc[chron.geo_id.eq(g)]) for g in GEOS]+[("seven_county_pooled",pooled)]
    for name,frame in views:
        for weight in LAUS_WEIGHTS:
            for balance in BALANCES:
                fig,ax=plt.subplots(figsize=(10,4)); subset=frame.copy()
                if "laus_weight_policy" not in subset:
                    subset=subset.merge(
                        scenario_grid()[["scenario_id","laus_weight_policy","balance_policy","ma_window"]],
                        on="scenario_id",
                        how="left",
                        validate="many_to_one",
                    )
                subset=subset.loc[subset.laus_weight_policy.eq(weight)&subset.balance_policy.eq(balance)]
                for ma,g in subset.groupby("ma_window"): ax.plot(g.date,g.core_demand_score,label=ma)
                ax.axhline(0,color="black",lw=.7); ax.legend(ncol=4); ax.set_title(f"{name} | {weight} | {balance}"); fig.tight_layout()
                fig.savefig(visual/f"A__{name}__{weight}__{balance}.png",dpi=110); plt.close(fig)
    for weight in LAUS_WEIGHTS:
        for ma in MA_WINDOWS:
            q=pooled.loc[pooled.laus_weight_policy.eq(weight)&pooled.ma_months.eq(ma)]
            wide=q.pivot(index="date",columns="balance_policy",values="core_demand_score")
            ylim=float(np.nanmax(np.abs(wide.to_numpy())))
            fig,ax=plt.subplots(figsize=(10,4)); wide.plot(ax=ax); ax.axhline(0,color="black",lw=.7); ax.set_ylim(-ylim,ylim); fig.tight_layout()
            fig.savefig(visual/f"B__{weight}__MA{ma}.png",dpi=110); plt.close(fig)
            diff=wide[BALANCES[0]]-wide[BALANCES[1]]; fig,ax=plt.subplots(figsize=(10,3)); ax.plot(diff.index,diff); ax.axhline(0,color="black",lw=.7); fig.tight_layout()
            fig.savefig(visual/f"C__{weight}__MA{ma}.png",dpi=110); plt.close(fig)


def build_review(run: Path, output: Path, root: Path | None = None) -> Path:
    root=(root or Path(__file__).resolve().parents[2]).resolve(); run=run.resolve()
    if run.name != RUN_ID: raise ValueError(f"authoritative run identity must be {RUN_ID}")
    if not run.is_dir(): raise FileNotFoundError(f"authoritative run absent: {run}")
    registry=scenario_grid(); chron,detail=_build_chronology(run,root,registry)
    if set(chron.geo_id)!=set(GEOS): raise ValueError("seven-county decision basis was not preserved")
    county,main=_summaries(chron); pooled,story,responsiveness=_story(chron)
    interactions=main.copy(); interactions["balance_gap_median_abs_core"]=interactions.groupby(["ma_window","laus_weight_policy"])["median_abs_core_score"].transform(lambda x:x.max()-x.min())
    evaluation=main.merge(story.loc[story.reference_ma.eq("MA6")],left_on=["ma_window","laus_weight_policy","balance_policy"],right_on=["candidate_ma","laus_weight_policy","balance_policy"],how="left")
    output.mkdir(parents=True,exist_ok=False)
    exports={"laus_ma_window_scenario_registry":registry,"laus_ma_window_main_effect":main,
        "laus_ma_window_by_county":county,"laus_ma_window_responsiveness":responsiveness,
        "laus_ma_window_story_preservation":story,"laus_ma_window_interactions":interactions,
        "laus_ma_window_evaluation_matrix":evaluation,
        "laus_ma_window_governance_status":pd.DataFrame([{**GOVERNANCE,"decision_basis":"seven_county_pooled","governed_county_count":7,"dc_included":True}])}
    for name,frame in exports.items(): frame.to_csv(output/f"{name}.csv",index=False)
    chron.to_csv(output/"laus_ma_window_chronology.csv",index=False); detail.to_csv(output/"laus_ma_window_contributions.csv",index=False)
    _plots(chron,output)
    links="".join(f'<li><a href="{html.escape(name)}.csv">{html.escape(name)}</a></li>' for name in exports)
    (output/"laus_ma_window_review.html").write_text("<!doctype html><meta charset='utf-8'><title>LAUS MA window calibration</title><h1>LAUS MA window calibration</h1><p>Diagnostic evidence only; no automated winner or production change.</p><ul>"+links+"</ul>",encoding="utf-8")
    return output
