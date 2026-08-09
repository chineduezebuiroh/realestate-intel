"""Diagnostic-only production-stage evidence for the BPS total-units metric."""
from __future__ import annotations

import html
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image, ImageDraw

from regime._00_config_loader import load_regime_config
from regime._01_feature_engine import build_feature_matrix_with_lineage
from regime._02_feature_normalizer import normalize_features
from regime._03_metric_scorer import score_metrics

GEOGRAPHIES = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
FEATURES = ("bps_total_units_level", "bps_total_units_short", "bps_total_units_long")
STAGES = ("raw_bps_total_units", "ma12_structural_level", "normalized_level_score",
          "short_feature", "normalized_short_score", "long_feature",
          "normalized_long_score", "metric_score")
ATTRIBUTIONS = frozenset({"source_volatility", "sparse_zero_heavy_source",
    "normalization_amplification", "short_long_feature_amplification", "mixed",
    "no_material_problem"})
TOLERANCE = 1e-12


def _canonical_input(source: pd.DataFrame) -> pd.DataFrame:
    work = source.copy()
    for old, new in {"metric_key": "canonical_metric_key", "metric_value": "value"}.items():
        if new not in work and old in work:
            work = work.rename(columns={old: new})
    required = {"geo_id", "date", "canonical_metric_key", "value"}
    if missing := required - set(work):
        raise ValueError(f"source metrics missing columns: {sorted(missing)}")
    work = work[list(required)].copy()
    work["date"] = pd.to_datetime(work["date"])
    work["value"] = pd.to_numeric(work["value"], errors="coerce")
    work = work[work.geo_id.isin(GEOGRAPHIES) & work.canonical_metric_key.eq("permit_activity")].dropna(subset=["value"])
    if set(work.geo_id.unique()) != set(GEOGRAPHIES):
        raise ValueError(f"authoritative source lacks required geographies: {sorted(set(GEOGRAPHIES)-set(work.geo_id.unique()))}")
    return work.sort_values(["geo_id", "date"])


def zero_streaks(raw: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for geo, group in raw.groupby("geo_id", sort=True):
        run = []
        for date in group.sort_values("date").loc[group.value.eq(0), "date"]:
            if run and (date.to_period("M") - run[-1].to_period("M")).n != 1:
                rows.append({"geo_id": geo, "streak_start": run[0], "streak_end": run[-1], "streak_length_months": len(run)})
                run = []
            run.append(date)
        if run:
            rows.append({"geo_id": geo, "streak_start": run[0], "streak_end": run[-1], "streak_length_months": len(run)})
    return pd.DataFrame(rows, columns=["geo_id", "streak_start", "streak_end", "streak_length_months"])


def _movement(series: pd.Series) -> float:
    x = pd.to_numeric(series, errors="coerce").dropna()
    scale = x.quantile(.75) - x.quantile(.25)
    return float(x.diff().abs().median() / scale) if len(x) > 1 and scale > 0 else 0.0


def _stability(chronology: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for geo, group in chronology.groupby("geo_id", sort=True):
        for stage in STAGES:
            x = pd.to_numeric(group[stage], errors="coerce").dropna(); d = x.diff().abs().dropna()
            rows.append({"geo_id": geo, "stage": stage, "observation_count": len(x),
                "median_abs_mom_change": d.median(), "p90_abs_mom_change": d.quantile(.9),
                "p99_abs_mom_change": d.quantile(.99), "max_abs_mom_change": d.max(),
                "robust_comparable_movement": _movement(x)})
    return pd.DataFrame(rows)


def _production_contract() -> pd.DataFrame:
    config = load_regime_config(validate=True)
    feats = config.features[config.features.feature_key.isin(FEATURES)].copy()
    expected = {"bps_total_units_level": ("ma_level", "12m", .50),
                "bps_total_units_short": ("ma_pct_change", "12m/lag3m", .25),
                "bps_total_units_long": ("ma_pct_change", "12m/lag12m", .25)}
    if len(feats) != 3 or any((r.transform, r.feature_window, float(r.feature_weight)) != expected[r.feature_key]
                              for r in feats.itertuples()):
        raise ValueError("current BPS feature registry disagrees with frozen MA12/50-25-25 contract")
    md = config.metric_dimensions[config.metric_dimensions.metric_key.eq("bps_total_units")]
    if len(md) != 1 or md.iloc[0].canonical_metric_key != "permit_activity" or float(md.iloc[0].metric_weight) != .20:
        raise ValueError("current Supply metric registry disagrees with frozen BPS 0.20 contract")
    return feats.merge(md[["metric_key", "canonical_metric_key", "subcomponent", "metric_weight"]], on="metric_key").assign(
        normalization_method="expanding_percentile", normalization_polarity="positive",
        parity_tolerance=TOLERANCE, supply_membership=True)


def _movement_evidence(chronology: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    columns = ["level_contribution", "short_contribution", "long_contribution"]
    frames = []
    for _, g in chronology.groupby("geo_id", sort=True):
        x = g[["geo_id", "date", "metric_score", *columns]].copy()
        x["metric_delta"] = x.metric_score.diff()
        for c in columns: x[f"{c}_delta"] = x[c].diff()
        x["reconstructed_metric_delta"] = sum(x[f"{c}_delta"] for c in columns)
        x["absolute_reconstruction_error"] = (x.metric_delta-x.reconstructed_metric_delta).abs()
        frames.append(x)
    movement = pd.concat(frames, ignore_index=True).dropna(subset=["metric_delta", "reconstructed_metric_delta"])
    if (movement.absolute_reconstruction_error > TOLERANCE).any():
        raise AssertionError("weighted contribution deltas do not reconstruct metric delta")
    summaries=[]; drivers=[]
    for geo, g in movement.groupby("geo_id", sort=True):
        denom = sum(g[f"{c}_delta"].abs() for c in columns)
        for family, c in zip(("level", "short", "long"), columns):
            d=g[f"{c}_delta"]; corr=d.corr(g.metric_delta)
            nonzero=d.ne(0)&g.metric_delta.ne(0)
            share=float((d.abs()/denom.replace(0,np.nan)).mean())
            summaries.append({"geo_id":geo,"feature_family":family,
                "median_abs_contribution_delta":d.abs().median(),"p90_abs_contribution_delta":d.abs().quantile(.9),
                "p99_abs_contribution_delta":d.abs().quantile(.99),"max_abs_contribution_delta":d.abs().max(),
                "mean_abs_contribution_delta":d.abs().mean(),
                "share_of_total_absolute_child_movement":share,"correlation_with_metric_delta":corr})
            drivers.append({"geo_id":geo,"feature_family":family,"observation_count":len(d),
                "correlation_with_metric_delta":corr,"nonzero_comparison_count":int(nonzero.sum()),
                "same_month_signed_agreement_share":float((np.sign(d[nonzero])==np.sign(g.loc[nonzero,"metric_delta"])).mean()) if nonzero.any() else np.nan})
    return movement, pd.DataFrame(summaries), pd.DataFrame(drivers)


def build_evidence(source: pd.DataFrame, source_run_id: str) -> dict[str, pd.DataFrame]:
    raw = _canonical_input(source); contract = _production_contract()
    observations = raw.assign(metric_origin="bps_total_units")
    features, _ = build_feature_matrix_with_lineage(
        canonical_observations=observations, derived_metric_lineage=pd.DataFrame())
    wanted = features[features.feature_key.isin(FEATURES)]
    normalized = normalize_features(wanted); metric = score_metrics(normalized)
    policies = normalized[["normalization_method", "lookback_periods", "min_periods", "score_direction"]].drop_duplicates()
    if len(policies) != 1 or tuple(policies.iloc[0].astype(str)) != ("expanding_percentile", "120", "36", "positive"):
        raise ValueError(f"current normalization registry disagrees with frozen BPS expanding-percentile contract: {policies.to_dict('records')}")
    f = wanted.pivot(index=["geo_id","date"], columns="feature_key", values="raw_feature_value")
    n = normalized.pivot(index=["geo_id","date"], columns="feature_key", values="feature_score")
    chronology = raw.pivot(index=["geo_id","date"], columns="canonical_metric_key", values="value").join(f).join(n,rsuffix="_score").reset_index().rename(columns={
        "permit_activity":"raw_bps_total_units", "bps_total_units_level":"ma12_structural_level",
        "bps_total_units_level_score":"normalized_level_score", "bps_total_units_short":"short_feature",
        "bps_total_units_short_score":"normalized_short_score", "bps_total_units_long":"long_feature",
        "bps_total_units_long_score":"normalized_long_score"})
    chronology = chronology.merge(metric[["geo_id","date","metric_score","feature_weight_sum"]], on=["geo_id","date"], how="left")
    base_weights={"level":.5,"short":.25,"long":.25}
    for family in base_weights:
        available=chronology[f"normalized_{family}_score"].notna() & chronology.metric_score.notna()
        chronology[f"effective_{family}_weight"] = np.where(available,base_weights[family]/chronology.feature_weight_sum,0.0)
        chronology[f"{family}_contribution"] = np.where(chronology.metric_score.notna(),
            chronology[f"normalized_{family}_score"].fillna(0)*chronology[f"effective_{family}_weight"],np.nan)
    error=(chronology[["level_contribution","short_contribution","long_contribution"]].sum(axis=1,min_count=1)-chronology.metric_score).abs()
    if (error.dropna()>TOLERANCE).any(): raise AssertionError("weighted contributions do not reconstruct metric score")

    zstreak=zero_streaks(raw); sparsity=[]
    for geo,g in raw.groupby("geo_id",sort=True):
        x=g.set_index("date").value.sort_index(); months=x.index.to_period("M")
        expected=pd.period_range(months.min(),months.max(),freq="M"); missing=expected.difference(months)
        row={"geo_id":geo,"observation_count":len(x),"first_date":x.index.min(),"last_date":x.index.max(),
             "expected_month_count":len(expected),"missing_month_count":len(missing),"coverage_pct":len(x)/len(expected),
             "zero_month_count":int(x.eq(0).sum()),"zero_share":x.eq(0).mean()}
        if not (row["expected_month_count"]>=row["observation_count"] and 0<=row["missing_month_count"]<=row["expected_month_count"] and 0<=row["coverage_pct"]<=1):
            raise AssertionError(f"invalid monthly coverage audit for {geo}")
        sparsity.append(row)
    sparse=pd.DataFrame(sparsity); stability=_stability(chronology)
    stab=stability.pivot(index="geo_id",columns="stage",values="robust_comparable_movement")
    transitions=[("raw_bps_total_units","ma12_structural_level"),("ma12_structural_level","normalized_level_score"),
      ("short_feature","normalized_short_score"),("long_feature","normalized_long_score"),
      ("normalized_level_score","level_contribution"),("normalized_short_score","short_contribution"),
      ("normalized_long_score","long_contribution")]
    attribution=[]
    for geo in GEOGRAPHIES:
        for a,b in transitions:
            before=_movement(chronology.loc[chronology.geo_id.eq(geo),a]); after=_movement(chronology.loc[chronology.geo_id.eq(geo),b])
            attribution.append({"geo_id":geo,"from_stage":a,"to_stage":b,"primary_measure":"median_absolute_first_difference_divided_by_IQR","from_movement":before,"to_movement":after,"amplification_ratio":after/before if before else np.nan})
        attribution.append({"geo_id":geo,"from_stage":"level_contribution+short_contribution+long_contribution","to_stage":"metric_score","primary_measure":"exact_weighted_reconstruction","from_movement":_movement(chronology.loc[chronology.geo_id.eq(geo),"metric_score"]),"to_movement":stab.loc[geo,"metric_score"],"amplification_ratio":1.0})
    movement,contrib_summary,drivers=_movement_evidence(chronology)
    summaries=[]
    for geo in GEOGRAPHIES:
        s=stab.loc[geo]; zero=float(sparse.loc[sparse.geo_id.eq(geo),"zero_share"].iloc[0]); flags=[]
        if zero>=.2: flags.append("sparse_zero_heavy_source")
        if s.raw_bps_total_units>=.5: flags.append("source_volatility")
        if s.normalized_level_score>s.ma12_structural_level*1.25: flags.append("normalization_amplification")
        if max(s.normalized_short_score,s.normalized_long_score)>s.normalized_level_score*1.25: flags.append("short_long_feature_amplification")
        primary="no_material_problem" if not flags else flags[0] if len(flags)==1 else "mixed"
        summaries.append({"geo_id":geo,"primary_attribution":primary,"secondary_attribution":"|".join(flags if primary=="mixed" else flags[1:]),
          "raw_zero_share":zero,"raw_volatility":s.raw_bps_total_units,"ma12_volatility":s.ma12_structural_level,
          "normalized_level_volatility":s.normalized_level_score,"normalized_short_volatility":s.normalized_short_score,
          "normalized_long_volatility":s.normalized_long_score,"metric_score_volatility":s.metric_score,
          "evidence_notes":"Descriptive classification only; absolute movement shares are not causal variance shares."})
    lineage=pd.DataFrame([{"stage":"raw_bps_total_units","registry_metric_key":"bps_total_units","canonical_metric_key":"permit_activity","formula":"Census BPS total permitted housing units","units":"Units"}])
    parity=pd.DataFrame([{"geo_id":g,"parity_check":"contributions_reconstruct_metric_score","tolerance":TOLERANCE,"max_abs_difference":error[chronology.geo_id.eq(g)].max(),"status":"pass"} for g in GEOGRAPHIES])
    return {"lineage_audit":lineage,"production_contract":contract,"chronology":chronology,"source_sparsity_audit":sparse,
      "zero_streaks":zstreak,"stage_stability":stability,"stage_attribution":pd.DataFrame(attribution),
      "metric_movement_attribution":movement,"contribution_summary":contrib_summary,"metric_driver_audit":drivers,
      "parity_audit":parity,"attribution_summary":pd.DataFrame(summaries),
      "human_decision_status":pd.DataFrame([{"source_run_id":source_run_id,"recommendation_state":"none","promotion_state":"none","human_decision":"pending"}])}


def _line_visual(frame: pd.DataFrame, geo: str, path: Path) -> None:
    width,height=1400,920; image=Image.new("RGB",(width,height),"white"); draw=ImageDraw.Draw(image)
    draw.text((30,15),f"{geo}: BPS production-stage attribution",fill="black")
    panels=[(["raw_bps_total_units"],"Panel 1 — raw monthly BPS units"),(["ma12_structural_level"],"Panel 2 — MA12 structural level"),
      (["normalized_level_score","normalized_short_score","normalized_long_score"],"Panel 3 — normalized level / short / long scores"),
      (["level_contribution","short_contribution","long_contribution","metric_score"],"Panel 4 — weighted contributions + final metric score")]
    colors=["#2563eb","#dc2626","#059669","#111827"]; n=max(len(frame)-1,1)
    for p,(columns,title) in enumerate(panels):
        top=55+p*210; bottom=top+170; left=80; right=width-25
        draw.rectangle((left,top,right,bottom),outline="#aaa"); draw.text((left+5,top+5),title,fill="black")
        vals=pd.concat([pd.to_numeric(frame[c],errors="coerce") for c in columns]).dropna(); lo,hi=(float(vals.min()),float(vals.max())) if len(vals) else (0.,1.)
        if hi==lo: hi=lo+1
        for ci,column in enumerate(columns):
            points=[]
            for i,value in enumerate(pd.to_numeric(frame[column],errors="coerce")):
                if pd.isna(value):
                    if len(points)>1: draw.line(points,fill=colors[ci],width=2)
                    points=[]; continue
                points.append((left+i*(right-left)/n,bottom-10-(float(value)-lo)/(hi-lo)*(bottom-top-30)))
            if len(points)>1: draw.line(points,fill=colors[ci],width=2)
            draw.text((left+290*ci,top+22),column,fill=colors[ci])
    image.save(path,format="PNG",optimize=False)


def _comparison_visual(summary: pd.DataFrame,path:Path)->None:
    width,height=1500,600; image=Image.new("RGB",(width,height),"white"); draw=ImageDraw.Draw(image)
    draw.text((20,15),"Seven-geography robust production-stage movement",fill="black")
    columns=["raw_volatility","ma12_volatility","normalized_level_volatility","normalized_short_volatility","normalized_long_volatility","metric_score_volatility"]
    maximum=max(float(summary[c].max()) for c in columns) or 1; colors=["#111827","#2563eb","#059669","#dc2626","#7c3aed","#0891b2"]
    for i,row in summary.iterrows():
        y=60+i*74; draw.text((10,y),str(row.geo_id)[:30],fill="black")
        for j,c in enumerate(columns):
            value=float(row[c]); x=300+j*190; draw.rectangle((x,y,x+int(120*value/maximum),y+18),fill=colors[j]); draw.text((x,y+20),f"{c[:16]} {value:.3f}",fill=colors[j])
    image.save(path,format="PNG",optimize=False)


def write_bundle(evidence:dict[str,pd.DataFrame],output_dir:Path,source_run_id:str)->int:
    output_dir.mkdir(parents=True,exist_ok=True); visual_dir=output_dir/"visuals"; visual_dir.mkdir(exist_ok=True)
    for key,frame in evidence.items(): frame.to_csv(output_dir/f"bps_permit_volatility_{key}.csv",index=False)
    figures=[]
    for geo in GEOGRAPHIES:
        name=f"{geo}__bps_permit_volatility.png"; _line_visual(evidence["chronology"].query("geo_id == @geo").sort_values("date"),geo,visual_dir/name); figures.append(name)
    comparison="seven_geo_bps_permit_volatility_comparison.png"; _comparison_visual(evidence["attribution_summary"],visual_dir/comparison)
    sections=["<h1>BPS Production-Stage Volatility Diagnostic</h1>","<p><strong>Diagnostic only: no production policy change or recommendation.</strong></p>"]
    for title,key in [("Production contract","production_contract"),("Coverage","source_sparsity_audit"),("Attribution","attribution_summary"),("Absolute movement contribution shares","contribution_summary"),("Metric drivers","metric_driver_audit")]: sections.append(f"<h2>{title}</h2>{evidence[key].to_html(index=False)}")
    sections.append(f"<img src='visuals/{comparison}'>"+"".join(f"<h3>{html.escape(g)}</h3><img src='visuals/{n}'>" for g,n in zip(GEOGRAPHIES,figures)))
    (output_dir/"bps_permit_volatility_review.html").write_text("<!doctype html><meta charset='utf-8'><style>body{font-family:sans-serif}img{max-width:100%}table{font-size:11px}</style>"+"".join(sections),encoding="utf-8")
    runtime=pd.DataFrame([{"source_run_id":source_run_id,"geography_count":7,"visual_count":8,"output_file_count":len(list(output_dir.rglob("*")))+1}])
    runtime.to_csv(output_dir/"bps_permit_volatility_runtime_summary.csv",index=False)
    return len(list(output_dir.rglob("*")))
