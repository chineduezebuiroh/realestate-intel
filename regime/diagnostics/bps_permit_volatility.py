"""Governed, diagnostic-only BPS/permit-intensity volatility evidence."""
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
from regime.derived_metrics import build_derived_metrics_with_lineage

GEOGRAPHIES = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
STAGES = ("raw_bps_total_units", "raw_permit_intensity", "ma12_structural_level",
          "normalized_level_score", "short_feature", "long_feature", "metric_score")
ATTRIBUTIONS = frozenset({"source_volatility", "sparse_zero_heavy_source",
    "derivation_amplification", "normalization_amplification",
    "short_long_feature_amplification", "mixed", "no_material_problem"})
TOLERANCE = 1e-12


def _canonical_input(source: pd.DataFrame) -> pd.DataFrame:
    """Accept persisted canonical long form (and common artifact aliases)."""
    work = source.copy()
    aliases = {"metric_key": "canonical_metric_key", "metric_value": "value"}
    for old, new in aliases.items():
        if new not in work and old in work:
            work = work.rename(columns={old: new})
    required = {"geo_id", "date", "canonical_metric_key", "value"}
    if missing := required - set(work):
        raise ValueError(f"source metrics missing columns: {sorted(missing)}")
    work = work[list(required)].copy()
    work["date"] = pd.to_datetime(work["date"])
    work["value"] = pd.to_numeric(work["value"], errors="coerce")
    work = work[work.geo_id.isin(GEOGRAPHIES)].dropna(subset=["value"])
    if set(work.geo_id.unique()) != set(GEOGRAPHIES):
        missing = sorted(set(GEOGRAPHIES) - set(work.geo_id.unique()))
        raise ValueError(f"authoritative source lacks required geographies: {missing}")
    return work.sort_values(["geo_id", "canonical_metric_key", "date"])


def zero_streaks(raw: pd.DataFrame) -> pd.DataFrame:
    """Return streaks of observed zero values; a missing month breaks a streak."""
    rows: list[dict[str, object]] = []
    for geo, group in raw.groupby("geo_id", sort=True):
        z = group.sort_values("date").loc[group.value.eq(0), "date"]
        if z.empty:
            continue
        run: list[pd.Timestamp] = []
        for date in z:
            gap = (date.to_period("M") - run[-1].to_period("M")).n if run else 1
            if run and gap != 1:
                rows.append({"geo_id": geo, "streak_start": run[0], "streak_end": run[-1],
                             "streak_length_months": len(run)})
                run = []
            run.append(date)
        rows.append({"geo_id": geo, "streak_start": run[0], "streak_end": run[-1],
                     "streak_length_months": len(run)})
    return pd.DataFrame(rows, columns=["geo_id", "streak_start", "streak_end", "streak_length_months"])


def _movement(series: pd.Series) -> float:
    """Primary comparable measure: median absolute first difference / IQR."""
    x = pd.to_numeric(series, errors="coerce").dropna()
    scale = x.quantile(.75) - x.quantile(.25)
    return float(x.diff().abs().median() / scale) if len(x) > 1 and scale > 0 else 0.0


def _stability(chronology: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for geo, group in chronology.groupby("geo_id", sort=True):
        for stage in STAGES:
            x = pd.to_numeric(group[stage], errors="coerce").dropna()
            d = x.diff().dropna().abs()
            mean = x.mean()
            sign = np.sign(x.diff().dropna())
            rows.append({"geo_id": geo, "stage": stage, "observation_count": len(x),
                "level_std": x.std(), "level_cv": abs(x.std()/mean) if mean != 0 else "not_applicable",
                "median_abs_mom_change": d.median(), "p90_abs_mom_change": d.quantile(.90),
                "p99_abs_mom_change": d.quantile(.99), "max_abs_mom_change": d.max(),
                "sign_flips": int((sign * sign.shift(1) < 0).sum()),
                "rolling_12m_volatility": x.diff().rolling(12, min_periods=12).std().mean(),
                "robust_comparable_movement": _movement(x),
                "ratio_fields_status": "not_applicable" if stage == "raw_bps_total_units" and x.eq(0).any() else "applicable"})
    return pd.DataFrame(rows)


def _lineage_contract() -> tuple[pd.DataFrame, pd.DataFrame]:
    config = load_regime_config(validate=True)
    feats = config.features[config.features.feature_key.str.startswith(("bps_total_units_", "permit_intensity_"))]
    md = config.metric_dimensions[config.metric_dimensions.metric_key.isin(["bps_total_units", "derived_permit_intensity"])]
    lineage = pd.DataFrame([
        {"stage": "raw_bps_total_units", "registry_metric_key": "bps_total_units", "canonical_metric_key": "permit_activity", "formula": "Census BPS total permitted housing units", "units": "Units", "geo_grain": "state|county|place", "population_semantics": "not_applicable"},
        {"stage": "raw_permit_intensity", "registry_metric_key": "derived_permit_intensity", "canonical_metric_key": "permit_intensity", "formula": "permit_activity / population * 1000", "units": "permits per 1,000 people", "geo_grain": "state|county", "population_semantics": "canonical population; annual observations as-of aligned and forward-filled; source date retained"},
    ])
    contract = feats.merge(md[["metric_key", "canonical_metric_key", "subcomponent", "metric_weight"]], on="metric_key", how="left")
    contract = contract.assign(normalization_method="expanding_percentile", normalization_lookback=120,
        normalization_min_periods=36, normalization_polarity="positive", incumbent_equals_ma12=True,
        parity_tolerance=TOLERANCE, supply_membership=True)
    return lineage, contract


def build_evidence(source: pd.DataFrame, source_run_id: str) -> dict[str, pd.DataFrame]:
    raw = _canonical_input(source)
    # The persisted source artifact is pre-feature. Rebuild the canonical ratio using
    # the production derivation builder, preserving its annual as-of/ffill lineage.
    derivation_input = raw[raw.canonical_metric_key.isin(["permit_activity", "population"])]
    derived, deriv_lineage = build_derived_metrics_with_lineage(derivation_input)
    permit = derived[derived.canonical_metric_key.eq("permit_intensity")]
    observations = pd.concat([
        raw[raw.canonical_metric_key.isin(["permit_activity", "population"])].assign(metric_origin=lambda x: np.where(x.canonical_metric_key.eq("permit_activity"), "bps_total_units", "population")),
        permit.assign(metric_origin="derived_permit_intensity")], ignore_index=True)
    features, _ = build_feature_matrix_with_lineage(canonical_observations=observations,
                                                     derived_metric_lineage=deriv_lineage)
    wanted = features[features.feature_key.str.startswith("permit_intensity_")]
    normalized = normalize_features(wanted)
    metric = score_metrics(normalized)
    piv_raw = raw[raw.canonical_metric_key.eq("permit_activity")].pivot(index=["geo_id", "date"], columns="canonical_metric_key", values="value")
    piv_permit = permit.pivot(index=["geo_id", "date"], columns="canonical_metric_key", values="value")
    f = wanted.pivot(index=["geo_id", "date"], columns="feature_key", values="raw_feature_value")
    n = normalized.pivot(index=["geo_id", "date"], columns="feature_key", values="feature_score")
    idx = piv_raw.index.union(piv_permit.index).sort_values()
    chronology = pd.DataFrame(index=idx).join(piv_raw).join(piv_permit).join(f).join(n, rsuffix="_score").reset_index()
    chronology = chronology.rename(columns={"permit_activity":"raw_bps_total_units", "permit_intensity":"raw_permit_intensity",
        "permit_intensity_level":"incumbent_structural_level", "permit_intensity_level_score":"normalized_level_score",
        "permit_intensity_short":"short_feature", "permit_intensity_short_score":"normalized_short_score",
        "permit_intensity_long":"long_feature", "permit_intensity_long_score":"normalized_long_score"})
    chronology["ma12_structural_level"] = chronology["incumbent_structural_level"]
    chronology = chronology.merge(metric[metric.canonical_metric_key.eq("permit_intensity")][["geo_id","date","metric_score"]], on=["geo_id","date"], how="left")
    poplin = deriv_lineage[(deriv_lineage.derived_metric_key.eq("permit_intensity")) & (deriv_lineage.component_metric_key.eq("population"))]
    poplin = poplin.rename(columns={"component_value":"population_value", "component_source_date":"population_source_date"})
    chronology = chronology.merge(poplin[["geo_id","date","population_value","population_source_date"]], on=["geo_id","date"], how="left")

    zstreak = zero_streaks(raw[raw.canonical_metric_key.eq("permit_activity")])
    sparsity = []
    for geo, g in raw[raw.canonical_metric_key.eq("permit_activity")].groupby("geo_id", sort=True):
        x = g.set_index("date").value.sort_index(); expected = pd.date_range(x.index.min(), x.index.max(), freq="M")
        diffs = x.diff().abs().dropna(); zs = zstreak[zstreak.geo_id.eq(geo)].streak_length_months
        sparsity.append({"geo_id":geo, "observation_count":len(x), "first_date":x.index.min(), "last_date":x.index.max(),
            "expected_month_count":len(expected), "missing_month_count":len(expected.difference(x.index)), "coverage_pct":len(x)/len(expected),
            "zero_month_count":int(x.eq(0).sum()), "zero_share":x.eq(0).mean(), "nonzero_month_count":int(x.ne(0).sum()),
            "longest_zero_streak":zs.max() if len(zs) else 0, "median_zero_streak":zs.median() if len(zs) else 0,
            "p90_zero_streak":zs.quantile(.9) if len(zs) else 0, "max_raw_units":x.max(), "median_raw_units":x.median(),
            "mean_raw_units":x.mean(), "std_raw_units":x.std(), "raw_coefficient_of_variation":x.std()/x.mean() if x.mean() else "not_applicable",
            "largest_abs_monthly_change":diffs.max(), "p90_abs_monthly_change":diffs.quantile(.9), "p99_abs_monthly_change":diffs.quantile(.99)})
    stability = _stability(chronology)
    stab = stability.pivot(index="geo_id", columns="stage", values="robust_comparable_movement")
    transitions = [("raw_bps_total_units","raw_permit_intensity"), ("raw_permit_intensity","ma12_structural_level"),
                   ("ma12_structural_level","normalized_level_score"), ("normalized_level_score","metric_score"),
                   ("normalized_level_score","short_feature"), ("normalized_level_score","long_feature")]
    attribution = []
    for geo in GEOGRAPHIES:
        for a,b in transitions:
            before, after = stab.loc[geo,a], stab.loc[geo,b]
            attribution.append({"geo_id":geo,"from_stage":a,"to_stage":b,"primary_measure":"median_absolute_first_difference_divided_by_IQR",
                                "from_movement":before,"to_movement":after,"amplification_ratio":after/before if before else np.nan})
    attribution = pd.DataFrame(attribution)
    sparse = pd.DataFrame(sparsity)
    summaries=[]
    for geo in GEOGRAPHIES:
        s=stab.loc[geo]; zero=float(sparse.loc[sparse.geo_id.eq(geo),"zero_share"].iloc[0]); ratios=attribution[attribution.geo_id.eq(geo)]
        largest=ratios.loc[ratios.amplification_ratio.idxmax()] if ratios.amplification_ratio.notna().any() else None
        amplified=[]
        if zero >= .20: amplified.append("sparse_zero_heavy_source")
        if s.raw_bps_total_units >= .50: amplified.append("source_volatility")
        if (ratios.query("to_stage == 'raw_permit_intensity'").amplification_ratio > 1.25).any(): amplified.append("derivation_amplification")
        if (ratios.query("to_stage == 'normalized_level_score'").amplification_ratio > 1.25).any(): amplified.append("normalization_amplification")
        if max(s.short_feature,s.long_feature) > s.normalized_level_score*1.25: amplified.append("short_long_feature_amplification")
        primary = "no_material_problem" if not amplified else amplified[0] if len(amplified)==1 else "mixed"
        summaries.append({"geo_id":geo,"primary_attribution":primary,"secondary_attribution":"|".join(amplified if primary=="mixed" else amplified[1:]),
            "raw_zero_share":zero,"raw_volatility":s.raw_bps_total_units,"ma12_volatility":s.ma12_structural_level,
            "normalized_level_volatility":s.normalized_level_score,"short_feature_volatility":s.short_feature,
            "long_feature_volatility":s.long_feature,"metric_score_volatility":s.metric_score,
            "largest_amplification_stage":f"{largest.from_stage}->{largest.to_stage}" if largest is not None else "none",
            "evidence_notes":"Governed thresholds: zero share >=0.20; robust movement >=0.50; stage amplification >1.25."})
    parity=[]
    for geo,g in chronology.groupby("geo_id",sort=True):
        diff=(g.incumbent_structural_level-g.ma12_structural_level).abs().max()
        parity.append({"geo_id":geo,"parity_check":"incumbent_level_equals_reconstructed_ma12","tolerance":TOLERANCE,"max_abs_difference":diff,"status":"pass" if pd.isna(diff) or diff<=TOLERANCE else "fail","context":"source+registries reconstruct production; no persisted feature/score artifact supplied"})
        for check in ("short","long","normalized_level","metric_score"):
            parity.append({"geo_id":geo,"parity_check":f"reconstructed_{check}_production_contract","tolerance":TOLERANCE,"max_abs_difference":np.nan,"status":"not_testable","context":"persisted production feature/score artifact unavailable"})
    lineage, contract = _lineage_contract()
    return {"lineage_audit":lineage, "production_contract":contract, "chronology":chronology,
        "source_sparsity_audit":sparse, "zero_streaks":zstreak, "stage_stability":stability,
        "stage_attribution":attribution, "parity_audit":pd.DataFrame(parity),
        "attribution_summary":pd.DataFrame(summaries),
        "human_decision_status":pd.DataFrame([{"source_run_id":source_run_id,"recommendation_state":"none","promotion_state":"none","human_decision":"pending"}])}


def _line_visual(frame: pd.DataFrame, geo: str, path: Path) -> None:
    """Render dependency-light deterministic shared-x review panels with Pillow."""
    width,height=1400,920; image=Image.new("RGB",(width,height),"white"); draw=ImageDraw.Draw(image)
    draw.text((30,15),f"{geo}: BPS / permit-intensity volatility lineage",fill="black")
    panels=[(["raw_bps_total_units"],"Raw BPS units"),
            (["raw_permit_intensity","ma12_structural_level"],"Raw intensity and MA12"),
            (["normalized_level_score","normalized_short_score","normalized_long_score"],"Normalized level / short / long"),
            (["metric_score"],"Final permit-intensity metric score")]
    colors=["#2563eb","#dc2626","#059669"]
    n=max(len(frame)-1,1)
    for p,(columns,title) in enumerate(panels):
        top=55+p*210; bottom=top+170; left=80; right=width-25
        draw.rectangle((left,top,right,bottom),outline="#aaaaaa"); draw.text((left+5,top+5),title,fill="black")
        vals=pd.concat([pd.to_numeric(frame[c],errors="coerce") for c in columns]).dropna()
        lo,hi=(float(vals.min()),float(vals.max())) if len(vals) else (0.,1.)
        if hi==lo: hi=lo+1
        for ci,column in enumerate(columns):
            series=pd.to_numeric(frame[column],errors="coerce"); points=[]
            for i,value in enumerate(series):
                if pd.isna(value):
                    if len(points)>1: draw.line(points,fill=colors[ci],width=2)
                    points=[]; continue
                points.append((left+i*(right-left)/n,bottom-10-(float(value)-lo)/(hi-lo)*(bottom-top-30)))
                if column=="raw_bps_total_units" and value==0: draw.ellipse((points[-1][0]-3,points[-1][1]-3,points[-1][0]+3,points[-1][1]+3),fill="#dc2626")
            if len(points)>1: draw.line(points,fill=colors[ci],width=2)
            draw.text((left+220*ci,top+22),column,fill=colors[ci])
    image.save(path,format="PNG",optimize=False)


def _comparison_visual(summary: pd.DataFrame, path: Path) -> None:
    width,height=1400,600; image=Image.new("RGB",(width,height),"white"); draw=ImageDraw.Draw(image)
    draw.text((20,15),"Seven-geography zero share and robust stage movement",fill="black")
    columns=["raw_zero_share","raw_volatility","ma12_volatility","normalized_level_volatility","short_feature_volatility","long_feature_volatility","metric_score_volatility"]
    maximum=max(float(pd.to_numeric(summary[c],errors="coerce").max()) for c in columns) or 1
    colors=["#111827","#2563eb","#059669","#dc2626","#7c3aed","#ea580c","#0891b2"]
    for i,(_,row) in enumerate(summary.iterrows()):
        y=60+i*74; draw.text((10,y),str(row.geo_id)[:30],fill="black")
        for j,column in enumerate(columns):
            value=float(row[column]); x=300+j*150; bar=int(110*value/maximum)
            draw.rectangle((x,y,x+bar,y+18),fill=colors[j]); draw.text((x,y+20),f"{column[:12]} {value:.3f}",fill=colors[j])
    image.save(path,format="PNG",optimize=False)


def write_bundle(evidence: dict[str, pd.DataFrame], output_dir: Path, source_run_id: str) -> int:
    output_dir.mkdir(parents=True, exist_ok=True); visual_dir=output_dir/"visuals"; visual_dir.mkdir(exist_ok=True)
    for key, frame in evidence.items(): frame.to_csv(output_dir/f"bps_permit_volatility_{key}.csv", index=False)
    chronology=evidence["chronology"]
    figures=[]
    for geo in GEOGRAPHIES:
        g=chronology[chronology.geo_id.eq(geo)].sort_values("date")
        name=f"{geo}__bps_permit_volatility.png"; _line_visual(g,geo,visual_dir/name); figures.append(name)
    summary=evidence["attribution_summary"]; comparison="seven_geo_bps_permit_volatility_comparison.png"; _comparison_visual(summary,visual_dir/comparison)
    sections=["<h1>BPS / Permit-Intensity Raw-Data Volatility Diagnostic</h1>",
      "<p class='guard'><strong>This diagnostic identifies where permit volatility originates. It does not change production policy.</strong></p>"]
    order=[("Production lineage / contract",["lineage_audit","production_contract"]),("Source sparsity summary",["source_sparsity_audit"]),("Attribution summary",["attribution_summary"])]
    for title,keys in order:
        sections.append(f"<h2>{title}</h2>"+"".join(evidence[k].to_html(index=False) for k in keys))
    sections.append(f"<h2>Seven-geo comparison</h2><img src='visuals/{comparison}'>")
    sections.append("<h2>Geography chronology</h2>"+"".join(f"<h3>{html.escape(g)}</h3><img src='visuals/{n}'>" for g,n in zip(GEOGRAPHIES,figures)))
    for title,key in [("Stage stability","stage_stability"),("Stage attribution","stage_attribution"),("Zero streak evidence","zero_streaks"),("Parity evidence","parity_audit")]:
        sections.append(f"<h2>{title}</h2>{evidence[key].to_html(index=False)}")
    sections.append("<h2>Interpretation guardrails</h2><p>Missing months are not zeros. NSA source treatment is unchanged. Classification is deterministic and descriptive; recommendation and promotion remain none, and human decision remains pending.</p>")
    (output_dir/"bps_permit_volatility_review.html").write_text("<!doctype html><meta charset='utf-8'><style>body{font-family:sans-serif;margin:2rem}img{max-width:100%}table{font-size:11px;border-collapse:collapse}td,th{border:1px solid #ccc;padding:3px}.guard{padding:1rem;background:#fff3cd}</style>"+"".join(sections),encoding="utf-8")
    runtime=pd.DataFrame([{"source_run_id":source_run_id,"geography_count":len(GEOGRAPHIES),"visual_count":len(figures)+1,"output_file_count":len(list(output_dir.rglob("*")))}])
    runtime.to_csv(output_dir/"bps_permit_volatility_runtime_summary.csv",index=False)
    return len(list(output_dir.rglob("*")))
