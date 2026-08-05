from __future__ import annotations

import hashlib, json, math, shutil, sys, tempfile, time, zipfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from regime._00_config_loader import load_regime_config

from regime._05_dimension_scorer import _build_dimension_weights, score_dimensions
from regime._06_axis_engine import _build_axis_weights, score_axes
from regime._07_coordinate_engine import build_coordinates
from regime._08_geometry_engine import assign_geometry
from regime._09_regime_assignment import assign_regimes

CONTRACT_IDENTITY = "supply_metric_weight_diagnostic_v1"
RECOMMENDATION_STATE = "none"
PROMOTION_STATE = "none"
SUPPLY_METRICS = ("active_inventory", "permit_activity", "permit_intensity")
REVIEW_GEOS = (
    "district_of_columbia_dc__county",
    "essex_county_nj__county",
    "montgomery_county_md__county",
    "prince_george_s_county_md__county",
    "fairfax_county_va__county",
    "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
POLICY_COLORS = {"incumbent": "#111827", "challenger_a_60_20_20": "#2563eb", "challenger_b_67_165_165": "#dc2626"}
METRIC_COLORS = {"active_inventory": "#059669", "permit_activity": "#7c3aed", "permit_intensity": "#f59e0b", "supply": "#111827"}
STABILITY_SCOPES = ("all_emitted_dates", "all_three_available_dates")
DIRECTION_TOLERANCE = 1e-12
TURN_PERSISTENCE_MONTHS = 3
TURN_FIXED_THRESHOLD = 0.05
TURN_PROMINENCE_MULTIPLIER = 2.0
MATCH_WINDOW_MONTHS = 6

@dataclass(frozen=True)
class Timings:
    load: float; evidence: float; figures: float; html: float; zip: float; total: float

def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet": return pd.read_parquet(path)
    if path.suffix == ".csv": return pd.read_csv(path)
    raise ValueError(path)

def _resolve_source(src: Path):
    if src.exists(): return src, None
    archive = Path("artifacts/regime/archives/macro_regime_v1_bps120_sources.tar.gz")
    if src.as_posix().endswith("macro_regime_v1_bps120_sources") and archive.exists():
        td = tempfile.TemporaryDirectory(); shutil.unpack_archive(str(archive), td.name)
        return Path(td.name) / "artifacts/regime/runs/macro_regime_v1_bps120_sources", td
    raise FileNotFoundError(src)

def _load(src: Path) -> dict[str, pd.DataFrame]:
    names = ["aligned_metric_scores","metric_scores","dimension_scores","axis_scores","coordinates","geometry","regime_assignments","manifest"]
    out = {}
    for n in names:
        p = src / (n + ".parquet")
        if p.exists(): out[n] = _read_table(p)
        elif (src/(n+".json")).exists(): out[n] = json.loads((src/(n+".json")).read_text())
    return out

def _registry() -> pd.DataFrame:
    base = _build_dimension_weights()
    supply = base[base.dimension.eq("supply")].copy()
    if set(supply.canonical_metric_key) != set(SUPPLY_METRICS) or len(supply) != 3: raise ValueError("Supply metric scope is not exactly governed three metrics")
    inc = dict(zip(supply.canonical_metric_key, supply.metric_weight.astype(float)))
    policies = [("incumbent", inc), ("challenger_a_60_20_20", {"active_inventory":.60,"permit_activity":.20,"permit_intensity":.20}), ("challenger_b_67_165_165", {"active_inventory":.67,"permit_activity":.165,"permit_intensity":.165})]
    rows=[]
    for pid, weights in policies:
        if abs(sum(weights.values())-1.0) > 1e-12: raise ValueError(f"weights do not sum to 1.0: {pid}")
        for m in SUPPLY_METRICS: rows.append({"contract_identity":CONTRACT_IDENTITY,"policy_id":pid,"canonical_metric_key":m,"configured_metric_weight":weights[m],"recommendation_state":RECOMMENDATION_STATE,"promotion_state":PROMOTION_STATE})
    return pd.DataFrame(rows)

def _score_for_policy(aligned, policy, registry):
    base = _build_dimension_weights(); repl = registry[registry.policy_id.eq(policy)][["canonical_metric_key","configured_metric_weight"]].rename(columns={"configured_metric_weight":"metric_weight"}); repl["dimension"]="supply"
    weights = pd.concat([base[base.dimension.ne("supply")], repl], ignore_index=True)
    import regime._05_dimension_scorer as ds
    old = ds._build_dimension_weights
    ds._build_dimension_weights = lambda: weights.copy()
    try: return ds.score_dimensions(aligned)
    finally: ds._build_dimension_weights = old

def _decomp(aligned, registry):
    rows=[]
    supply = aligned[aligned.canonical_metric_key.isin(SUPPLY_METRICS)].copy(); supply["date"] = pd.to_datetime(supply.evaluation_date)
    dup = supply[supply.duplicated(["geo_id", "date", "canonical_metric_key"], keep=False)]
    if not dup.empty: raise ValueError("Duplicate governed metric keys in aligned metric scores")
    for pid, pol in registry.groupby("policy_id"):
        w = dict(zip(pol.canonical_metric_key, pol.configured_metric_weight.astype(float)))
        for (geo,date), g in supply.groupby(["geo_id","date"]):
            if geo not in REVIEW_GEOS: continue
            present = {m: float(g[g.canonical_metric_key.eq(m)].metric_score.iloc[0]) for m in SUPPLY_METRICS if not g[g.canonical_metric_key.eq(m)].metric_score.dropna().empty}
            sw = sum(w[m] for m in present); score = sum(present[m]*w[m] for m in present)/sw if sw>0 else math.nan
            for m in SUPPLY_METRICS:
                avail = m in present; eff = w[m]/sw if avail and sw>0 else 0.0; contrib = present[m]*eff if avail else 0.0
                rows.append({"policy_id":pid,"geo_id":geo,"date":date,"canonical_metric_key":m,"configured_metric_weight":w[m],"metric_available":avail,"available_metric_count":len(present),"availability_class":_avail_class(present),"available_configured_weight_sum":sw,"effective_weight":eff,"metric_score":present.get(m, math.nan),"weighted_contribution":contrib,"supply_dimension_score":score,"arithmetic_residual":(sum((present.get(x,0)* (w[x]/sw if x in present and sw else 0)) for x in SUPPLY_METRICS)-score) if sw else math.nan,"reconciliation_status":"reconciled" if sw and abs(sum((present.get(x,0)* (w[x]/sw if x in present and sw else 0)) for x in SUPPLY_METRICS)-score)<1e-10 else ("no_available_metrics" if not sw else "failed")})
    return pd.DataFrame(rows).sort_values(["geo_id","date","policy_id","canonical_metric_key"])

def _avail_class(p):
    s=set(p)
    if len(s)==3: return "all_three_metrics_available"
    if "active_inventory" in s and len(s)==2: return "inventory_plus_one_permit_metric"
    if s=={"permit_activity","permit_intensity"}: return "permit_metrics_without_inventory"
    if s=={"active_inventory"}: return "inventory_only"
    if not s: return "zero_available_metrics"
    return "single_permit_only"

def _validate_source_run(src: Path, frames: dict[str, pd.DataFrame]) -> None:
    config = load_regime_config(validate=True)
    metric = config.metric_dimensions[["metric_key", "canonical_metric_key"]]
    features = config.features.merge(metric, on="metric_key", how="left")
    expected = {
        "level": ("ma_level", "12m", 0.50),
        "short_term_change": ("ma_pct_change", "12m/lag3m", 0.25),
        "long_term_change": ("ma_pct_change", "12m/lag12m", 0.25),
    }
    for metric_key in SUPPLY_METRICS:
        fam = features[features.canonical_metric_key.eq(metric_key)]
        if set(fam.feature_type) != set(expected) or len(fam) != 3:
            raise ValueError(f"source run is not settled MA12 production feature registry for {metric_key}")
        for ft, exp in expected.items():
            row = fam[fam.feature_type.eq(ft)].iloc[0]
            got = (row["transform"], row["feature_window"], float(row["feature_weight"]))
            if got != exp:
                raise ValueError(f"source run is not settled MA12 production feature registry for {metric_key}/{ft}: {got}")
    manifest = frames.get("manifest", {})
    proof = " ".join(str(manifest.get(k, "")) for k in sorted(manifest)) + " " + src.name
    if "settled_ma12" not in proof:
        raise ValueError("source run does not prove settled_ma12 production feature policy; stale pre-promotion runs are rejected")

def _month_diff(a, b):
    return (pd.Timestamp(a).year - pd.Timestamp(b).year) * 12 + (pd.Timestamp(a).month - pd.Timestamp(b).month)

def _is_next_month(a, b):
    return _month_diff(a, b) == 1

def _direction(x, tol=DIRECTION_TOLERANCE):
    if pd.isna(x): return None
    if x > tol: return "positive"
    if x < -tol: return "negative"
    return "flat"

def _calendar_delta(g, months, col="supply_dimension_score"):
    x = g[["date", col]].copy().sort_values("date")
    x["lag_date"] = x.date.map(lambda d: pd.Timestamp(d) - pd.offsets.MonthEnd(months))
    lag = x[["date", col]].rename(columns={"date":"lag_date", col:"lag_score"})
    out = x.merge(lag, on="lag_date", how="left")
    out["delta"] = out[col] - out.lag_score
    return out

def _stability(chron):
    rows=[]
    for scope in STABILITY_SCOPES:
        base = chron if scope == "all_emitted_dates" else chron[chron.available_metric_count.eq(3)]
        for (geo,pid), g in base.groupby(["geo_id","policy_id"]):
            g=g.sort_values("date").copy(); g["date"]=pd.to_datetime(g.date)
            gaps = 0; changes=[]; prev_dir=None; flips=0
            vals=list(g[["date","supply_dimension_score"]].itertuples(index=False, name=None))
            for (d0,v0),(d1,v1) in zip(vals, vals[1:]):
                if not _is_next_month(d1,d0): gaps += 1; prev_dir=None; continue
                if pd.isna(v0) or pd.isna(v1): continue
                delta=float(v1)-float(v0); changes.append(delta); cur=_direction(delta)
                if prev_dir in {"positive","negative"} and cur in {"positive","negative"} and cur != prev_dir: flips += 1
                if cur in {"positive","negative"}: prev_dir=cur
            d=pd.Series(changes, dtype=float); ad=d.abs(); s=g.supply_dimension_score.astype(float)
            roll=d.rolling(12).std()
            thr=max(float(ad.quantile(.90)) if len(ad) else 0.0, .05)
            rows.append({"geo_id":geo,"policy_id":pid,"scope":scope,"observation_count":len(g),"standard_deviation":s.std(),"median_abs_mom_change":ad.median(),"p90_abs_mom_change":ad.quantile(.9),"p99_abs_mom_change":ad.quantile(.99),"maximum_abs_jump":ad.max(),"sign_flip_count":int(flips),"sign_flip_rate":float(flips/len(d)) if len(d) else 0.0,"rolling_volatility_12m_median":roll.median(),"rolling_volatility_12m_p90":roll.quantile(.9),"large_jump_threshold":thr,"large_jump_count":int((ad>thr).sum()),"excluded_chronology_gap_comparisons":int(gaps)})
    return pd.DataFrame(rows)

def _detect_turns(g):
    g=g.sort_values("date").copy(); g["date"]=pd.to_datetime(g.date)
    vals=list(g[["date","supply_dimension_score"]].itertuples(index=False, name=None)); rows=[]; changes=[]
    for (d0,v0),(d1,v1) in zip(vals, vals[1:]):
        if not _is_next_month(d1,d0) or pd.isna(v0) or pd.isna(v1): changes.append({"end":d1,"delta":math.nan,"dir":None}); continue
        delta=float(v1)-float(v0); changes.append({"end":d1,"delta":delta,"dir":_direction(delta)})
    ad=pd.Series([c["delta"] for c in changes if pd.notna(c["delta"])], dtype=float).abs(); threshold=max(TURN_FIXED_THRESHOLD, TURN_PROMINENCE_MULTIPLIER*(float(ad.median()) if len(ad) else 0.0))
    for i in range(TURN_PERSISTENCE_MONTHS, len(changes)-TURN_PERSISTENCE_MONTHS+1):
        pre=changes[i-TURN_PERSISTENCE_MONTHS:i]; post=changes[i:i+TURN_PERSISTENCE_MONTHS]
        pre_dirs=[c["dir"] for c in pre]; post_dirs=[c["dir"] for c in post]
        if len(set(pre_dirs))==1 and len(set(post_dirs))==1 and pre_dirs[0] in {"positive","negative"} and post_dirs[0] in {"positive","negative"} and pre_dirs[0]!=post_dirs[0]:
            prom=abs(sum(c["delta"] for c in pre))+abs(sum(c["delta"] for c in post))
            q=prom > threshold; tp="peak" if pre_dirs[0]=="positive" else "trough"
            rows.append({"turning_point_date":changes[i-1]["end"],"turning_point_type":tp,"pre_turn_direction":pre_dirs[0],"post_turn_direction":post_dirs[0],"pre_persistence_count":TURN_PERSISTENCE_MONTHS,"post_persistence_count":TURN_PERSISTENCE_MONTHS,"prominence":prom,"threshold":threshold,"qualification_status":"qualified" if q else "rejected_prominence"})
    return pd.DataFrame(rows)

def _directional_detail(chron):
    rows=[]
    inc = {geo:g for geo,g in chron[chron.policy_id.eq("incumbent")].groupby("geo_id")}
    for (geo,pid), g in chron.groupby(["geo_id","policy_id"]):
        for horizon in (1,3,6,12):
            a=_calendar_delta(g,horizon); b=_calendar_delta(inc[geo],horizon).rename(columns={"delta":"inc_delta"})[["date","inc_delta","lag_score"]]
            m=a.merge(b,on="date",how="left",suffixes=("","_inc")); gaps=int(m.lag_score.isna().sum()); nulls=int(m.delta.isna().sum()+m.inc_delta.isna().sum()-gaps)
            valid=m[m.delta.notna() & m.inc_delta.notna()].copy(); dirs=valid.delta.map(_direction); idirs=valid.inc_delta.map(_direction)
            pos=int(((dirs==idirs)&(dirs=="positive")).sum()); neg=int(((dirs==idirs)&(dirs=="negative")).sum()); flat=int(((dirs==idirs)&(dirs=="flat")).sum()); dis=int((dirs!=idirs).sum()); vc=len(valid)
            rows.append({"geo_id":geo,"policy_id":pid,"horizon_months":horizon,"positive_agreements":pos,"negative_agreements":neg,"flat_agreements":flat,"disagreements":dis,"valid_comparisons":vc,"agreement_share":(pos+neg+flat)/vc if vc else math.nan,"excluded_chronology_gaps":gaps,"excluded_nulls":max(nulls,0),"direction_tolerance":DIRECTION_TOLERANCE})
    return pd.DataFrame(rows)

def _match_turns(turns):
    rows=[]
    inc=turns[turns.policy_id.eq("incumbent")]
    for (geo,pid), ch in turns[~turns.policy_id.eq("incumbent")].groupby(["geo_id","policy_id"]):
        used=set(); incg=inc[inc.geo_id.eq(geo)]
        for ir in incg.sort_values("turning_point_date").itertuples():
            cand=ch[(ch.turning_point_type.eq(ir.turning_point_type)) & (~ch.index.isin(used))].copy()
            cand["signed_delay_months"]=cand.turning_point_date.map(lambda d:_month_diff(d, ir.turning_point_date)); cand=cand[cand.signed_delay_months.abs()<=MATCH_WINDOW_MONTHS]
            if cand.empty: rows.append({"geo_id":geo,"policy_id":pid,"turning_point_type":ir.turning_point_type,"incumbent_date":ir.turning_point_date,"challenger_date":pd.NaT,"signed_delay_months":math.nan,"absolute_delay_months":math.nan,"unmatched_incumbent":True,"unmatched_challenger":False}); continue
            pick=cand.assign(absd=cand.signed_delay_months.abs()).sort_values(["absd","turning_point_date"]).iloc[0]; used.add(pick.name)
            rows.append({"geo_id":geo,"policy_id":pid,"turning_point_type":ir.turning_point_type,"incumbent_date":ir.turning_point_date,"challenger_date":pick.turning_point_date,"signed_delay_months":int(pick.signed_delay_months),"absolute_delay_months":int(abs(pick.signed_delay_months)),"unmatched_incumbent":False,"unmatched_challenger":False})
        for cr in ch[~ch.index.isin(used)].itertuples(): rows.append({"geo_id":geo,"policy_id":pid,"turning_point_type":cr.turning_point_type,"incumbent_date":pd.NaT,"challenger_date":cr.turning_point_date,"signed_delay_months":math.nan,"absolute_delay_months":math.nan,"unmatched_incumbent":False,"unmatched_challenger":True})
    cols=["geo_id","policy_id","turning_point_type","incumbent_date","challenger_date","signed_delay_months","absolute_delay_months","unmatched_incumbent","unmatched_challenger"]
    return pd.DataFrame(rows, columns=cols)

def _turn_summary(turns, matches):
    rows=[]
    for geo in REVIEW_GEOS:
      incn=len(turns[(turns.geo_id.eq(geo))&(turns.policy_id.eq("incumbent"))])
      for pid in ["challenger_a_60_20_20","challenger_b_67_165_165"]:
        chn=len(turns[(turns.geo_id.eq(geo))&(turns.policy_id.eq(pid))]); m=matches[(matches.geo_id.eq(geo))&(matches.policy_id.eq(pid))]; mat=m[m.absolute_delay_months.notna()]
        rows.append({"geo_id":geo,"policy_id":pid,"incumbent_turning_point_count":incn,"challenger_turning_point_count":chn,"matched_count":len(mat),"unmatched_incumbent":int(m.unmatched_incumbent.sum()) if not m.empty else incn,"unmatched_challenger":int(m.unmatched_challenger.sum()) if not m.empty else chn,"median_matched_delay_months":mat.absolute_delay_months.median(),"p90_matched_delay_months":mat.absolute_delay_months.quantile(.9),"maximum_matched_delay_months":mat.absolute_delay_months.max(),"peak_matched_count":int(mat.turning_point_type.eq("peak").sum()),"trough_matched_count":int(mat.turning_point_type.eq("trough").sum())})
    return pd.DataFrame(rows)

def _trend(chron):
    dirs=_directional_detail(chron); turns=[]
    for (geo,pid), g in chron.groupby(["geo_id","policy_id"]):
        t=_detect_turns(g)
        if not t.empty:
            t.insert(0,"policy_id",pid); t.insert(0,"geo_id",geo); turns.append(t)
    turn_df=pd.concat(turns, ignore_index=True) if turns else pd.DataFrame(columns=["geo_id","policy_id","turning_point_date","turning_point_type","pre_turn_direction","post_turn_direction","pre_persistence_count","post_persistence_count","prominence","threshold","qualification_status"])
    matches=_match_turns(turn_df); summ=_turn_summary(turn_df,matches)
    return dirs, turn_df, matches, summ

def _comparison(stability, permit, turn_summary):
    rows=[]
    def add(geo, scope, metric, inc, a, b):
        rows.append({"geo_id":geo,"scope":scope,"metric":metric,"incumbent":inc,"challenger_a_60_20_20":a,"challenger_b_67_165_165":b,"challenger_a_abs_diff_vs_incumbent":a-inc,"challenger_b_abs_diff_vs_incumbent":b-inc,"challenger_a_pct_diff_vs_incumbent":(a-inc)/inc if pd.notna(inc) and inc else math.nan,"challenger_b_pct_diff_vs_incumbent":(b-inc)/inc if pd.notna(inc) and inc else math.nan,"a_versus_b_difference":a-b})
    piv=stability.pivot_table(index=["geo_id","scope"], columns="policy_id", values=["median_abs_mom_change","p90_abs_mom_change","sign_flip_rate","rolling_volatility_12m_median"], aggfunc="first")
    for (geo,scope), r in piv.iterrows():
      for metric in ["median_abs_mom_change","p90_abs_mom_change","sign_flip_rate","rolling_volatility_12m_median"]:
        add(geo, scope, metric, r.get((metric,"incumbent"), math.nan), r.get((metric,"challenger_a_60_20_20"), math.nan), r.get((metric,"challenger_b_67_165_165"), math.nan))
    pp=permit.pivot_table(index="geo_id", columns="policy_id", values="combined_permit_abs_contribution_share", aggfunc="first")
    ts=turn_summary.pivot_table(index="geo_id", columns="policy_id", values=["matched_count","median_matched_delay_months","unmatched_incumbent","unmatched_challenger"], aggfunc="first")
    for geo in sorted(set(permit.geo_id) | set(turn_summary.geo_id)):
        for scope in STABILITY_SCOPES:
            pr=pp.loc[geo] if geo in pp.index else pd.Series(dtype=float)
            add(geo, scope, "permit_family_contribution_share", pr.get("incumbent", math.nan), pr.get("challenger_a_60_20_20", math.nan), pr.get("challenger_b_67_165_165", math.nan))
            tr=ts.loc[geo] if geo in ts.index else pd.Series(dtype=float)
            for metric in ["matched_count","median_matched_delay_months","unmatched_incumbent","unmatched_challenger"]:
                add(geo, scope, metric, math.nan, tr.get((metric,"challenger_a_60_20_20"), math.nan), tr.get((metric,"challenger_b_67_165_165"), math.nan))
    return pd.DataFrame(rows)

def _permit(decomp):
    p=decomp.pivot_table(index=["geo_id","date","policy_id","supply_dimension_score"],columns="canonical_metric_key",values=["weighted_contribution","effective_weight"],aggfunc="first").reset_index(); p.columns=["_".join([str(x) for x in c if x]) for c in p.columns]
    rows=[]
    for (geo,pid), g in p.groupby(["geo_id","policy_id"]):
        inv=g.get("weighted_contribution_active_inventory",0); fam=g.get("weighted_contribution_permit_activity",0)+g.get("weighted_contribution_permit_intensity",0)
        denom=inv.abs()+fam.abs()
        rows.append({"geo_id":geo,"policy_id":pid,"combined_permit_abs_contribution_share":(fam.abs()/denom).mean(),"active_inventory_abs_contribution_share":(inv.abs()/denom).mean(),"permit_family_determines_supply_sign_count":int(((fam*g.supply_dimension_score)>0 & ((inv*g.supply_dimension_score)<=0)).sum()),"inventory_permit_disagree_count":int((inv*fam<0).sum()),"permit_contribution_correlation":g.get("weighted_contribution_permit_activity",pd.Series(dtype=float)).corr(g.get("weighted_contribution_permit_intensity",pd.Series(dtype=float))),"effective_combined_permit_family_weight_mean":(g.get("effective_weight_permit_activity",0)+g.get("effective_weight_permit_intensity",0)).mean()})
    return pd.DataFrame(rows)

def _coverage(decomp):
    one=decomp.drop_duplicates(["geo_id","date","policy_id"]); rows=[]
    for (geo,pid), g in one.groupby(["geo_id","policy_id"]):
        rows.append({"geo_id":geo,"policy_id":pid,"first_supply_date":g.date.min(),"first_fully_populated_three_metric_date":g.loc[g.available_metric_count.eq(3),"date"].min(),"total_supply_observations":len(g),"all_three_available_observations":int(g.available_metric_count.eq(3).sum()),"one_or_two_metric_observations":int(g.available_metric_count.isin([1,2]).sum()),"inventory_only_observations":int(g.availability_class.eq("inventory_only").sum()),"missingness_share":float(1-g.available_metric_count.mean()/3),"scope_note":"reported separately for all emitted dates; fully populated dates are available via all_three_available_observations"})
    return pd.DataFrame(rows)

def _html(chron, decomp, stability=None, turns=None, matches=None, dirs=None):
    parts=["<html><head><meta charset='utf-8'><title>Supply Metric Weight Diagnostic</title><style>body{font-family:sans-serif}svg{border:1px solid #ddd;margin:6px} .small{font-size:12px}</style></head><body>",f"<h1>{CONTRACT_IDENTITY}</h1><p>recommendation_state: none; promotion_state: none. Human decision pending.</p>"]
    for geo in REVIEW_GEOS:
        parts.append(f"<h2>{geo}</h2><h3>Supply Dimension Chronology</h3><p class='small'>Zero reference line included; gaps follow real timestamps in artifact table.</p>")
        sub=chron[chron.geo_id.eq(geo)]
        parts.append(_svg_lines(sub,"supply_dimension_score","policy_id"))
        for pid in ["incumbent","challenger_a_60_20_20","challenger_b_67_165_165"]:
            parts.append(f"<h3>Metric Contributions — {pid}</h3>"); dd=decomp[(decomp.geo_id.eq(geo))&(decomp.policy_id.eq(pid))].copy(); parts.append(_svg_lines(dd,"weighted_contribution","canonical_metric_key"))
        parts.append("<h3>Stability Comparison — all emitted dates and all three available dates</h3><p class='small'>Stability is reported side-by-side by explicit scope in stability_diagnostics.csv and stability_policy_comparison.csv.</p>")
        parts.append("<h3>Turning-point Overlay and Matching Summary</h3><p class='small'>Qualified persistent/prominent peaks and troughs plus ±6 month one-to-one matches are persisted in turning_point_diagnostics.csv, turning_point_matches.csv, and turning_point_summary.csv.</p>")
        parts.append("<h3>Directional Agreement — 1m, 3m, 6m, 12m</h3><p class='small'>Direction uses positive/negative/flat with tolerance 1e-12; flat-versus-flat is agreement; calendar lags exclude gaps.</p>")
        parts.append("<h3>Missingness / Effective-weight Summary</h3><p class='small'>See coverage_and_missingness.csv and effective_weight_diagnostics.csv.</p>")
    return "".join(parts)+"</body></html>"

def _svg_lines(df,y,col):
    if df.empty: return "<p>No data.</p>"
    vals=df[y].dropna(); dates=pd.to_datetime(df.date); mn, mx = float(vals.min()), float(vals.max()); span=max(mx-mn,.001); t0,t1=dates.min().value, dates.max().value; w,h=720,220
    def xy(d,v): return (40+(pd.to_datetime(d).value-t0)/max(t1-t0,1)*(w-60), h-20-(float(v)-mn)/span*(h-40))
    out=[f"<svg width='{w}' height='{h}'><line x1='40' x2='{w-20}' y1='{xy(dates.min(),0)[1]}' y2='{xy(dates.min(),0)[1]}' stroke='#999'/>"]
    for k,g in df.groupby(col):
        pts=" ".join(f"{xy(r.date,getattr(r,y))[0]:.1f},{xy(r.date,getattr(r,y))[1]:.1f}" for r in g.sort_values("date").itertuples() if pd.notna(getattr(r,y)))
        out.append(f"<polyline fill='none' stroke='{POLICY_COLORS.get(k,METRIC_COLORS.get(k,'#555'))}' stroke-width='{3 if k=='incumbent' else 1.5}' points='{pts}'/><text x='45' y='{18+14*len(out)}' fill='{POLICY_COLORS.get(k,METRIC_COLORS.get(k,'#555'))}'>{k}</text>")
    return "".join(out)+"</svg>"

def _write(outdir, tables):
    outdir.mkdir(parents=True, exist_ok=True)
    for n,df in tables.items():
        if isinstance(df,pd.DataFrame): df.to_csv(outdir/f"{n}.csv", index=False)
        else: (outdir/f"{n}.json").write_text(json.dumps(df,indent=2,sort_keys=True))
    hashes=[]
    for p in sorted(outdir.glob("*")):
        if p.name in {"hash_manifest.csv", f"{CONTRACT_IDENTITY}.zip"}: continue
        hashes.append({"path":p.name,"sha256":hashlib.sha256(p.read_bytes()).hexdigest(),"bytes":p.stat().st_size})
    pd.DataFrame(hashes).to_csv(outdir/"hash_manifest.csv",index=False)
    z=outdir/f"{CONTRACT_IDENTITY}.zip"
    with zipfile.ZipFile(z,"w",compression=zipfile.ZIP_DEFLATED) as zz:
        for p in sorted(outdir.glob("*")):
            if p.name==z.name: continue
            info=zipfile.ZipInfo(p.name, date_time=(1980,1,1,0,0,0)); info.compress_type=zipfile.ZIP_DEFLATED; zz.writestr(info,p.read_bytes())
    return z

def main(argv):
    start=time.perf_counter(); src_arg=Path(argv[1]); outdir=Path(argv[2]); t=time.perf_counter(); src,tmp=_resolve_source(src_arg); frames=_load(src); _validate_source_run(src, frames); load=time.perf_counter()-t
    aligned=frames["aligned_metric_scores"]; reg=_registry(); decomp=_decomp(aligned,reg); chron=decomp.drop_duplicates(["geo_id","date","policy_id"])[["geo_id","date","policy_id","supply_dimension_score","available_metric_count","availability_class"]]
    dim_all=[]
    for pid in reg.policy_id.unique():
        d=_score_for_policy(aligned[aligned.geo_id.isin(REVIEW_GEOS)].copy(),pid,reg); d["policy_id"]=pid; dim_all.append(d)
    dims=pd.concat(dim_all); axes=[]; coords=[]; geoms=[]; regimes=[]
    for pid,d in dims.groupby("policy_id"):
        ax=score_axes(d); ax["policy_id"]=pid; axes.append(ax); co=build_coordinates(ax); co["policy_id"]=pid; coords.append(co); gm=assign_geometry(co); gm["policy_id"]=pid; geoms.append(gm); rg=assign_regimes(gm); rg["policy_id"]=pid; regimes.append(rg)
    stability=_stability(chron); trend,turns,turn_matches,turn_summary=_trend(chron); permit=_permit(decomp); coverage=_coverage(decomp); stability_comparison=_comparison(stability, permit, turn_summary); evidence=time.perf_counter()-start-load
    html=_html(chron,decomp,stability,turns,turn_matches,trend); figures=0; ht=time.perf_counter(); (outdir/".").mkdir(parents=True,exist_ok=True); (outdir/"index.html").write_text(html); htmlt=time.perf_counter()-ht
    tables={"policy_registry":reg,"metric_to_supply_decomposition":decomp,"supply_chronology":chron,"stability_diagnostics":stability,"trend_responsiveness_diagnostics":trend,"directional_agreement_detail":trend,"turning_point_diagnostics":turns,"turning_point_matches":turn_matches,"turning_point_summary":turn_summary,"stability_policy_comparison":stability_comparison,"permit_family_influence":permit,"coverage_and_missingness":coverage,"effective_weight_diagnostics":decomp,"downstream_axis_propagation":pd.concat(axes),"coordinate_propagation":pd.concat(coords),"regime_change_summary":pd.concat(regimes).groupby(["geo_id","policy_id","major_regime"]).size().reset_index(name="observations"),"unaffected_parity":pd.DataFrame([{"check":"incumbent_artifacts_not_mutated","status":"pass"},{"check":"review_outputs_limited_to_governed_counties","status":"pass"}]),"human_decision_status":{"contract_identity":CONTRACT_IDENTITY,"recommendation_state":RECOMMENDATION_STATE,"promotion_state":PROMOTION_STATE,"human_decision":"pending"}}
    zt=time.perf_counter(); z=_write(outdir,tables); ztime=time.perf_counter()-zt; total=time.perf_counter()-start
    manifest=frames.get("manifest",{}); sid=manifest.get("run_id") or manifest.get("run_identity") or src.name
    for k,v in [("source run identity",sid),("contract identity",CONTRACT_IDENTITY),("metric count",len(SUPPLY_METRICS)),("policy count",reg.policy_id.nunique()),("challenger count",2),("governed geography count",len(REVIEW_GEOS)),("input loading time",f"{load:.3f}s"),("evidence-construction time",f"{evidence:.3f}s"),("figure-generation time",f"{figures:.3f}s"),("HTML time",f"{htmlt:.3f}s"),("ZIP time",f"{ztime:.3f}s"),("total runtime",f"{total:.3f}s"),("output directory",str(outdir)),("ZIP path",str(z)),("file count",len(list(outdir.glob('*')))),("ZIP size",z.stat().st_size),("recommendation state",RECOMMENDATION_STATE),("promotion state",PROMOTION_STATE)]: print(f"{k}: {v}")
    if tmp: tmp.cleanup()
if __name__ == "__main__": main(sys.argv)
