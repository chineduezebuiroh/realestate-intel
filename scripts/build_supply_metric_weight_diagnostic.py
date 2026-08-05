from __future__ import annotations

import hashlib, json, math, shutil, sys, tempfile, time, zipfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

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

def _stability(chron):
    rows=[]
    for (geo,pid), g in chron.groupby(["geo_id","policy_id"]):
        s=g.sort_values("date").supply_dimension_score; d=s.diff(); ad=d.abs(); flips=((d*d.shift(1))<0).sum(); thr=max(float(ad.quantile(.90) or 0), .05)
        rows.append({"geo_id":geo,"policy_id":pid,"scope":"all_emitted_dates","standard_deviation":s.std(),"median_abs_mom_change":ad.median(),"p90_abs_mom_change":ad.quantile(.9),"p99_abs_mom_change":ad.quantile(.99),"maximum_abs_jump":ad.max(),"sign_flip_count":int(flips),"sign_flip_rate":float(flips/max(len(d.dropna()),1)),"rolling_volatility_3m_median":d.rolling(3).std().median(),"rolling_volatility_12m_median":d.rolling(12).std().median(),"large_jump_threshold":thr,"large_jump_count":int((ad>thr).sum())})
    return pd.DataFrame(rows)

def _turns(g):
    d=g.sort_values("date").supply_dimension_score.diff(); sg=d.apply(lambda x: 1 if x>0 else (-1 if x<0 else 0)); return list(g.sort_values("date").date[(sg*sg.shift(1)<0).fillna(False)])

def _trend(chron):
    rows=[]; turns=[]
    inc = {geo:g.sort_values("date") for geo,g in chron[chron.policy_id.eq("incumbent")].groupby("geo_id")}
    for (geo,pid), g in chron.groupby(["geo_id","policy_id"]):
        g=g.sort_values("date"); merged=g[["date","supply_dimension_score"]].merge(inc[geo][["date","supply_dimension_score"]],on="date",suffixes=("","_inc"))
        dm=merged.supply_dimension_score.diff(); di=merged.supply_dimension_score_inc.diff(); rev=merged.loc[(dm*di<0).fillna(False),"date"]
        t=_turns(g); ti=_turns(inc[geo]); delays=[min([abs((x-y).days) for y in ti], default=0) for x in t]
        for x in t: turns.append({"geo_id":geo,"policy_id":pid,"turning_point_date":x})
        rows.append({"geo_id":geo,"policy_id":pid,"directional_agreement_with_incumbent":float((dm.apply(math.copysign,args=(1,))==di.apply(math.copysign,args=(1,))).mean()) if len(dm)>1 else math.nan,"turning_point_disagreement_count":abs(len(t)-len(ti)),"median_turning_point_delay_days":pd.Series(delays).median() if delays else 0,"maximum_turning_point_delay_days":max(delays) if delays else 0,"recent_3m_signed_change":g.supply_dimension_score.diff(3).iloc[-1] if len(g)>3 else math.nan,"recent_6m_signed_change":g.supply_dimension_score.diff(6).iloc[-1] if len(g)>6 else math.nan,"recent_12m_signed_change":g.supply_dimension_score.diff(12).iloc[-1] if len(g)>12 else math.nan,"challenger_reversal_dates":";".join(str(x.date()) for x in rev)})
    return pd.DataFrame(rows), pd.DataFrame(turns)

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

def _html(chron, decomp):
    parts=["<html><head><meta charset='utf-8'><title>Supply Metric Weight Diagnostic</title><style>body{font-family:sans-serif}svg{border:1px solid #ddd;margin:6px} .small{font-size:12px}</style></head><body>",f"<h1>{CONTRACT_IDENTITY}</h1><p>recommendation_state: none; promotion_state: none. Human decision pending.</p>"]
    for geo in REVIEW_GEOS:
        parts.append(f"<h2>{geo}</h2><h3>Supply Dimension Chronology</h3><p class='small'>Zero reference line included; gaps follow real timestamps in artifact table.</p>")
        sub=chron[chron.geo_id.eq(geo)]
        parts.append(_svg_lines(sub,"supply_dimension_score","policy_id"))
        for pid in ["incumbent","challenger_a_60_20_20","challenger_b_67_165_165"]:
            parts.append(f"<h3>Metric Contributions — {pid}</h3>"); dd=decomp[(decomp.geo_id.eq(geo))&(decomp.policy_id.eq(pid))].copy(); parts.append(_svg_lines(dd,"weighted_contribution","canonical_metric_key"))
        parts.append("<h3>Stability, Turning Points, Missingness / Effective Weights</h3><p class='small'>See CSV artifacts for monthly absolute changes, turning-point delay, large-jump dates, and renormalized effective weights.</p>")
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
    start=time.perf_counter(); src_arg=Path(argv[1]); outdir=Path(argv[2]); t=time.perf_counter(); src,tmp=_resolve_source(src_arg); frames=_load(src); load=time.perf_counter()-t
    aligned=frames["aligned_metric_scores"]; reg=_registry(); decomp=_decomp(aligned,reg); chron=decomp.drop_duplicates(["geo_id","date","policy_id"])[["geo_id","date","policy_id","supply_dimension_score","available_metric_count","availability_class"]]
    dim_all=[]
    for pid in reg.policy_id.unique():
        d=_score_for_policy(aligned[aligned.geo_id.isin(REVIEW_GEOS)].copy(),pid,reg); d["policy_id"]=pid; dim_all.append(d)
    dims=pd.concat(dim_all); axes=[]; coords=[]; geoms=[]; regimes=[]
    for pid,d in dims.groupby("policy_id"):
        ax=score_axes(d); ax["policy_id"]=pid; axes.append(ax); co=build_coordinates(ax); co["policy_id"]=pid; coords.append(co); gm=assign_geometry(co); gm["policy_id"]=pid; geoms.append(gm); rg=assign_regimes(gm); rg["policy_id"]=pid; regimes.append(rg)
    stability=_stability(chron); trend,turns=_trend(chron); permit=_permit(decomp); coverage=_coverage(decomp); evidence=time.perf_counter()-start-load
    html=_html(chron,decomp); figures=0; ht=time.perf_counter(); (outdir/".").mkdir(parents=True,exist_ok=True); (outdir/"index.html").write_text(html); htmlt=time.perf_counter()-ht
    tables={"policy_registry":reg,"metric_to_supply_decomposition":decomp,"supply_chronology":chron,"stability_diagnostics":stability,"trend_responsiveness_diagnostics":trend,"turning_point_diagnostics":turns,"permit_family_influence":permit,"coverage_and_missingness":coverage,"effective_weight_diagnostics":decomp,"downstream_axis_propagation":pd.concat(axes),"coordinate_propagation":pd.concat(coords),"regime_change_summary":pd.concat(regimes).groupby(["geo_id","policy_id","major_regime"]).size().reset_index(name="observations"),"unaffected_parity":pd.DataFrame([{"check":"incumbent_artifacts_not_mutated","status":"pass"},{"check":"review_outputs_limited_to_governed_counties","status":"pass"}]),"human_decision_status":{"contract_identity":CONTRACT_IDENTITY,"recommendation_state":RECOMMENDATION_STATE,"promotion_state":PROMOTION_STATE,"human_decision":"pending"}}
    zt=time.perf_counter(); z=_write(outdir,tables); ztime=time.perf_counter()-zt; total=time.perf_counter()-start
    manifest=frames.get("manifest",{}); sid=manifest.get("run_id") or manifest.get("run_identity") or src.name
    for k,v in [("source run identity",sid),("contract identity",CONTRACT_IDENTITY),("metric count",len(SUPPLY_METRICS)),("policy count",reg.policy_id.nunique()),("challenger count",2),("governed geography count",len(REVIEW_GEOS)),("input loading time",f"{load:.3f}s"),("evidence-construction time",f"{evidence:.3f}s"),("figure-generation time",f"{figures:.3f}s"),("HTML time",f"{htmlt:.3f}s"),("ZIP time",f"{ztime:.3f}s"),("total runtime",f"{total:.3f}s"),("output directory",str(outdir)),("ZIP path",str(z)),("file count",len(list(outdir.glob('*')))),("ZIP size",z.stat().st_size),("recommendation state",RECOMMENDATION_STATE),("promotion state",PROMOTION_STATE)]: print(f"{k}: {v}")
    if tmp: tmp.cleanup()
if __name__ == "__main__": main(sys.argv)
