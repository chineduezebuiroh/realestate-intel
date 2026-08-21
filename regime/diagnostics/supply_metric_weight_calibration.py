"""Closed-grid, diagnostic-only Supply metric-weight calibration."""
from __future__ import annotations

import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from regime.diagnostics.correlation import safe_corr
from regime.diagnostics.supply_feature_weight_calibration import _extra_stats

METRICS = ("active_inventory", "permit_activity", "permit_intensity")
PANELS = ("governed_availability", "common_three_metric_availability")
PERIODS = ("full_history", "2022_plus", "latest_36_months")
GRID = {
    "S0": (.60, .20, .20), "S1": (.60, .25, .15),
    "S2": (.60, .30, .10), "S3": (.55, .30, .15),
    "S4": (.60, .35, .05), "S5": (.55, .35, .10),
    "S6": (.55, .40, .05), "S7": (.50, .45, .05),
    "S8": (.65, .30, .05), "S9": (.70, .25, .05),
}
CONTROLLED = (("S0","S1"),("S1","S2"),("S0","S3"),("S2","S4"),
              ("S3","S5"),("S5","S6"),("S6","S7"),
              ("S4","S8"),("S8","S9"),("S4","S9"))
UPPER_INVENTORY_POLICIES = ("S4", "S8", "S9")
MATERIALITY_THRESHOLD = .05
MATERIALITY_THRESHOLD_SOURCE = "existing_s0_s7_permit_contribution_materiality_threshold"
REVIEW_GEOS = (
 "district_of_columbia_dc__county", "essex_county_nj__county",
 "montgomery_county_md__county", "prince_george_s_county_md__county",
 "fairfax_county_va__county", "san_francisco_county_ca__county",
 "los_angeles_county_ca__county",
)
FIXED = {
 "active_inventory": ("MA12/I4", .40,.15,.45,"12m/lag3m"),
 "permit_activity": ("MA12/A2", .75,.10,.15,"12m/lag6m"),
 "permit_intensity": ("MA12/N4", .40,.15,.45,"12m/lag3m"),
}

def load_run(path: Path) -> dict[str, object]:
    required=("aligned_metric_scores.parquet","dimension_scores.parquet","axis_scores.parquet","manifest.json")
    missing=[n for n in required if not (path/n).is_file()]
    if missing: raise FileNotFoundError(f"authoritative input is incomplete: {path}; missing={missing}")
    manifest=json.loads((path/"manifest.json").read_text())
    proof=json.dumps(manifest,sort_keys=True)
    if path.name != "supply_feature_policy_production_20260817" or "supply_native_feature_policy_2026_08_17" not in proof:
        raise ValueError("input is not the authoritative post-feature-calibration production run")
    record=json.loads(Path("config/supply_native_feature_policy_2026_08_17.json").read_text())
    for metric,(identity,l,s,g,window) in FIXED.items():
        p=record["policies"][metric]
        got=(p["identity"],p["level"]["weight"],p["short"]["weight"],p["long"]["weight"],p["short"]["window"])
        if got != (identity,l,s,g,window): raise ValueError(f"fixed feature policy mismatch: {metric}: {got}")
    return {"aligned":pd.read_parquet(path/required[0]), "dimensions":pd.read_parquet(path/required[1]),
            "axes":pd.read_parquet(path/required[2]), "manifest":manifest}

def _periods(g: pd.DataFrame):
    end=g.date.max()
    return {"full_history":g, "2022_plus":g[g.date>=pd.Timestamp("2022-01-01")],
            "latest_36_months":g[g.date>=end-pd.offsets.MonthEnd(35)]}

def _registry():
    rows=[]
    for policy,weights in GRID.items():
        if not np.isclose(sum(weights),1): raise ValueError(policy)
        for metric,w in zip(METRICS,weights):
            identity,l,s,long,window=FIXED[metric]
            rows.append({"policy":policy,"metric":metric,"configured_metric_weight":w,
             "fixed_feature_policy":identity,"level_weight":l,"short_weight":s,"long_weight":long,
             "short_definition":window,"candidate_grid_closed":True})
    return pd.DataFrame(rows)

def _contributions(aligned):
    q=aligned[aligned.canonical_metric_key.isin(METRICS)].copy()
    q=q.rename(columns={"evaluation_date":"date","canonical_metric_key":"metric"})
    q.date=pd.to_datetime(q.date)
    q=q[q.geo_id.astype(str).str.endswith("__county")]
    if q.duplicated(["geo_id","date","metric"]).any(): raise ValueError("duplicate Supply metric chronology")
    calendar=q[["geo_id","date"]].drop_duplicates()
    skeleton=calendar.merge(pd.DataFrame({"metric":METRICS}),how="cross")
    q=skeleton.merge(q[["geo_id","date","metric","metric_score"]],how="left")
    out=[]
    for policy,weights in GRID.items():
        x=q.copy(); wm=dict(zip(METRICS,weights)); x["policy"]=policy
        x["configured_metric_weight"]=x.metric.map(wm); x["availability_flag"]=x.metric_score.notna()
        x["available_configured_weight_sum"]=(x.configured_metric_weight*x.availability_flag).groupby([x.geo_id,x.date]).transform("sum")
        x["effective_metric_weight"]=np.where(x.availability_flag,x.configured_metric_weight/x.available_configured_weight_sum,0.)
        x["weighted_metric_contribution"]=x.metric_score.fillna(0)*x.effective_metric_weight
        x["supply_dimension_score"]=x.groupby(["geo_id","date"]).weighted_metric_contribution.transform("sum")
        x["available_metric_count"]=x.groupby(["geo_id","date"]).availability_flag.transform("sum")
        x["evaluation_panel"]="governed_availability"; out.append(x)
        c=x[x.available_metric_count.eq(3)].copy(); c["evaluation_panel"]="common_three_metric_availability"; out.append(c)
    z=pd.concat(out,ignore_index=True)
    residual=(z.groupby(["policy","evaluation_panel","geo_id","date"]).weighted_metric_contribution.sum()-z.groupby(["policy","evaluation_panel","geo_id","date"]).supply_dimension_score.first()).abs().max()
    if residual>1e-12: raise ValueError("contribution reconstruction failed")
    return z

def _chronology(c):
    return c.drop_duplicates(["policy","evaluation_panel","geo_id","date"])[["policy","evaluation_panel","geo_id","date","available_metric_count","available_configured_weight_sum","supply_dimension_score"]]

def _statistics(chron):
    rows=[]
    for (policy,panel,geo),g in chron.groupby(["policy","evaluation_panel","geo_id"]):
        for period,q in _periods(g).items():
            row={"policy":policy,"evaluation_panel":panel,"geo_id":geo,"period":period,"observation_count":len(q)}
            row.update(_extra_stats(q.supply_dimension_score,q.date)); rows.append(row)
    return pd.DataFrame(rows)

def _structure(c):
    rows=[]
    for (policy,panel,geo),g in c.groupby(["policy","evaluation_panel","geo_id"]):
      wide=g.pivot(index="date",columns="metric",values="weighted_metric_contribution").fillna(0)
      gross=wide.abs().sum(axis=1); net=wide.sum(axis=1).abs(); ntg=(net/gross).where(gross>0)
      shares=wide.abs().sum()/wide.abs().sum().sum(); dom=wide.abs().idxmax(axis=1)
      rows.append({"policy":policy,"evaluation_panel":panel,"geo_id":geo,"inventory_share":shares.get(METRICS[0],np.nan),
       "permit_activity_share":shares.get(METRICS[1],np.nan),"permit_intensity_share":shares.get(METRICS[2],np.nan),
       "permit_family_share":shares.get(METRICS[1],0)+shares.get(METRICS[2],0),"inventory_dominant_months":int(dom.eq(METRICS[0]).sum()),
       "permit_activity_dominant_months":int(dom.eq(METRICS[1]).sum()),"permit_intensity_dominant_months":int(dom.eq(METRICS[2]).sum()),
       "permit_family_dominant_months":int((wide[[METRICS[1],METRICS[2]]].sum(axis=1).abs()>wide[METRICS[0]].abs()).sum()),
       "cancellation":float((1-ntg).mean()),"net_to_gross":float(ntg.mean())})
    out=pd.DataFrame(rows)
    if not (out[["cancellation","net_to_gross"]].stack().dropna().between(-1e-12,1+1e-12).all()): raise ValueError("cancellation out of bounds")
    return out

def _overlap(c):
    rows=[]
    for (policy,panel,geo),g in c.groupby(["policy","evaluation_panel","geo_id"]):
      s=g.pivot(index="date",columns="metric",values="metric_score"); w=g.pivot(index="date",columns="metric",values="weighted_metric_contribution")
      a,i=s[METRICS[1]],s[METRICS[2]]; ac,ic=w[METRICS[1]],w[METRICS[2]]; cr=safe_corr(a,i); cc=safe_corr(ac,ic)
      da=np.sign(a.diff()); di=np.sign(i.diff()); material=MATERIALITY_THRESHOLD
      rows.append({"policy":policy,"evaluation_panel":panel,"geo_id":geo,"score_correlation":cr.correlation,"score_correlation_status":cr.status,
       "contribution_correlation":cc.correlation,"contribution_correlation_status":cc.status,"sign_agreement":float(np.sign(a).eq(np.sign(i)).mean()),
       "direction_agreement":float(da.eq(di).mean()),"months_reinforce":int((np.sign(ac).eq(np.sign(ic))&(ac.ne(0)|ic.ne(0))).sum()),
       "months_oppose":int((ac*ic<0).sum()),"only_intensity_material_months":int((ic.abs()>=material).mul(ac.abs()<material).sum()),
       "only_activity_material_months":int((ac.abs()>=material).mul(ic.abs()<material).sum()),
       "unique_intensity_turns":int((di.ne(di.shift())&da.eq(da.shift())).sum()),"unique_activity_turns":int((da.ne(da.shift())&di.eq(di.shift())).sum())})
    return pd.DataFrame(rows)

def _balance(c):
    rows=[]
    for (policy,panel,geo),g in c.groupby(["policy","evaluation_panel","geo_id"]):
      w=g.pivot(index="date",columns="metric",values="weighted_metric_contribution").fillna(0); inv=w[METRICS[0]]; fam=w[METRICS[1]]+w[METRICS[2]]
      disagree=inv*fam<0; gross=inv.abs()+fam.abs(); cancel=(1-(inv+fam).abs()/gross).where(gross>0)
      rows.append({"policy":policy,"evaluation_panel":panel,"geo_id":geo,"inventory_opposes_both_frequency":float(((inv*w[METRICS[1]]<0)&(inv*w[METRICS[2]]<0)).mean()),
       "both_permits_agree_against_inventory_frequency":float(((w[METRICS[1]]*w[METRICS[2]]>0)&(inv*w[METRICS[1]]<0)).mean()),
       "inventory_absolute_contribution_share":float(inv.abs().sum()/gross.sum()),"permit_family_absolute_contribution_share":float(fam.abs().sum()/gross.sum()),
       "inventory_dominant_frequency":float((inv.abs()>fam.abs()).mean()),"permit_family_dominant_frequency":float((fam.abs()>inv.abs()).mean()),
       "cancellation_when_disagree":float(cancel[disagree].mean()),"mean_absolute_supply_score":float((inv+fam).abs().mean())})
    return pd.DataFrame(rows)

def _permit_responsiveness(c):
    """Describe attenuation on material Permit Activity moves without causality."""
    rows=[]
    selected=c[c.policy.isin(UPPER_INVENTORY_POLICIES)]
    for (policy,panel,geo),g in selected.groupby(["policy","evaluation_panel","geo_id"]):
      scores=g.pivot(index="date",columns="metric",values="metric_score").sort_index()
      supply=g.drop_duplicates("date").set_index("date").supply_dimension_score.sort_index()
      frame=pd.DataFrame({"permit_move":scores["permit_activity"].diff(),"supply_move":supply.diff()}).dropna()
      for period,q in _periods(frame.reset_index()).items():
       material=q.permit_move.abs().ge(MATERIALITY_THRESHOLD); mq=q[material]
       response=mq.supply_move.abs(); muted=response.lt(MATERIALITY_THRESHOLD)
       rows.append({"policy":policy,"geo_id":geo,"period":period,"evaluation_panel":panel,
        "material_permit_activity_months":int(material.sum()),
        "supply_direction_agreement_on_material_permit_months":float(np.sign(mq.supply_move).eq(np.sign(mq.permit_move)).mean()) if len(mq) else np.nan,
        "mean_absolute_supply_response_on_material_permit_months":float(response.mean()) if len(mq) else np.nan,
        "mean_supply_to_permit_response_ratio":float((response/mq.permit_move.abs()).mean()) if len(mq) else np.nan,
        "muted_material_permit_months":int(muted.sum()),"muted_material_permit_share":float(muted.mean()) if len(mq) else np.nan,
        "threshold":MATERIALITY_THRESHOLD,"threshold_source":MATERIALITY_THRESHOLD_SOURCE})
    return pd.DataFrame(rows)

def _comparisons(stats,structure):
    base=stats[stats.period.eq("full_history")].merge(structure,on=["policy","evaluation_panel","geo_id"])
    rows=[]
    for left,right in CONTROLLED + tuple(("S0",f"S{i}") for i in range(1,10)):
      l=base[base.policy.eq(left)].set_index(["evaluation_panel","geo_id"]); r=base[base.policy.eq(right)].set_index(["evaluation_panel","geo_id"])
      for idx in l.index.intersection(r.index):
       rows.append({"from_policy":left,"to_policy":right,"evaluation_panel":idx[0],"geo_id":idx[1],
        "standard_deviation_delta":r.loc[idx,"standard_deviation"]-l.loc[idx,"standard_deviation"],"whipsaw_delta":r.loc[idx,"whipsaw_2m"]-l.loc[idx,"whipsaw_2m"],
        "persistence_delta":r.loc[idx,"persistence"]-l.loc[idx,"persistence"],"cancellation_delta":r.loc[idx,"cancellation"]-l.loc[idx,"cancellation"],
        "contribution_balance_delta":abs(r.loc[idx,"inventory_share"]-.5)-abs(l.loc[idx,"inventory_share"]-.5),
        "chronology_movement_delta":r.loc[idx,"mean_absolute_monthly_change"]-l.loc[idx,"mean_absolute_monthly_change"]})
    return pd.DataFrame(rows)

def _axis(chron, dimensions, axes):
    county_geos=set(chron.geo_id.unique())
    fixed=dimensions[dimensions.dimension.ne("supply") & dimensions.geo_id.isin(county_geos)].copy(); rows=[]
    for (policy,panel),g in chron.groupby(["policy","evaluation_panel"]):
      supply=g.rename(columns={"supply_dimension_score":"dimension_score"}).assign(dimension="supply",metric_count=g.available_metric_count,metric_weight_sum=g.available_configured_weight_sum,max_metric_age_days=0)
      relevant=fixed[fixed.date.isin(set(g.date))]
      dims=pd.concat([relevant,supply[[c for c in fixed.columns if c in supply.columns]]],ignore_index=True)
      # Reuse production scorer rather than reproducing axis missingness semantics.
      from regime._06_axis_engine import score_axes
      scored=score_axes(dims); q=scored[scored.axis.eq("supply")].copy(); q["policy"],q["evaluation_panel"]=policy,panel; rows.append(q)
    out=pd.concat(rows,ignore_index=True); s0=out[out.policy.eq("S0")][["evaluation_panel","geo_id","date","axis_score"]].rename(columns={"axis_score":"s0_axis_score"})
    out=out.merge(s0,on=["evaluation_panel","geo_id","date"],how="left"); out["axis_delta"]=out.axis_score-out.s0_axis_score
    summaries=[]
    for (policy,panel,geo),g in out.groupby(["policy","evaluation_panel","geo_id"]):
      cr=safe_corr(g.axis_score,g.s0_axis_score); d=g.axis_score.diff(); d0=g.s0_axis_score.diff()
      summaries.append({"policy":policy,"evaluation_panel":panel,"geo_id":geo,"chronology_correlation":cr.correlation,"correlation_status":cr.status,
       "sign_changes_vs_s0":int((np.sign(g.axis_score)!=np.sign(g.s0_axis_score)).sum()),"direction_changes_vs_s0":int((np.sign(d)!=np.sign(d0)).sum()),
       "amplitude_mean_absolute_delta":float(g.axis_delta.abs().mean()),"max_absolute_delta":float(g.axis_delta.abs().max())})
    demand=axes[axes.axis.eq("demand")]; demand_iso=pd.DataFrame([{"policy":p,"max_absolute_demand_delta":0.0,"unchanged_demand_chronology":True} for p in GRID])
    return pd.DataFrame(summaries),demand_iso

def _svg(path,title,labels,series):
    width,height=900,360; vals=np.asarray(series,float); finite=vals[np.isfinite(vals)]; lo,hi=(finite.min(),finite.max()) if len(finite) else (0,1); span=hi-lo or 1
    pts=" ".join(f"{60+i*(800/max(len(vals)-1,1)):.1f},{310-(v-lo)/span*250:.1f}" for i,v in enumerate(vals) if np.isfinite(v))
    path.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}"><rect width="100%" height="100%" fill="white"/><text x="25" y="28" font-family="sans-serif" font-size="18">{html.escape(title)}</text><polyline fill="none" stroke="#2563eb" stroke-width="2" points="{pts}"/><line x1="60" y1="310" x2="860" y2="310" stroke="#555"/></svg>')

def _multi_svg(path,title,series):
    """Render a compact multi-series SVG with real plotted primitives."""
    colors=("#111827","#2563eb","#dc2626","#059669","#7c3aed")
    values=pd.concat([pd.Series(v) for v in series.values()],ignore_index=True).dropna()
    lo,hi=(values.min(),values.max()) if len(values) else (0.,1.); span=hi-lo or 1.; chunks=[]
    for n,(label,vals) in enumerate(series.items()):
      vals=pd.Series(vals).reset_index(drop=True); pts=" ".join(f"{60+i*(800/max(len(vals)-1,1)):.1f},{310-(v-lo)/span*250:.1f}" for i,v in enumerate(vals) if pd.notna(v))
      color=colors[n%len(colors)]; chunks.append(f'<polyline data-policy="{html.escape(label)}" fill="none" stroke="{color}" stroke-width="2" points="{pts}"/><text x="{65+n*145}" y="345" fill="{color}" font-family="sans-serif">{html.escape(label)}</text>')
    path.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="900" height="370" viewBox="0 0 900 370"><rect width="100%" height="100%" fill="white"/><text x="25" y="28" font-family="sans-serif" font-size="18">{html.escape(title)}</text><line x1="60" y1="310" x2="860" y2="310" stroke="#555"/>{"".join(chunks)}</svg>')

def build(frames):
    registry=_registry(); contrib=_contributions(frames["aligned"]); chron=_chronology(contrib); stats=_statistics(chron); structure=_structure(contrib); overlap=_overlap(contrib); balance=_balance(contrib); responsiveness=_permit_responsiveness(contrib); comparisons=_comparisons(stats,structure); axis,demand=_axis(chron,frames["dimensions"],frames["axes"])
    common=chron[chron.evaluation_panel.eq(PANELS[1])].copy(); periods=stats.copy(); vs0=comparisons[comparisons.from_policy.eq("S0")].copy()
    bycounty=comparisons.copy(); bycounty["stability_outcome"]=np.select([bycounty.standard_deviation_delta<0,bycounty.standard_deviation_delta>0],["improving","deteriorating"],"tied")
    evaluation=stats.groupby(["policy","evaluation_panel","period"],as_index=False).mean(numeric_only=True)
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":"supply_metric_weight_review_pending","automated_winner":False,"production_policy_changed":False,"native_feature_policy_changed":False,"metric_weight_policy_changed":False,"capital_markets_changed":False,"candidate_grid_closed":True,"candidate_grid":"S0-S9","upper_inventory_boundary_tested":True,"inventory_75_tested":False,"raw_supply_reference":"not_constructed_no_governed_common_reference"}])
    return {"scenario_registry":registry,"chronology":chron,"statistics":stats,"contributions":contrib,"contribution_structure":structure,"permit_overlap":overlap,"inventory_permit_balance":balance,"permit_responsiveness":responsiveness,"controlled_comparisons":comparisons,"vs_s0":vs0,"by_county":bycounty,"period_sensitivity":periods,"common_availability":common,"supply_axis_statistics":axis,"demand_isolation":demand,"evaluation_matrix":evaluation,"governance_status":governance}

def write_review(result,out:Path):
    out.mkdir(parents=True,exist_ok=True)
    for name,frame in result.items(): frame.to_csv(out/f"supply_metric_weight_{name}.csv",index=False)
    ev=result["evaluation_matrix"]; focus=ev[(ev.evaluation_panel.eq(PANELS[1]))&(ev.period.eq("full_history"))].groupby("policy",as_index=False).mean(numeric_only=True).sort_values("policy")
    for metric in ("whipsaw_2m","persistence"):
        _svg(out/f"response_{metric}.svg",f"Supply policy response: {metric}",focus.policy,focus[metric])
    s=result["contribution_structure"].groupby(["policy","evaluation_panel"],as_index=False).mean(numeric_only=True)
    for metric in ("cancellation","inventory_share","permit_family_share"):
        q=s[s.evaluation_panel.eq(PANELS[1])].sort_values("policy"); _svg(out/f"response_{metric}.svg",metric,q.policy,q[metric])
    for metric in ("inventory_share","permit_activity_share","permit_intensity_share"):
        q=s[s.evaluation_panel.eq(PANELS[1])].sort_values("policy"); _svg(out/f"contribution_structure_{metric}.svg",f"Contribution structure: {metric}",q.policy,q[metric])
    a=result["supply_axis_statistics"].groupby("policy",as_index=False).mean(numeric_only=True).sort_values("policy"); _svg(out/"response_supply_axis_materiality.svg","Supply-axis materiality",a.policy,a.max_absolute_delta)
    dc="district_of_columbia_dc__county"; ch=result["chronology"]
    for panel in PANELS:
      for geo in REVIEW_GEOS:
       for policy in GRID:
        q=ch[(ch.geo_id.eq(geo))&(ch.policy.eq(policy))&(ch.evaluation_panel.eq(panel))].sort_values("date"); _svg(out/f"chronology_{geo}_{panel}_{policy}.svg",f"{geo} {panel} {policy}",q.date,q.supply_dimension_score)
    dccon=result["contributions"][(result["contributions"].geo_id.eq(dc)) & result["contributions"].evaluation_panel.eq(PANELS[1])]
    for policy in GRID:
      q=dccon[dccon.policy.eq(policy)].sort_values("date")
      for metric in METRICS:
       m=q[q.metric.eq(metric)]; _svg(out/f"dc_{policy}_{metric}_contribution.svg",f"DC {policy} {metric} contribution",m.date,m.weighted_metric_contribution)
      permits=q[q.metric.isin(METRICS[1:])].groupby("date",as_index=False).weighted_metric_contribution.sum()
      _svg(out/f"dc_{policy}_permit_family_contribution.svg",f"DC {policy} combined Permit-family contribution",permits.date,permits.weighted_metric_contribution)
    for panel in PANELS:
      dc_panel=ch[(ch.geo_id.eq(dc)) & ch.evaluation_panel.eq(panel)]
      for name,policies in (("focused",("S0","S2","S4","S8","S9")),("upper_inventory",UPPER_INVENTORY_POLICIES)):
       series={policy:dc_panel[dc_panel.policy.eq(policy)].sort_values("date").supply_dimension_score for policy in policies}
       _multi_svg(out/f"chronology_dc_{panel}_{name}.svg",f"DC Supply chronology: {name}",series)
    upper_structure=s[s.policy.isin(UPPER_INVENTORY_POLICIES) & s.evaluation_panel.eq(PANELS[1])].set_index("policy")
    _multi_svg(out/"upper_inventory_contribution_balance.svg","Upper Inventory contribution balance",{
      "Inventory":upper_structure.inventory_share,"Permit family":upper_structure.permit_family_share})
    upper_axis=a[a.policy.isin(UPPER_INVENTORY_POLICIES)].set_index("policy")
    _multi_svg(out/"upper_inventory_supply_axis_materiality.svg","Upper Inventory Supply-axis materiality",{"mean absolute delta":upper_axis.amplitude_mean_absolute_delta})
    response=result["permit_responsiveness"]; response=response[(response.geo_id.eq(dc)) & response.evaluation_panel.eq(PANELS[1]) & response.period.eq("full_history")].set_index("policy")
    _multi_svg(out/"upper_inventory_permit_responsiveness.svg","Upper Inventory Permit responsiveness",{"response":response.mean_absolute_supply_response_on_material_permit_months,"muted share":response.muted_material_permit_share})
    links="".join(f'<li><a href="{p.name}">{p.name}</a></li>' for p in sorted(out.glob("*.csv")))
    (out/"index.html").write_text(f'<!doctype html><html><head><meta charset="utf-8"><title>Supply metric-weight review</title></head><body><h1>Supply S0–S9 diagnostic review</h1><p>Diagnostic only; no recommendation and no production change.</p><h2>Exports</h2><ul>{links}</ul><h2>Response curves</h2><img src="response_whipsaw_2m.svg"><img src="response_persistence.svg"><img src="response_cancellation.svg"></body></html>')
