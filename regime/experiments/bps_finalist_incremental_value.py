"""Diagnostic-only incremental-information comparison of the two BPS finalists."""
from __future__ import annotations

import html
from pathlib import Path
import numpy as np
import pandas as pd
from PIL import Image, ImageDraw

from regime.diagnostics.bps_permit_volatility import GEOGRAPHIES, TOLERANCE, _production_contract
from regime.experiments.bps_feature_weight_comparison import _chronology as weight_chronology
from regime.experiments.bps_short_horizon_comparison import _scale_valid_turns
from regime.diagnostics.capital_markets_ma import match_turning_points

POLICIES={"BPS-FINAL-70":(.70,.15,.15),"BPS-FINAL-80":(.80,.10,.10)}
FEATURES=("level","short","long")
SUPPLY_METRIC_WEIGHT=.20
DECISION_ID="bps_finalist_incremental_value_2026_08_09"

def policy_registry():
    return pd.DataFrame([{"policy_id":p,"level_weight":w[0],"short_weight":w[1],"long_weight":w[2],
        "level_formula":"MA12(raw bps_total_units)","short_formula":"MA12 / lag6(MA12) - 1","long_formula":"MA12 / lag12(MA12) - 1",
        "short_horizon":"lag6","long_horizon":"lag12","transform_family":"ratio","normalization_method":"expanding_percentile",
        "normalization_polarity":"positive","supply_metric_weight":SUPPLY_METRIC_WEIGHT,"scope":"BPS-only","production_status":"diagnostic"} for p,w in POLICIES.items()])

def _weighted(frame, weights, keep=FEATURES):
    available={f:frame[f"normalized_{f}_score"].notna() if f in keep else pd.Series(False,index=frame.index) for f in FEATURES}
    total=sum(available[f]*weights[i] for i,f in enumerate(FEATURES))
    contrib={}; effective={}
    for i,f in enumerate(FEATURES):
        effective[f]=np.where(available[f],weights[i]/total.replace(0,np.nan),0.)
        contrib[f]=frame[f"normalized_{f}_score"].fillna(0)*effective[f]
    return sum(contrib.values()).where(total.gt(0)),effective,contrib

def _chronology(source):
    prior,_=weight_chronology(source); frames=[]
    mapping={"BPS-FINAL-70":"BPS-W-70-15-15","BPS-FINAL-80":"BPS-W-80-10-10"}
    parity=[]
    for policy,weights in POLICIES.items():
        old=prior.query("policy_id == @mapping[@policy]").copy(); frame=old.drop(columns=["policy_id","metric_score","effective_level_weight","effective_short_weight","effective_long_weight","level_contribution","short_contribution","long_contribution"])
        score,effective,contrib=_weighted(frame,weights); frame.insert(0,"policy_id",policy)
        for f in FEATURES: frame[f"effective_{f}_weight"]=effective[f]; frame[f"{f}_contribution"]=contrib[f]
        frame["metric_score"]=score
        delta=(frame.metric_score.to_numpy()-old.metric_score.to_numpy()); maximum=np.nanmax(np.abs(delta))
        parity.append({"policy_id":policy,"prior_policy_id":mapping[policy],"field":"metric_score","max_abs_difference":maximum,"tolerance":TOLERANCE,"status":"pass" if maximum<=TOLERANCE else "fail"}); frames.append(frame)
    chron=pd.concat(frames,ignore_index=True)
    err=(chron[[f"{f}_contribution" for f in FEATURES]].sum(axis=1)-chron.metric_score).abs().dropna()
    if len(err) and err.max()>TOLERANCE: raise AssertionError("score reconstruction failed")
    base=chron.query("policy_id == 'BPS-FINAL-70'").set_index(["geo_id","date"])
    other=chron.query("policy_id == 'BPS-FINAL-80'").set_index(["geo_id","date"])
    if (base[[f"normalized_{f}_score" for f in FEATURES]]-other[[f"normalized_{f}_score" for f in FEATURES]]).abs().max().max()>TOLERANCE: raise AssertionError("normalized chronology differs")
    return chron,pd.DataFrame(parity)

def _ablations(chron):
    short=[]; momentum=[]
    for policy,g in chron.groupby("policy_id"):
        w=POLICIES[policy]; ns,_,_=_weighted(g,w,("level","long")); nm,_,_=_weighted(g,w,("level",))
        common=g[["policy_id","geo_id","date"]].copy(); s=common.copy(); s["full_metric_score"]=g.metric_score; s["no_short_metric_score"]=ns; s["incremental_short_effect"]=s.full_metric_score-ns; s["absolute_incremental_short_effect"]=s.incremental_short_effect.abs(); short.append(s)
        m=common.copy(); m["full_metric_score"]=g.metric_score; m["no_momentum_metric_score"]=nm; m["incremental_momentum_effect"]=m.full_metric_score-nm; m["absolute_incremental_momentum_effect"]=m.incremental_momentum_effect.abs(); momentum.append(m)
    short=pd.concat(short); momentum=pd.concat(momentum)
    def summary(x,col,prefix):
        rows=[]
        for p,g in x.groupby("policy_id"):
            v=g[col].dropna(); r=g.sort_values("date").groupby("geo_id").tail(36)[col].dropna(); rows.append({"policy_id":p,f"median_abs_{prefix}_effect":v.median(),f"p90_abs_{prefix}_effect":v.quantile(.9),f"p99_abs_{prefix}_effect":v.quantile(.99),f"max_abs_{prefix}_effect":v.max(),f"recent_36m_median_abs_{prefix}_effect":r.median(),f"recent_36m_p90_abs_{prefix}_effect":r.quantile(.9)})
        return pd.DataFrame(rows)
    return short,summary(short,"absolute_incremental_short_effect","short"),momentum,summary(momentum,"absolute_incremental_momentum_effect","momentum")

def _movement(chron):
    rows=[]
    for (p,geo),g in chron.sort_values("date").groupby(["policy_id","geo_id"]):
        x=g[["date","metric_score"]+[f"{f}_contribution" for f in FEATURES]].copy(); x.insert(0,"geo_id",geo); x.insert(0,"policy_id",p); x["metric_delta"]=x.metric_score.diff()
        for f in FEATURES: x[f"{f}_contribution_delta"]=x[f"{f}_contribution"].diff()
        x=x.dropna(subset=["metric_delta"]); x["dominant_driver"]=x[[f"{f}_contribution_delta" for f in FEATURES]].abs().idxmax(axis=1).str.split("_").str[0]; rows.append(x)
    movement=pd.concat(rows); audit=[]
    for (p,g),x in movement.groupby(["policy_id","geo_id"]):
        shares=x.dominant_driver.value_counts(normalize=True); audit.append({"policy_id":p,"geo_id":g,"level_dominant_share":shares.get("level",0),"short_dominant_share":shares.get("short",0),"long_dominant_share":shares.get("long",0)})
    return movement,pd.DataFrame(audit)

def _turn_evidence(chron,short_ab,momentum_ab):
    rows=[]; leads=[]
    for (p,geo),g in chron.groupby(["policy_id","geo_id"]):
        g=g.sort_values("date"); ref=_scale_valid_turns(g,"ma12_level"); full=_scale_valid_turns(g,"metric_score")
        ns=short_ab.query("policy_id==@p and geo_id==@geo"); nm=momentum_ab.query("policy_id==@p and geo_id==@geo")
        no_short=_scale_valid_turns(ns,"no_short_metric_score"); no_mom=_scale_valid_turns(nm,"no_momentum_metric_score")
        mf=match_turning_points(ref,full,12); ms=match_turning_points(ref,no_short,12); mm=match_turning_points(ref,no_mom,12)
        for i,r in ref.reset_index(drop=True).iterrows():
            def hit(m):
                q=m.iloc[i] if i<len(m) else pd.Series(); return bool(q.get("matched",False)),q.get("signed_delay_months",np.nan)
            hf,lf=hit(mf); hs,ls=hit(ms); hm,lm=hit(mm); delayed=hf and hs and ls>lf; missed=hf and not hs
            rows.append({"policy_id":p,"geo_id":geo,"reference_turn_date":r.turning_point_date,"turning_point_type":r.turning_point_type,"reference_turn_count":1,"full_matched_turn_count":int(hf),"no_short_matched_turn_count":int(hs),"no_momentum_matched_turn_count":int(hm),"turns_delayed_without_short":int(delayed),"turns_missed_without_short":int(missed),"false_turns_removed_without_short":max(len(full)-len(no_short),0),"added_lag_without_short":ls-lf if hf and hs else np.nan,"matching_contract":"persistent prominence-qualified; same-type deterministic one-to-one ±12m"})
            date=pd.Timestamp(r.turning_point_date); before=g[pd.to_datetime(g.date)<date].tail(6); direction=-1 if r.turning_point_type=="peak" else 1; changes=before.normalized_short_score.diff(); candidates=changes[changes*direction>0]
            lead=int((date-pd.to_datetime(before.loc[candidates.index[-1],"date"])).days/30.4375) if len(candidates) else np.nan
            leads.append({"policy_id":p,"geo_id":geo,"reference_turn_date":date,"turning_point_type":r.turning_point_type,"short_leads":bool(len(candidates)),"lead_months":lead})
    turn=pd.DataFrame(rows); lead=pd.DataFrame(leads)
    return turn,lead

def build_evidence(source,source_run_id):
    _production_contract(); chron,parity=_chronology(source)
    if set(chron.geo_id)!=set(GEOGRAPHIES): raise ValueError("exact seven-geography panel required")
    short,short_sum,momentum,momentum_sum=_ablations(chron); movement,drivers=_movement(chron); turns,lead=_turn_evidence(chron,short,momentum)
    noise=[]
    for (p,geo),g in movement.groupby(["policy_id","geo_id"]):
        st=g.short_contribution_delta.abs().quantile(.75); mt=g.metric_delta.abs().quantile(.25); x=g.copy(); x["material_short_threshold_p75"]=st; x["minimal_metric_threshold_p25"]=mt; x["material_short_move"]=x.short_contribution_delta.abs().ge(st); x["minimal_metric_response"]=x.metric_delta.abs().le(mt); x["noise_event"]=x.material_short_move&x.minimal_metric_response; noise.append(x)
    noise=pd.concat(noise)
    wide=chron.pivot(index=["geo_id","date"],columns="policy_id",values="metric_score").reset_index(); wide["metric_70"]=wide["BPS-FINAL-70"]; wide["metric_80"]=wide["BPS-FINAL-80"]; wide["difference"]=wide.metric_70-wide.metric_80; wide["absolute_difference"]=wide.difference.abs(); threshold=wide.absolute_difference.quantile(.95)
    divergence=wide[["geo_id","date","metric_70","metric_80","difference","absolute_difference"]]; extreme=divergence[divergence.absolute_difference>=threshold].copy()
    base=chron.query("policy_id=='BPS-FINAL-70'").set_index(["geo_id","date"]); ext=[]
    turn_dates=set(pd.to_datetime(turns.reference_turn_date)); t70=set(pd.to_datetime(turns.query("policy_id=='BPS-FINAL-70'").reference_turn_date)); t80=set(pd.to_datetime(turns.query("policy_id=='BPS-FINAL-80'").reference_turn_date))
    for _,r in extreme.iterrows():
        b=base.loc[(r.geo_id,r.date)]; near=lambda dates:any(abs((pd.Timestamp(r.date)-d).days)<=93 for d in dates); structural=near(turn_dates); classification="useful_momentum_differentiation" if structural else ("likely_noise_differentiation" if abs(b.short_contribution)<abs(b.level_contribution)*.25 else "ambiguous")
        b80=chron[(chron.policy_id=='BPS-FINAL-80')&(chron.geo_id==r.geo_id)&(chron.date==r.date)].iloc[0]
        ext.append({**r.to_dict(),"level_score":b.normalized_level_score,"short_score":b.normalized_short_score,"long_score":b.normalized_long_score,**{f"{f}_contribution_70":b[f"{f}_contribution"] for f in FEATURES},**{f"{f}_contribution_80":b80[f"{f}_contribution"] for f in FEATURES},"structural_turn_nearby":structural,"qualified_metric_turn_70_nearby":near(t70),"qualified_metric_turn_80_nearby":near(t80),"classification":classification,"classification_evidence":"within ±3 months of structural turn" if structural else "distribution-derived divergence without nearby structural turn"})
    extreme=pd.DataFrame(ext)
    info=[]; stability=[]; recent=[]
    for p in POLICIES:
        n=noise.query("policy_id==@p"); material=n[n.material_short_move]; useful=material[material.date.apply(lambda d:any(abs((pd.Timestamp(d)-x).days)<=93 for x in turn_dates))]; ambiguous=max(len(material)-len(useful)-int(material.noise_event.sum()),0)
        info.append({"policy_id":p,"material_short_events":len(material),"useful_short_events":len(useful),"noise_short_events":int(material.noise_event.sum()),"ambiguous_short_events":ambiguous,"useful_share":len(useful)/len(material) if len(material) else np.nan,"noise_share":material.noise_event.mean() if len(material) else np.nan})
        g=chron.query("policy_id==@p").sort_values(["geo_id","date"]); d=g.groupby("geo_id").metric_score.diff(); tt=turns.query("policy_id==@p"); added=tt.added_lag_without_short.dropna(); stability.append({"policy_id":p,"metric_median_abs_mom":d.abs().median(),"metric_p90_abs_mom":d.abs().quantile(.9),"metric_p99_abs_mom":d.abs().quantile(.99),"metric_max_abs_jump":d.abs().max(),"metric_sign_flips":int((np.sign(g.metric_score)!=np.sign(g.groupby('geo_id').metric_score.shift())).sum()),"metric_rolling_12m_volatility":g.groupby('geo_id').metric_score.rolling(12,min_periods=2).std().median(),"metric_turning_points":int(tt.full_matched_turn_count.sum()),"latest_36m_turns":int(tt[pd.to_datetime(tt.reference_turn_date)>=pd.to_datetime(g.date).max()-pd.DateOffset(months=36)].full_matched_turn_count.sum()),"median_responsiveness_lag":added.median(),"p90_responsiveness_lag":added.quantile(.9),"share_within_3_months":added.abs().le(3).mean() if len(added) else np.nan})
        rg=g.groupby("geo_id").tail(36); rn=n.sort_values("date").groupby("geo_id").tail(35); rs=short.query("policy_id==@p").sort_values('date').groupby('geo_id').tail(36); recent.append({"policy_id":p,"metric_volatility":rg.groupby('geo_id').metric_score.std().median(),"turn_count":stability[-1]['latest_36m_turns'],"largest_jump":rg.groupby('geo_id').metric_score.diff().abs().max(),"median_abs_short_effect":rs.absolute_incremental_short_effect.median(),"p90_abs_short_effect":rs.absolute_incremental_short_effect.quantile(.9),"short_dominant_share":rn.dominant_driver.eq('short').mean(),"noise_share":rn.noise_event.mean(),"useful_short_event_count":len(useful[useful.date>=pd.to_datetime(g.date).max()-pd.DateOffset(months=36)])})
    info=pd.DataFrame(info); stability=pd.DataFrame(stability); recent=pd.DataFrame(recent)
    decisions=[]
    for p,w in POLICIES.items():
        s=stability.query('policy_id==@p').iloc[0]; ss=short_sum.query('policy_id==@p').iloc[0]; tr=turns.query('policy_id==@p'); inf=info.query('policy_id==@p').iloc[0]; ex=extreme.classification.value_counts(); decisions.append({"Policy":p,"Level weight":w[0],"Short weight":w[1],"Long weight":w[2],"Metric median abs MoM":s.metric_median_abs_mom,"Metric P90":s.metric_p90_abs_mom,"Metric P99":s.metric_p99_abs_mom,"Metric max jump":s.metric_max_abs_jump,"Metric sign flips":s.metric_sign_flips,"Metric turning points":s.metric_turning_points,"Recent-36m metric volatility":recent.query('policy_id==@p').iloc[0].metric_volatility,"Short dominant movement share":drivers.query('policy_id==@p').short_dominant_share.median(),"Median abs short ablation effect":ss.median_abs_short_effect,"P90 abs short ablation effect":ss.p90_abs_short_effect,"Useful short-event share":inf.useful_share,"Short noise share":inf.noise_share,"Full matched structural turns":tr.full_matched_turn_count.sum(),"Turns missed without short":tr.turns_missed_without_short.sum(),"Turns delayed without short":tr.turns_delayed_without_short.sum(),"Median added lag without short":tr.added_lag_without_short.median(),"P90 added lag without short":tr.added_lag_without_short.quantile(.9),"Extreme divergence months classified useful":ex.get('useful_momentum_differentiation',0),"Extreme divergence months classified noise":ex.get('likely_noise_differentiation',0),"Extreme divergence months ambiguous":ex.get('ambiguous',0),"Decision":"pending"})
    status=pd.DataFrame([{"decision_id":DECISION_ID,"source_run_id":source_run_id,"recommendation_state":"none","promotion_state":"none","human_decision":"pending","automated_winner":False}])
    return {"policy_registry":policy_registry(),"policy_chronology":chron,"short_ablation_chronology":short,"short_ablation_summary":short_sum,"momentum_ablation_chronology":momentum,"momentum_ablation_summary":momentum_sum,"dominant_driver_audit":drivers,"turn_ablation_audit":turns,"short_lead_value_audit":lead,"short_noise_audit":noise,"metric_divergence":divergence,"extreme_divergence_review":extreme,"incremental_information_summary":info,"stability_responsiveness_summary":stability,"recent_36m_summary":recent,"decision_matrix":pd.DataFrame(decisions),"parity_audit":parity,"human_decision_status":status}

def _visual(frame,title,path):
    im=Image.new('RGB',(1400,900),'white'); d=ImageDraw.Draw(im); d.text((20,15),title,fill='black'); panels=[('ma12_level','MA12 structural level'),('normalized_short_score','Normalized level / lag6 short / lag12 long'),('metric_score','Final metric finalists'),('short_contribution','Short contribution / divergence context')]; colors={'BPS-FINAL-70':'#7c3aed','BPS-FINAL-80':'#2563eb'}
    for i,(col,label) in enumerate(panels):
        top=50+i*205; bottom=top+170; d.rectangle((75,top,1370,bottom),outline='#aaa'); d.text((80,top+4),label,fill='black'); vals=frame[col].dropna(); lo,hi=(vals.min(),vals.max()) if len(vals) else (0,1); hi=hi if hi!=lo else lo+1
        for j,(p,g) in enumerate(frame.groupby('policy_id')):
            g=g.sort_values('date'); pts=[(75+k*1295/max(len(g)-1,1),bottom-8-(v-lo)/(hi-lo)*140) for k,v in enumerate(g[col]) if pd.notna(v)];
            if len(pts)>1:d.line(pts,fill=colors[p],width=3)
            d.text((90+j*300,top+22),p,fill=colors[p])
    im.save(path)

def write_bundle(evidence,output_dir,source_run_id):
    output_dir.mkdir(parents=True,exist_ok=True); visuals=output_dir/'visuals'; visuals.mkdir(exist_ok=True)
    for k,v in evidence.items(): v.to_csv(output_dir/f'bps_finalist_{k}.csv',index=False)
    names=[]
    for geo in GEOGRAPHIES: name=f'{geo}__bps_finalist_comparison.png'; _visual(evidence['policy_chronology'].query('geo_id==@geo'),geo,visuals/name); names.append(name)
    comparison='seven_geo_bps_finalist_comparison.png'; _visual(evidence['policy_chronology'],'Seven-geography finalist comparison',visuals/comparison)
    order=['policy_registry','decision_matrix','short_ablation_summary','momentum_ablation_summary','dominant_driver_audit','turn_ablation_audit','short_lead_value_audit','short_noise_audit','extreme_divergence_review','incremental_information_summary','stability_responsiveness_summary','recent_36m_summary']
    body=['<h1>BPS Finalist Incremental Value Review</h1><h2>Frozen lag6 architecture</h2><p>MA12 level; lag6/lag12 ratio momentum; positive expanding percentile normalization; Supply weight 0.20.</p>']+[f'<h2>{html.escape(k.replace("_"," ").title())}</h2>{evidence[k].to_html(index=False)}' for k in order]; body+=['<h2>Visuals</h2>'+''.join(f"<img src='visuals/{n}'>" for n in [comparison]+names),'<h2>Governance</h2>'+evidence['human_decision_status'].to_html(index=False)]
    (output_dir/'bps_finalist_incremental_value_review.html').write_text('<!doctype html><meta charset=utf-8><style>body{font-family:sans-serif}img{max-width:100%}table{font-size:10px}</style>'+''.join(body),encoding='utf-8')
    runtime=pd.DataFrame([{"decision_id":DECISION_ID,"source_run_id":source_run_id,"geography_count":7,"policy_count":2,"visual_count":8,"parity_tolerance":TOLERANCE,"output_file_count":len(list(output_dir.rglob('*')))+1}]); runtime.to_csv(output_dir/'bps_finalist_runtime_summary.csv',index=False); return len(list(output_dir.rglob('*')))
