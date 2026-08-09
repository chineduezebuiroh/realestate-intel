"""Focused smoke test for the diagnostic-only BPS finalist experiment."""
import numpy as np
import pandas as pd
from regime.pandas_compat import MONTH_END
from regime.diagnostics.bps_permit_volatility import GEOGRAPHIES,TOLERANCE
from regime.experiments import bps_finalist_incremental_value as diag

def fixture():
    rows=[]
    for n,geo in enumerate(GEOGRAPHIES):
        for i,date in enumerate(pd.date_range('2013-01-31',periods=156,freq=MONTH_END)):
            rows.append({'geo_id':geo,'date':date,'canonical_metric_key':'permit_activity','value':max(100+n*8+i*.15+24*np.sin(i/5)+7*np.sin(i/2.2),1)})
    return pd.DataFrame(rows)

def main():
    registry=diag.policy_registry(); assert registry.policy_id.tolist()==['BPS-FINAL-70','BPS-FINAL-80']; assert registry[['level_weight','short_weight','long_weight']].to_records(index=False).tolist()==list(diag.POLICIES.values())
    assert registry.short_horizon.eq('lag6').all() and registry.long_horizon.eq('lag12').all(); assert registry.transform_family.eq('ratio').all(); assert registry.level_formula.eq('MA12(raw bps_total_units)').all(); assert registry.normalization_method.eq('expanding_percentile').all() and registry.normalization_polarity.eq('positive').all(); assert registry.supply_metric_weight.eq(.20).all()
    e=diag.build_evidence(fixture(),'fixture'); c=e['policy_chronology']; assert set(c.geo_id)==set(GEOGRAPHIES) and c.policy_id.nunique()==2
    a=c.query("policy_id=='BPS-FINAL-70'").set_index(['geo_id','date']); b=c.query("policy_id=='BPS-FINAL-80'").set_index(['geo_id','date']); cols=['normalized_level_score','normalized_short_score','normalized_long_score']; assert (a[cols]-b[cols]).abs().max().max()<=TOLERANCE
    assert (c[['level_contribution','short_contribution','long_contribution']].sum(axis=1)-c.metric_score).abs().dropna().max()<=TOLERANCE
    s=e['short_ablation_chronology']; row=c.query("policy_id=='BPS-FINAL-70'").dropna(subset=cols).iloc[-1]; expected=(.70*row.normalized_level_score+.15*row.normalized_long_score)/.85; got=s.query("policy_id=='BPS-FINAL-70' and geo_id==@row.geo_id and date==@row.date").iloc[0].no_short_metric_score; assert abs(expected-got)<=TOLERANCE
    m=e['momentum_ablation_chronology']; got=m.query("policy_id=='BPS-FINAL-70' and geo_id==@row.geo_id and date==@row.date").iloc[0].no_momentum_metric_score; assert abs(got-row.normalized_level_score)<=TOLERANCE
    assert len(e['dominant_driver_audit']) and len(e['turn_ablation_audit']) and e['turn_ablation_audit'].matching_contract.str.contains('same-type deterministic one-to-one').all(); assert len(e['short_lead_value_audit'])
    noise=e['short_noise_audit']; assert {'material_short_threshold_p75','minimal_metric_threshold_p25'}.issubset(noise); threshold=e['metric_divergence'].absolute_difference.quantile(.95); assert len(e['extreme_divergence_review']) and e['extreme_divergence_review'].absolute_difference.ge(threshold).all() and e['metric_divergence'].query('absolute_difference < @threshold').absolute_difference.max()<threshold
    d=e['decision_matrix']; assert len(d)==2 and d.Decision.eq('pending').all(); assert not any(any(x in str(col).lower() for x in ('rank','composite','winner')) for col in d)
    assert e['parity_audit'].status.eq('pass').all(); status=e['human_decision_status'].iloc[0]; assert status.recommendation_state=='none' and status.promotion_state=='none' and status.human_decision=='pending' and not status.automated_winner
    print('[smoke] BPS finalist incremental value: PASS')
if __name__=='__main__': main()
