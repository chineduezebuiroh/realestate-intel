"""Focused smoke coverage for the lag-6-frozen BPS feature-weight family."""
from __future__ import annotations

import numpy as np
import pandas as pd

from regime.pandas_compat import MONTH_END

from regime.diagnostics.bps_permit_volatility import GEOGRAPHIES, TOLERANCE
from regime.experiments import bps_feature_weight_comparison as diag


def fixture() -> pd.DataFrame:
    dates=pd.date_range("2015-01-31",periods=132,freq=MONTH_END); rows=[]
    for number,geo in enumerate(GEOGRAPHIES):
        for i,date in enumerate(dates):
            value=100+number*8+i*.15+24*np.sin(i/5.0)+7*np.sin(i/2.2)
            rows.append({"geo_id":geo,"date":date,"canonical_metric_key":"permit_activity","value":max(value,1.)})
    return pd.DataFrame(rows)


def main() -> None:
    registry=diag.policy_registry()
    assert registry.policy_id.tolist()==["BPS-W-50-25-25","BPS-W-60-20-20","BPS-W-70-15-15","BPS-W-80-10-10","BPS-W-90-05-05"]
    assert registry[["level_weight","short_weight","long_weight"]].to_records(index=False).tolist()==list(diag.POLICIES.values())
    assert registry.short_horizon.eq("lag6").all() and registry.long_horizon.eq("lag12").all()
    assert registry.transform_family.eq("ratio").all() and registry.level_formula.eq("MA12(raw bps_total_units)").all()
    assert registry.normalization_method.eq("expanding_percentile").all() and registry.normalization_polarity.eq("positive").all()
    assert registry.supply_metric_weight.eq(.20).all() and registry.scope.eq("BPS-only").all()
    evidence=diag.build_evidence(fixture(),"fixture")
    chronology=evidence["policy_chronology"]
    assert set(chronology.geo_id)==set(GEOGRAPHIES) and chronology.policy_id.nunique()==5
    assert evidence["parity_audit"].status.eq("pass").all() and evidence["parity_audit"].max_abs_difference.max()<=TOLERANCE
    base=chronology.query("policy_id == @diag.INCUMBENT").set_index(["geo_id","date"])
    for _,candidate in chronology.groupby("policy_id"):
        candidate=candidate.set_index(["geo_id","date"])
        assert (candidate[["normalized_level_score","normalized_short_score","normalized_long_score"]]-base[["normalized_level_score","normalized_short_score","normalized_long_score"]]).abs().max().max()<=TOLERANCE
    reconstructed=chronology[["level_contribution","short_contribution","long_contribution"]].sum(axis=1)
    assert (reconstructed-chronology.metric_score).abs().dropna().max()<=TOLERANCE
    movement=evidence["metric_movement_attribution"]
    assert movement.absolute_reconstruction_error.max()<=TOLERANCE and movement.policy_id.nunique()==5
    assert evidence["turning_point_summary"].policy_id.nunique()==5
    assert evidence["responsiveness_audit"].policy_id.nunique()==5 and evidence["responsiveness_audit"].matched_turn_count.sum()>0
    assert evidence["momentum_suppression_audit"].policy_id.nunique()==5
    decision=evidence["decision_matrix"]
    assert len(decision)==5 and decision.Decision.eq("pending").all()
    forbidden={"rank","composite","winner"}; assert not any(any(word in str(c).lower() for word in forbidden) for c in decision.columns)
    status=evidence["human_decision_status"].iloc[0]
    assert status.recommendation_state=="none" and status.promotion_state=="none" and status.human_decision=="pending"
    assert not status.automated_winner
    print("[smoke] BPS feature-weight comparison: PASS")


if __name__ == "__main__": main()
