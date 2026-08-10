from __future__ import annotations
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from regime.experiments.bps_short_horizon_comparison import DECISION_ID, GEOGRAPHIES, POLICIES, SELECTED_POLICY, build_evidence, write_bundle
from regime.diagnostics.capital_markets_ma import match_turning_points

def fixture() -> pd.DataFrame:
    rows=[]
    for number,geo in enumerate(GEOGRAPHIES,1):
        for i,date in enumerate(pd.date_range("2012-01-01",periods=168,freq="MS")):
            # Long triangular cycles survive MA12 and provide deterministic,
            # persistence-qualified structural and policy turning points.
            phase=i%48; triangle=phase if phase<24 else 48-phase
            value=120+10*number+3*triangle+.1*np.sin(i/7)
            rows.append({"geo_id":geo,"date":date,"canonical_metric_key":"permit_activity","value":float(value)})
    return pd.DataFrame(rows)

def main() -> int:
    production=Path("config/feature_registry.csv"); before=production.read_bytes(); evidence=build_evidence(fixture(),"fixture")
    registry=evidence["policy_registry"]; assert len(registry)==3 and list(registry.policy_id)==list(POLICIES)
    assert list(registry.short_horizon)==["lag1","lag3","lag6"] and registry.long_horizon.eq("lag12").all()
    assert registry.transform_family.eq("ratio").all() and registry.level_formula.eq("MA12(raw bps_total_units)").all()
    assert np.allclose(registry[["level_weight","short_weight","long_weight"]],[.5,.25,.25])
    assert registry.normalization_method.eq("expanding_percentile").all() and registry.normalization_polarity.eq("positive").all()
    assert registry.supply_metric_weight.eq(.20).all() and registry.scope.eq("BPS-only").all()
    assert registry.set_index("policy_id").selection_status.to_dict()=={"BPS-H-LAG1":"not_selected","BPS-H-LAG3":"not_selected","BPS-H-LAG6":"selected"}
    assert registry.query("production_status == 'production'").policy_id.tolist()==[SELECTED_POLICY]
    chron=evidence["policy_chronology"]; assert chron.geo_id.nunique()==7 and set(chron.policy_id)==set(POLICIES)
    assert evidence["parity_audit"].status.eq("pass").all() and evidence["parity_audit"].max_abs_difference.max()<=1e-12
    assert (chron[["level_contribution","short_contribution","long_contribution"]].sum(axis=1)-chron.metric_score).abs().dropna().max()<=1e-12
    assert evidence["metric_movement_attribution"].absolute_reconstruction_error.max()<=1e-12
    assert not evidence["responsiveness_audit"].empty and not evidence["directional_agreement"].empty
    audit=evidence["turn_detection_audit"]
    assert audit.query("series_name == 'ma12_level'").qualified_turn_count.sum()==126  # 42 shared references repeated for three policies
    assert audit.query("series_name == 'normalized_short_score'").qualified_turn_count.sum()>0
    assert audit.query("series_name == 'metric_score'").qualified_turn_count.sum()>0
    response=evidence["responsiveness_audit"]; assert response.matched_turn_count.sum()==123
    assert (response.matched_turn_count<=response.reference_turn_count).all() and (response.matched_turn_count<=response.policy_turn_count).all()
    shares=response[["share_within_1_month","share_within_3_months","share_within_6_months"]].dropna()
    assert (shares.iloc[:,0]<=shares.iloc[:,1]).all() and (shares.iloc[:,1]<=shares.iloc[:,2]).all()
    dates=pd.date_range("2020-01-01",periods=8,freq="MS")
    reference_turns=pd.DataFrame([{"turning_point_date":dates[1],"turning_point_type":"peak","qualified":True},{"turning_point_date":dates[4],"turning_point_type":"peak","qualified":True}])
    policy_turns=pd.DataFrame([{"turning_point_date":dates[2],"turning_point_type":"peak","qualified":True},{"turning_point_date":dates[6],"turning_point_type":"peak","qualified":True}])
    known=match_turning_points(reference_turns,policy_turns,12); matched=known[known.matched]
    assert matched.signed_delay_months.tolist()==[1,2] and matched.signed_delay_months.abs().tolist()==[1,2]
    assert matched.challenger_date.nunique()==2  # one-to-one, not nearest-event reuse
    decision=evidence["decision_matrix"]; assert len(decision)==3 and decision.set_index("Policy").Decision.to_dict()=={"BPS-H-LAG1":"not_selected","BPS-H-LAG3":"not_selected","BPS-H-LAG6":"selected"}
    assert decision[["Reference turn count","Matched turn count","Median absolute responsiveness lag months"]].notna().all().all()
    assert not any(x.lower() in {"rank","composite","winner"} for x in decision.columns)
    status=evidence["human_decision_status"].iloc[0]; assert (status.decision_id,status.selected_policy)==(DECISION_ID,SELECTED_POLICY)
    assert (status.recommendation_state,status.promotion_state,status.human_decision,status.bps_short_horizon_calibration_state)==("selected","promoted","approved","closed") and not bool(status.automated_winner)
    diff=evidence["promotion_config_diff"]; changed=diff.query("change_status == 'changed'")
    assert changed[["feature_key","field","before","after"]].to_dict("records")==[{"feature_key":"bps_total_units_short","field":"window","before":"12m/lag3m","after":"12m/lag6m"}]
    assert pd.to_datetime(chron.date).dt.to_period("M").notna().all()
    with tempfile.TemporaryDirectory() as tmp:
        out=Path(tmp); write_bundle(evidence,out,"fixture")
        required=["policy_registry","policy_chronology","stability","stability_summary","turning_points","turning_point_summary","turn_detection_audit","contribution_summary","metric_driver_audit","metric_movement_attribution","responsiveness_audit","directional_agreement","extreme_jump_attribution","recent_36m_summary","metric_score_comparison","decision_matrix","parity_audit","human_decision_status","promotion_policy_registry","promotion_config_diff","promotion_parity_audit","promotion_human_decision_status","runtime_summary","promotion_runtime_summary"]
        assert all((out/f"bps_short_horizon_{name}.csv").is_file() for name in required)
        assert (out/"bps_short_horizon_review.html").is_file() and len(list((out/"visuals").glob("*.png")))==8
        html=(out/"bps_short_horizon_review.html").read_text(); assert all(policy in html for policy in POLICIES) and "production" in html
    assert production.read_bytes()==before
    source=Path("regime/experiments/bps_short_horizon_comparison.py").read_text().lower(); assert "permit_intensity" not in source and "seasonal adjustment" not in source and "composite score" not in source and "winner score" not in source
    print("[bps_short_horizon_comparison] OK"); return 0
if __name__ == "__main__": raise SystemExit(main())
