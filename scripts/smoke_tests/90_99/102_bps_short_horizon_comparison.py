from __future__ import annotations
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from regime.experiments.bps_short_horizon_comparison import GEOGRAPHIES, POLICIES, build_evidence, write_bundle

def fixture() -> pd.DataFrame:
    rows=[]
    for number,geo in enumerate(GEOGRAPHIES,1):
        for i,date in enumerate(pd.date_range("2012-01-01",periods=168,freq="MS")):
            rows.append({"geo_id":geo,"date":date,"canonical_metric_key":"permit_activity","value":float(20*number+i%13+4*np.sin(i/5))})
    return pd.DataFrame(rows)

def main() -> int:
    production=Path("config/feature_registry.csv"); before=production.read_bytes(); evidence=build_evidence(fixture(),"fixture")
    registry=evidence["policy_registry"]; assert len(registry)==3 and list(registry.policy_id)==list(POLICIES)
    assert list(registry.short_horizon)==["lag1","lag3","lag6"] and registry.long_horizon.eq("lag12").all()
    assert registry.transform_family.eq("ratio").all() and registry.level_formula.eq("MA12(raw bps_total_units)").all()
    assert np.allclose(registry[["level_weight","short_weight","long_weight"]],[.5,.25,.25])
    assert registry.normalization_method.eq("expanding_percentile").all() and registry.normalization_polarity.eq("positive").all()
    assert registry.supply_metric_weight.eq(.20).all() and registry.scope.eq("BPS-only").all()
    chron=evidence["policy_chronology"]; assert chron.geo_id.nunique()==7 and set(chron.policy_id)==set(POLICIES)
    assert evidence["parity_audit"].status.eq("pass").all() and evidence["parity_audit"].max_abs_difference.max()<=1e-12
    assert (chron[["level_contribution","short_contribution","long_contribution"]].sum(axis=1)-chron.metric_score).abs().dropna().max()<=1e-12
    assert evidence["metric_movement_attribution"].absolute_reconstruction_error.max()<=1e-12
    assert not evidence["responsiveness_audit"].empty and not evidence["directional_agreement"].empty
    decision=evidence["decision_matrix"]; assert len(decision)==3 and decision.Decision.eq("pending").all()
    assert not any(x.lower() in {"rank","composite","winner"} for x in decision.columns)
    status=evidence["human_decision_status"].iloc[0]; assert (status.recommendation_state,status.promotion_state,status.human_decision)==("none","none","pending")
    assert pd.to_datetime(chron.date).dt.to_period("M").notna().all()
    with tempfile.TemporaryDirectory() as tmp:
        out=Path(tmp); write_bundle(evidence,out,"fixture")
        required=["policy_registry","policy_chronology","stability","stability_summary","turning_points","turning_point_summary","contribution_summary","metric_driver_audit","metric_movement_attribution","responsiveness_audit","directional_agreement","extreme_jump_attribution","recent_36m_summary","metric_score_comparison","decision_matrix","parity_audit","human_decision_status","runtime_summary"]
        assert all((out/f"bps_short_horizon_{name}.csv").is_file() for name in required)
        assert (out/"bps_short_horizon_review.html").is_file() and len(list((out/"visuals").glob("*.png")))==8
        html=(out/"bps_short_horizon_review.html").read_text(); assert all(policy in html for policy in POLICIES) and "incumbent" in Path("regime/experiments/bps_short_horizon_comparison.py").read_text()
    assert production.read_bytes()==before
    source=Path("regime/experiments/bps_short_horizon_comparison.py").read_text().lower(); assert "permit_intensity" not in source and "seasonal adjustment" not in source and "composite score" not in source
    print("[bps_short_horizon_comparison] OK"); return 0
if __name__ == "__main__": raise SystemExit(main())
