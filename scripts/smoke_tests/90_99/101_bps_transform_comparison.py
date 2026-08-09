from __future__ import annotations
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd

from regime.experiments.bps_transform_comparison import GEOGRAPHIES, POLICIES, build_evidence, write_bundle

def fixture() -> pd.DataFrame:
    rows=[]
    for number,geo in enumerate(GEOGRAPHIES,1):
        for i,date in enumerate(pd.date_range("2012-01-01",periods=168,freq="MS")):
            rows.append({"geo_id":geo,"date":date,"canonical_metric_key":"permit_activity",
                "value":float(20*number+i%13+4*np.sin(i/5))})
    return pd.DataFrame(rows)

def main() -> int:
    before=Path("config/feature_registry.csv").read_bytes(); evidence=build_evidence(fixture(),"fixture")
    registry=evidence["policy_registry"]; assert len(registry)==2 and set(registry.policy_id)==set(POLICIES)
    ratio=registry.set_index("policy_id").loc["BPS-T-RATIO"]; diff=registry.set_index("policy_id").loc["BPS-T-DIFF"]
    assert (ratio.short_formula,ratio.long_formula)==("MA12 / lag3(MA12) - 1","MA12 / lag12(MA12) - 1")
    assert (diff.short_formula,diff.long_formula)==("MA12 - lag3(MA12)","MA12 - lag12(MA12)")
    assert registry.level_formula.eq("MA12(raw bps_total_units)").all() and registry.short_horizon.eq("lag3").all() and registry.long_horizon.eq("lag12").all()
    assert np.allclose(registry[["level_weight","short_weight","long_weight"]],[.5,.25,.25])
    assert registry.normalization_method.eq("expanding_percentile").all() and registry.supply_metric_weight.eq(.20).all()
    chron=evidence["policy_chronology"]; assert chron.geo_id.nunique()==7 and set(chron.policy_id)==set(POLICIES)
    assert evidence["parity_audit"].status.eq("pass").all() and evidence["parity_audit"].max_abs_difference.max()<=1e-12
    assert evidence["directional_parity_audit"].sign_disagreement_count.sum()==0
    reconstruction=(chron[["level_contribution","short_contribution","long_contribution"]].sum(axis=1)-chron.metric_score).abs().dropna(); assert reconstruction.max()<=1e-12
    assert evidence["metric_movement_attribution"].absolute_reconstruction_error.max()<=1e-12
    assert not evidence["scale_fairness_audit"].empty and not evidence["ratio_denominator_audit"].empty
    decision=evidence["decision_matrix"]; assert len(decision)==2 and decision.Decision.eq("pending").all()
    assert not any(x.lower() in {"rank","composite","winner"} for x in decision.columns)
    status=evidence["human_decision_status"].iloc[0]; assert (status.recommendation_state,status.promotion_state,status.human_decision)==("none","none","pending")
    months=pd.to_datetime(chron.date).dt.to_period("M"); assert months.notna().all()
    with tempfile.TemporaryDirectory() as tmp:
        write_bundle(evidence,Path(tmp),"fixture"); out=Path(tmp)
        required=["scale_fairness_audit","ratio_denominator_audit","decision_matrix","runtime_summary"]
        assert all((out/f"bps_transform_{name}.csv").is_file() for name in required)
        assert (out/"bps_transform_review.html").is_file() and len(list((out/"visuals").glob("*.png")))==8
    assert Path("config/feature_registry.csv").read_bytes()==before
    source_text=Path("regime/experiments/bps_transform_comparison.py").read_text(); assert "permit_intensity" not in source_text and "cbsa" not in source_text
    print("[bps_transform_comparison] OK"); return 0
if __name__ == "__main__": raise SystemExit(main())
