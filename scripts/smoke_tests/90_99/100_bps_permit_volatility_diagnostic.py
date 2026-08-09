from __future__ import annotations
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd

from regime.diagnostics.bps_permit_volatility import ATTRIBUTIONS, GEOGRAPHIES, STAGES, build_evidence, write_bundle, zero_streaks


def fixture() -> pd.DataFrame:
    rows=[]; dates=pd.date_range("2015-01-01",periods=132,freq="MS")
    for number,geo in enumerate(GEOGRAPHIES,1):
        for i,date in enumerate(dates):
            if i == 9: continue
            value=0.0 if i in {2,3,20} else float(number*10+(i%7)*3)
            rows.append({"geo_id":geo,"date":date,"canonical_metric_key":"permit_activity","value":value})
    return pd.DataFrame(rows)


def main() -> int:
    source=fixture(); evidence=build_evidence(source,"fixture"); chronology=evidence["chronology"]
    assert set(chronology.geo_id)==set(GEOGRAPHIES) and len(GEOGRAPHIES)==7
    assert set(source.canonical_metric_key)=={"permit_activity"}
    diagnostic_text=Path("regime/diagnostics/bps_permit_volatility.py").read_text()
    assert "permit_intensity" not in diagnostic_text and "population" not in diagnostic_text and "cbsa" not in diagnostic_text
    assert "from regime.derived_metrics" not in diagnostic_text
    first=GEOGRAPHIES[0]; g=chronology.query("geo_id == @first").set_index("date")
    assert pd.Timestamp("2015-10-01") not in g.index
    assert g.raw_bps_total_units.eq(0).sum()==3
    streak=zero_streaks(source); assert streak.query("geo_id == @first").streak_length_months.tolist()==[2,1]
    coverage=evidence["source_sparsity_audit"]
    assert coverage.expected_month_count.ge(coverage.observation_count).all()
    assert coverage.missing_month_count.eq(1).all() and coverage.coverage_pct.le(1).all()
    assert set(evidence["stage_stability"].stage)==set(STAGES)
    assert {"normalized_short_score","normalized_long_score"}.issubset(chronology)
    available=chronology.dropna(subset=["normalized_level_score","normalized_short_score","normalized_long_score"])
    assert np.allclose(available[["effective_level_weight","effective_short_weight","effective_long_weight"]],[.5,.25,.25])
    contributions=chronology[["level_contribution","short_contribution","long_contribution"]].sum(axis=1,min_count=1)
    assert (contributions-chronology.metric_score).abs().dropna().max() <= 1e-12
    movement=evidence["metric_movement_attribution"]
    assert movement.absolute_reconstruction_error.max() <= 1e-12
    summary=evidence["contribution_summary"]
    assert len(summary)==21 and set(summary.feature_family)=={"level","short","long"}
    drivers=evidence["metric_driver_audit"]
    sufficient=drivers.observation_count.gt(2); assert np.isfinite(drivers.loc[sufficient,"correlation_with_metric_delta"]).all()
    assert set(evidence["attribution_summary"].primary_attribution).issubset(ATTRIBUTIONS)
    source_text=Path("regime/diagnostics/bps_permit_volatility.py").read_text()
    assert '(["level_contribution","short_contribution","long_contribution","metric_score"],"Panel 4' in source_text
    contract=evidence["production_contract"].set_index("feature_key")
    assert contract.feature_weight.astype(float).to_dict()=={"bps_total_units_level":.5,"bps_total_units_short":.25,"bps_total_units_long":.25}
    assert contract.metric_weight.astype(float).eq(.20).all()
    status=evidence["human_decision_status"].iloc[0]
    assert (status.recommendation_state,status.promotion_state,status.human_decision)==("none","none","pending")
    with tempfile.TemporaryDirectory() as tmp:
        count=write_bundle(evidence,Path(tmp),"fixture")
        assert count>=21 and len(list((Path(tmp)/"visuals").glob("*.png")))==8
        for name in ("metric_movement_attribution","contribution_summary","metric_driver_audit"):
            assert (Path(tmp)/f"bps_permit_volatility_{name}.csv").is_file()
    print("[bps_permit_volatility] OK")
    return 0


if __name__ == "__main__": raise SystemExit(main())
