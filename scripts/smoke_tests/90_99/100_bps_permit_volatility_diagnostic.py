from __future__ import annotations
import tempfile
from pathlib import Path
import pandas as pd

from regime.diagnostics.bps_permit_volatility import ATTRIBUTIONS, GEOGRAPHIES, build_evidence, write_bundle, zero_streaks

def fixture() -> pd.DataFrame:
    rows=[]; dates=pd.date_range("2018-01-31",periods=84,freq="M")
    for number,geo in enumerate(GEOGRAPHIES,1):
        for i,date in enumerate(dates):
            if i == 9: continue  # genuine missing month, never materialized
            value=0.0 if i in {2,3,20} else float(number*10+(i%7)*3)
            rows.append({"geo_id":geo,"date":date,"canonical_metric_key":"permit_activity","value":value})
        for date,pop in [(dates[0],100_000+number*1000),(dates[36],105_000+number*1000),(dates[72],110_000+number*1000)]:
            rows.append({"geo_id":geo,"date":date,"canonical_metric_key":"population","value":pop})
    return pd.DataFrame(rows)

def main() -> int:
    source=fixture(); evidence=build_evidence(source,"fixture")
    chronology=evidence["chronology"]
    assert set(chronology.geo_id)==set(GEOGRAPHIES) and len(GEOGRAPHIES)==7
    assert "raw_bps_total_units" in chronology and "raw_permit_intensity" in chronology
    first=GEOGRAPHIES[0]; g=chronology[chronology.geo_id.eq(first)].set_index("date")
    assert pd.Timestamp("2018-10-31") not in g.index  # missing did not become zero
    assert g.raw_bps_total_units.eq(0).sum()==3
    streak=zero_streaks(source[source.canonical_metric_key.eq("permit_activity")])
    assert streak[streak.geo_id.eq(first)].streak_length_months.tolist()==[2,1]
    # A missing month splits contiguous production feature segments. Full-window MA12
    # starts only after twelve subsequent observed contiguous months.
    assert pd.isna(g.loc[pd.Timestamp("2019-09-30"),"ma12_structural_level"])
    assert pd.notna(g.loc[pd.Timestamp("2019-10-31"),"ma12_structural_level"])
    contract=evidence["production_contract"]
    permit=contract[contract.feature_key.str.startswith("permit_intensity_")].set_index("feature_key")
    assert permit.loc["permit_intensity_short","feature_window"]=="12m/lag3m"
    assert permit.loc["permit_intensity_long","feature_window"]=="12m/lag12m"
    assert permit.feature_weight.astype(float).to_dict()=={"permit_intensity_level":.5,"permit_intensity_short":.25,"permit_intensity_long":.25}
    assert permit.normalization_method.eq("expanding_percentile").all()
    parity=evidence["parity_audit"]
    assert parity[parity.parity_check.eq("incumbent_level_equals_reconstructed_ma12")].status.eq("pass").all()
    assert contract.incumbent_equals_ma12.all()
    assert set(evidence["attribution_summary"].primary_attribution).issubset(ATTRIBUTIONS)
    status=evidence["human_decision_status"].iloc[0]
    assert (status.recommendation_state,status.promotion_state,status.human_decision)==("none","none","pending")
    with tempfile.TemporaryDirectory() as tmp:
        count=write_bundle(evidence,Path(tmp),"fixture")
        assert count>=21 and len(list((Path(tmp)/"visuals").glob("*.png")))==8
    assert 'freq="M"' in Path(__file__).read_text()
    print("[bps_permit_volatility] OK")
    return 0

if __name__ == "__main__": raise SystemExit(main())
