#!/usr/bin/env python3
"""Contract smoke for persisted-only finalist reversal and turn mechanics."""
from pathlib import Path
import tempfile
import pandas as pd

from regime.diagnostics.laus_finalist_stability import (CONTROLLED_PAIRS,GOVERNANCE,
    POLICIES,REQUIRED_INPUTS,SCENARIOS,cluster_consensus_turns,match_consensus,reversal_events,
    scenario_registry,validate_persisted_bundle)
from regime.experiments.demand_signal_attenuation import GEOS


def main():
    registry=scenario_registry()
    assert tuple(registry.scenario_id)==SCENARIOS and len(registry)==8
    assert POLICIES=={"B2":(.45,.15,.40),"B3":(.40,.15,.45),"L0":(.35,.20,.45),"L1":(.35,.15,.50)}
    assert set(registry.ma_months)=={6,9} and CONTROLLED_PAIRS==(("B2","B3"),("B3","L1"),("B3","L0"),("L0","L1"))
    assert GOVERNANCE=={"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":"finalist_review_pending","automated_winner":False,"production_policy_changed":False}
    # Down reversal at March is undone in month 3, but not month 2.
    series=pd.DataFrame({"date":pd.date_range("2020-01-31",periods=7,freq="ME"),"score":[0,2,1,.5,1.5,2.1,2.2]})
    events=reversal_events(series); event=events.iloc[0]
    assert event.durable_2m and not event.whipsaw_2m and event.whipsaw_3m and not event.durable_3m
    points=pd.DataFrame([{"scenario_id":sid,"turning_point_type":"peak","turning_point_date":pd.Timestamp("2020-06-30")+pd.offsets.MonthEnd(i%3-1)} for i,sid in enumerate(reversed(SCENARIOS))])
    assert len(cluster_consensus_turns(points.sample(frac=1,random_state=2),5))==1
    assert len(cluster_consensus_turns(points,6))==1 and len(cluster_consensus_turns(points,7))==1
    consensus=cluster_consensus_turns(points,6); matched=match_consensus(points,consensus)
    assert not matched.missed.any() and set(matched.latency_months)=={-1,0,1}
    # Row order cannot define identity.
    pd.testing.assert_frame_equal(scenario_registry().sort_values("scenario_id").reset_index(drop=True),registry.sample(frac=1,random_state=4).sort_values("scenario_id").reset_index(drop=True))
    with tempfile.TemporaryDirectory() as tmp:
        try: validate_persisted_bundle(Path(tmp))
        except FileNotFoundError: pass
        else: raise AssertionError("missing persisted evidence must fail closed")
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp)
        generic=pd.DataFrame({"evidence":[1]})
        for name in REQUIRED_INPUTS:
            generic.to_csv(root/name,index=False)
        metric=pd.DataFrame([{"scenario_id":sid,"ma_months":int(sid[2]),
            "weight_policy":sid.split("__")[1],"geo_id":geo,"date":"2026-01-31",
            "metric":"labor_force","metric_score":.1}
            for sid in SCENARIOS for geo in GEOS])
        metric.to_csv(root/"laus_long_weight_metric_chronology.csv",index=False)
        downstream=pd.DataFrame([{"scenario_id":sid,"geo_id":geo,"date":"2026-01-31",
            "cyclical_score":.1,"core_demand_score":.2}
            for sid in SCENARIOS for geo in GEOS])
        downstream.to_csv(root/"laus_long_weight_downstream_chronology.csv",index=False)
        frames=validate_persisted_bundle(root)
        assert {"date","cyclical_score","core_demand_score"}.issubset(
            frames["laus_long_weight_downstream_chronology"])
        # Aggregate statistics, and even chronology-shaped data hidden in an
        # arbitrary required frame, cannot substitute for the explicit input.
        downstream.to_csv(root/"laus_long_weight_cyclical_statistics.csv",index=False)
        (root/"laus_long_weight_downstream_chronology.csv").unlink()
        try: validate_persisted_bundle(root)
        except FileNotFoundError: pass
        else: raise AssertionError("the explicit downstream chronology must be required")
    print("PASS: diagnostic-only persisted LAUS finalist stability contract")


if __name__=="__main__": main()
