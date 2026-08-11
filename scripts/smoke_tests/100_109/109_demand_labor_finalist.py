"""Deterministic contract smoke for the Demand labor finalist review."""
from __future__ import annotations
import hashlib
import importlib.util
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd

from regime.experiments import demand_labor_finalist as f
from regime.experiments import demand_metric_redundancy as d
from regime.pandas_compat import MONTH_END

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location("smoke108",ROOT/"scripts/smoke_tests/100_109/108_demand_metric_redundancy.py")
s108=importlib.util.module_from_spec(spec); spec.loader.exec_module(s108)

def test_registry_correction():
    assert len({frozenset(v) for v in d.POLICIES.values()})==4
    assert d.POLICIES["DEM-LABOR-C"]==set(d.METRICS)-{"labor_force","employment"}
    assert d.POLICIES["DEM-LABOR-D"]==set(d.METRICS)-{"labor_force","laus_unemployment_rate"}

def test_deterministic_helpers():
    dates=pd.date_range("2020-01-31",periods=12,freq=MONTH_END)
    moves=pd.DataFrame({"geo_id":"g","date":dates,"move":[np.nan,.1,.11,-.2,.12,.11,.1,-.2,-.2,-.2,.2,.2]})
    a=f.reversal_events(moves,"move"); b=f.reversal_events(moves,"move")
    pd.testing.assert_frame_equal(a,b)
    series=pd.DataFrame({"geo_id":"g","date":dates,"value":np.cumsum(np.nan_to_num(moves.move))})
    pd.testing.assert_frame_equal(f.persistence_summary(series,"value","p","dimension"),f.persistence_summary(series,"value","p","dimension"))
    # Shared matcher guarantees same-type, one-to-one matches.
    turns=pd.DataFrame({"turning_point_date":dates[[3,8]],"turning_point_type":["peak","trough"],"qualified":True})
    matched=f.match_turning_points(turns,turns)
    assert matched.matched.all() and matched.challenger_date.is_unique

def test_bundle():
  before={p:(ROOT/p).read_bytes() for p in ("config/feature_registry.csv","config/metric_dimension_registry.csv","config/axis_registry.csv","config/normalization_registry.csv")}
  with tempfile.TemporaryDirectory() as td:
    root=Path(td); run=s108.fixture(root)
    dates=pd.read_parquet(run/"dimension_scores.parquet")[["geo_id","evaluation_date"]].drop_duplicates()
    dates.assign(major_regime="expansion",minor_regime="stable").to_parquet(run/"regime_assignments.parquet",index=False)
    tables=f.build(run,ROOT); out=root/"out"; f.write_review(tables,out)
    assert set(tables)==set(f.OUTPUTS)
    assert len(tables["demand_labor_finalist_policy_registry"])==2
    registry=tables["demand_labor_finalist_policy_registry"].set_index("policy")
    assert set(registry.index)==set(f.FINALISTS)
    assert "labor_force" in registry.loc["DEM-FINAL-A","labor_metrics_included"] and "labor_force" not in registry.loc["DEM-FINAL-B","labor_metrics_included"]
    weights={p:__import__('json').loads(registry.loc[p,"effective_metric_weights"]) for p in f.FINALISTS}
    ratios=[weights["DEM-FINAL-B"][m]/weights["DEM-FINAL-A"][m] for m in f.FINALISTS["DEM-FINAL-B"]]
    assert np.ptp(ratios)<1e-12 and abs(sum(weights["DEM-FINAL-B"].values())-1)<1e-12
    assert tables["demand_labor_finalist_parity_audit"].max_abs_error.max()<=1e-12
    assert set(tables["demand_labor_finalist_recent_36m"].geo_id)==set(f.REVIEW_GEOS)
    assert len(tables["demand_labor_finalist_decision_matrix"])==2 and set(tables["demand_labor_finalist_decision_matrix"].Decision)=={"pending"}
    gov=tables["demand_labor_finalist_governance_status"].iloc[0]
    assert (gov.recommendation_state,gov.promotion_state,gov.human_decision,gov.automated_winner)==("none","none","pending",False)
    leads = tables["demand_labor_finalist_lead_value"].loc[
        tables["demand_labor_finalist_lead_value"]["record_type"].eq("turn")
        & tables["demand_labor_finalist_lead_value"]["anticipated"].eq(True)
    ]
    assert (leads.observation_date < leads.turn_date).all()
    match = tables["demand_labor_finalist_turn_match"].loc[
        tables["demand_labor_finalist_turn_match"]["matched"].eq(True)
    ]
    assert match.groupby(["geo_id","series"]).challenger_date.apply(lambda x:x.is_unique).all()
    digest=lambda:hashlib.sha256((out/"demand_labor_finalist_decision_matrix.csv").read_bytes()).hexdigest()
    old=digest(); f.write_review(tables,out); assert old==digest()
  assert all((ROOT/p).read_bytes()==content for p,content in before.items())

if __name__=="__main__":
    test_registry_correction(); test_deterministic_helpers(); test_bundle()
    print("demand labor finalist diagnostic smoke passed")
