#!/usr/bin/env python3
from pathlib import Path
import hashlib
import tempfile
import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points
from regime.diagnostics.price_turning_point_calibration import (
    FINALISTS, METRICS, SCENARIOS, build, load_authoritative, write_review,
)
from regime.diagnostics.price_feature_anatomy import REVIEW_GEOS

dates=pd.date_range("2018-01-31",periods=96,freq="ME")
rows=[]
for metric_i,metric in enumerate(METRICS):
  for geo_i,geo in enumerate(REVIEW_GEOS):
    # Smooth, durable six-month legs plus a small deterministic metric/county offset.
    z=np.sin(np.arange(len(dates))*np.pi/6)+metric_i*.03+geo_i*.005
    for date,value in zip(dates,z): rows.append({"geo_id":geo,"date":date,"metric":metric,
        "raw_12m_change":value*.08,"raw_cycle_zscore":value})
raw=pd.DataFrame(rows)
candidates=pd.concat([raw.assign(policy=p,metric_score=raw.raw_cycle_zscore.shift(i%2))
    [["policy","geo_id","date","metric","metric_score"]] for i,p in enumerate(FINALISTS)],ignore_index=True)

registry=Path("regime/config/indicator_registry.csv")
before=hashlib.sha256(registry.read_bytes()).hexdigest() if registry.exists() else None
tables=build(raw.sample(frac=1,random_state=7),candidates)
again=build(raw,candidates)
assert tuple(tables["scenario_registry"].scenario_id)==tuple(x[0] for x in SCENARIOS)
assert set(tables["scenario_registry"].persistence_months)=={1,2,3} and len(tables["scenario_registry"])==3
assert set(tables["statistics"].metric)==set(METRICS)
assert tables["turns"].qualified.any()
assert set(tables["finalist_comparison"].policy)==set(FINALISTS)
assert tables["policy_sensitivity"].long_reference_influenced_selection.eq(False).all()
assert tables["governance_status"].production_policy_changed.eq(False).all()
pd.testing.assert_frame_equal(tables["statistics"],again["statistics"])

fixture=pd.DataFrame({"date":dates[:25],"value":np.sin(np.arange(25)*np.pi/4)})
strict=detect_turning_points(fixture,"value",persistence=1,fixed_prominence=100,prominence_multiplier=2)
loose=detect_turning_points(fixture,"value",persistence=1,fixed_prominence=0,prominence_multiplier=0)
assert int(strict.qualified.sum())==0 and int(loose.qualified.sum())>0
assert {"peak","trough"}.issubset(set(loose.loc[loose.qualified,"turning_point_type"]))
assert set(tables["durability"].horizon_months)=={2,3,6}

with tempfile.TemporaryDirectory() as d:
    out=Path(d); write_review(tables,out)
    svg=(out/"price_turn_detector_median_sale_price_dc.svg").read_text()
    assert "<polyline" in svg and ("▲" in svg or "▼" in svg)
    missing=out/"absent"
    try: load_authoritative(missing)
    except FileNotFoundError as e: assert "no substitute permitted" in str(e)
    else: raise AssertionError("missing authoritative artifacts must fail closed")
after=hashlib.sha256(registry.read_bytes()).hexdigest() if registry.exists() else None
assert before==after
print("price turning-point calibration smoke: ok")
