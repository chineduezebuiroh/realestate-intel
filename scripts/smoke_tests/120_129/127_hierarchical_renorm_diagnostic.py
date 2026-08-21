"""Smoke 127: bounded hierarchical re-normalization diagnostic contract."""
from __future__ import annotations
import hashlib
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd

from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime.diagnostics.hierarchical_renorm import (
    CLIP_HIGH,CLIP_LOW,DIMENSIONS,LOOKBACK,MIN_PERIODS,OUTPUT_NAMES,PATHS,
    build_diagnostic,build_paths,load_run,write_review,
)

def fixture(shuffle=False):
    dates=pd.date_range("2015-01-31",periods=132,freq="ME"); metrics=[]
    governed={"demand":["labor_force","employment","laus_unemployment_rate"],
      "price":["median_sale_price","median_ppsf"],"affordability":["price_to_income","payment_burden"],
      "capital_markets":["mortgage_30y","treasury_10y","spread_10y_2y"]}
    for d,keys in governed.items():
      for j,key in enumerate(keys):
       for i,date in enumerate(dates): metrics.append({"geo_id":"district_of_columbia_dc__county","evaluation_date":date,
         "canonical_metric_key":key,"metric_score":.35*np.sin(i/11+j)+.002*i,"metric_age_days":0})
    m=pd.DataFrame(metrics)
    if shuffle: m=m.sample(frac=1,random_state=9).reset_index(drop=True)
    dims=score_dimensions(m); axes=score_axes(dims)
    return {"aligned_metric_scores":m,"dimension_scores":dims,"axis_scores":axes}

def main():
    registry=Path("config/normalization_registry.csv"); before=hashlib.sha256(registry.read_bytes()).hexdigest()
    artifacts=fixture(); paths=build_paths(artifacts); long=paths["long"]
    assert tuple(sorted(long.path.unique())) == PATHS
    scenarios=build_diagnostic(artifacts).tables["scenario_registry"]
    assert (LOOKBACK,MIN_PERIODS,CLIP_LOW,CLIP_HIGH)==(120,36,.01,.99)
    assert scenarios.set_index("path").metric_renormalized.to_dict()=={"A":False,"B":True,"C":True}
    assert scenarios.set_index("path").dimension_renormalized.to_dict()=={"A":False,"B":False,"C":True}
    assert not scenarios.axis_renormalized.any()
    assert not long.component.eq("market_context").any() and set(long.query("layer=='dimension'").component)==set(DIMENSIONS)
    authoritative=artifacts["axis_scores"].query("axis=='demand'").sort_values(["geo_id","date"]).axis_score.reset_index(drop=True)
    observed=paths["axis_a"].sort_values(["geo_id","date"]).axis_score.reset_index(drop=True)
    pd.testing.assert_series_equal(authoritative,observed)
    # Production scorer parity proves weighted aggregation and missingness behavior are reused.
    pd.testing.assert_frame_equal(paths["axis_b"].reset_index(drop=True),score_axes(paths["dimensions_b"]).query("axis=='demand'").reset_index(drop=True))
    shuffled=build_paths(fixture(True))["long"].sort_values(["path","layer","geo_id","date","component"]).reset_index(drop=True)
    ordered=long.sort_values(["path","layer","geo_id","date","component"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(ordered,shuffled)
    assert hashlib.sha256(registry.read_bytes()).hexdigest()==before
    try: load_run(Path("/definitely/absent"))
    except FileNotFoundError: pass
    else: raise AssertionError("absent authoritative run did not fail closed")
    result=build_diagnostic(artifacts)
    with tempfile.TemporaryDirectory() as tmp:
      out=Path(tmp); write_review(result,out)
      assert all((out/f"hierarchical_renorm_{name}.csv").exists() for name in OUTPUT_NAMES)
      assert len(list(out.glob("*.svg")))==5
    print("Smoke 127 passed: exactly A/B/C, local policy, production aggregation reuse, row-order invariance, governance and fail-closed contracts")

if __name__ == "__main__": main()
