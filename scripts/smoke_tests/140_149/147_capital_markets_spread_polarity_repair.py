"""Smoke 147: canonical 10Y-minus-2Y source-polarity repair."""
from __future__ import annotations
import hashlib, json, tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from regime._00_config_loader import load_regime_config
from regime._01_feature_engine import _compute_feature_for_contiguous_segment, load_raw_metric_series
from regime.canonical_metrics import canonicalize_source_polarity, resolve_canonical_metrics, validate_spread_10y_2y_parity

CM=("mortgage_30y","mortgage_15y","treasury_10y","fedfunds","spread_10y_2y","spread_10y_fedfunds")
def main():
 cfg=load_regime_config(validate=True); dates=pd.date_range("2020-01-31",periods=24,freq="ME")
 ten=pd.DataFrame({"geo_id":"us_national","date":dates,"value":np.linspace(1,4,24)})
 two=pd.DataFrame({"geo_id":"us_national","date":dates,"value":np.linspace(2.5,2,24)})
 physical=two.copy(); physical["value"]=two.value-ten.value
 raw=[]
 values={"fred_mortgage_30y":5.,"fred_mortgage_15y":4.5,"fred_10y":3.,"fred_fedfunds":2.,"fred_10y_fedfunds_spread":1.}
 for key,value in values.items():
  raw.extend({"geo_id":"us_national","date":d,"metric_key":key,"value":value+i/100} for i,d in enumerate(dates))
 raw.extend({"geo_id":r.geo_id,"date":r.date,"metric_key":"fred_2y10y_spread","value":r.value} for r in physical.itertuples())
 resolved=resolve_canonical_metrics(pd.DataFrame(raw),cfg)
 spread=resolved[resolved.canonical_metric_key.eq("spread_10y_2y")][["geo_id","date","value"]]
 assert np.allclose(spread.value,ten.value-two.value,rtol=0,atol=1e-12)
 assert np.allclose(spread.value,-physical.value,rtol=0,atol=1e-12)
 parity=validate_spread_10y_2y_parity(spread,ten,two,physical); assert parity=={"canonical_formula_rows":24,"physical_inversion_rows":24}
 legacy=spread.copy(); legacy["value"]*=-1
 try: validate_spread_10y_2y_parity(legacy,ten,two,physical)
 except ValueError as exc: assert "treasury_10y - treasury_2y" in str(exc)
 else: raise AssertionError("legacy inverted orientation passed canonical parity")
 unchanged=pd.DataFrame(raw); unchanged=unchanged[unchanged.metric_key.ne("fred_2y10y_spread")].set_index(["date","metric_key"]).value
 actual=resolved[resolved.canonical_metric_key.ne("spread_10y_2y")].copy()
 mapping=cfg.metric_dimensions.set_index("canonical_metric_key").metric_key.to_dict(); actual["metric_key"]=actual.canonical_metric_key.map(mapping)
 assert np.allclose(actual.set_index(["date","metric_key"]).value.sort_index(),unchanged.sort_index())
 assert "spread_10y_fedfunds" in set(actual.canonical_metric_key)
 legacy_resolved=resolved.copy(); legacy_resolved.loc[legacy_resolved.canonical_metric_key.eq("spread_10y_2y"),"value"]*=-1
 assert np.allclose(spread.value,-legacy_resolved.query("canonical_metric_key=='spread_10y_2y'").value)
 features=cfg.features[cfg.features.metric_key.eq("fred_2y10y_spread")]
 assert set(features["transform"])=={"ma_level","ma_difference"} and set(features.feature_window)=={"9m","9m/lag3m","9m/lag12m"}
 group=spread.copy(); short=_compute_feature_for_contiguous_segment(group,"ma_difference","9m/lag3m","fred_2y10y_spread_short")
 ma=group.value.rolling(9,min_periods=9).mean(); assert np.allclose(short,ma-ma.shift(3),equal_nan=True)
 norm=pd.read_csv("config/normalization_registry.csv"); assert set(norm[norm.policy_key.isin(features.feature_key)].score_direction)=={"positive"}
 indicators=pd.read_csv("config/indicator_regime_registry.csv"); assert indicators.set_index("metric_key").loc["fred_2y10y_spread","direction"]=="positive"
 metric=cfg.metric_dimensions; weights=metric[metric.canonical_metric_key.isin(CM)].set_index("canonical_metric_key").metric_weight.astype(float).to_dict()
 assert weights=={"mortgage_30y":.11666666666666667,"mortgage_15y":.11666666666666667,"treasury_10y":.11666666666666667,"fedfunds":.10,"spread_10y_2y":.275,"spread_10y_fedfunds":.275}
 assert cfg.axes.query("dimension=='capital_markets'").set_index("axis").dimension_weight.astype(float).to_dict()=={"demand":.10,"supply":.15}
 promotion=json.loads(Path("config/capital_markets_native_feature_policy_2026_08_18.json").read_text())
 assert promotion["policies"]["spread_10y_2y"]["policy"]=="P7" and promotion["feature_calibration"]=="closed"
 assert all("policy_status" not in promotion["policies"][m] for m in CM)
 contract=json.loads(Path("config/capital_markets_spread_polarity_repair_2026_08_18.json").read_text())
 assert contract["spread_10y_2y_feature_policy"]=="P7" and contract["spread_10y_2y_feature_policy_status"]=="revalidation_required"
 assert contract["family_weight_evidence_status"]=="invalidated_pending_rerun"
 governance=Path("regime/diagnostics/capital_markets_family_weight_calibration.py").read_text()
 assert "capital_markets_feature_policy_corrected_production_20260818" in governance
 assert '"prior_invalidated_evidence_reused":False' in governance
 # Historical and corrected production artifacts may exist locally as
 # ignored immutable evidence. Smoke 147 only requires that generated
 # run artifacts are not tracked by Git.
 import subprocess
 repair_run = (
     "artifacts/regime/runs/"
     "capital_markets_spread_polarity_repair_20260818"
 )
 tracked = subprocess.run(
     ["git", "ls-files", repair_run],
     capture_output=True,
     text=True,
     check=True,
 ).stdout.strip()
 assert tracked == ""
 protected=[Path("config/feature_registry.csv"),Path("config/metric_dimension_registry.csv"),Path("config/normalization_registry.csv"),Path("config/axis_registry.csv")]
 before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}; canonicalize_source_polarity(resolved); assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
 with tempfile.TemporaryDirectory() as tmp:
  try: load_raw_metric_series(cfg,Path(tmp)/"absent.duckdb")
  except FileNotFoundError as exc: assert "Serving database not found" in str(exc)
  else: raise AssertionError("absent authoritative input did not fail closed")
 print("Smoke 147 passed: exact source inversion, formula/physical parity, unchanged policies, governance, and fail closed")
if __name__=="__main__": main()
