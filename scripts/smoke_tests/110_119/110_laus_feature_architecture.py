"""Deterministic contract smoke test for the LAUS architecture calibration."""
from pathlib import Path
import hashlib
import numpy as np
import pandas as pd

from regime.experiments.laus_feature_architecture import (
    GEOS, LAUS, MA_POLICIES, WEIGHT_POLICIES, PRODUCTION_WEIGHTS,
    _chronology, policy_registry, production_contract,
)

root=Path(__file__).resolve().parents[3]
before={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in (
 "config/feature_registry.csv","config/metric_dimension_registry.csv","config/axis_registry.csv","config/normalization_registry.csv")}
contract=production_contract(root)
assert set(contract.metric_key)==set(LAUS) and len(contract)==9
assert MA_POLICIES=={"LAUS-MA3":3,"LAUS-MA6":6,"LAUS-MA9":9,"LAUS-MA12":12}
ma=policy_registry("ma"); assert len(ma)==4 and ma.decision.eq("pending").all()
assert ma[["level_weight","short_weight","long_weight"]].drop_duplicates().iloc[0].tolist()==list(PRODUCTION_WEIGHTS)
try: policy_registry("weights")
except ValueError: pass
else: raise AssertionError("Stage B selected MA did not fail closed")
w=policy_registry("weights",6); assert len(w)==5 and w.decision.eq("pending").all()
assert np.allclose(w[["level_weight","short_weight","long_weight"]].sum(axis=1),1)
assert set(WEIGHT_POLICIES)==set(w.policy)

# Exercise shared smoothing + production normalization twice.  All seven governed
# counties and all three metrics receive identical, complete monthly fixtures.
dates=pd.date_range("2008-01-31",periods=180,freq="M"); rows=[]
for gi,geo in enumerate(GEOS):
 for mi,metric in enumerate(LAUS):
  for i,date in enumerate(dates): rows.append({"geo_id":geo,"date":date,"canonical_metric_key":metric,"raw_value":1000+gi*20+mi*100+i*2+np.sin(i/5)})
source=pd.DataFrame(rows); a=_chronology(source,"LAUS-MA6",6,PRODUCTION_WEIGHTS); b=_chronology(source,"LAUS-MA6",6,PRODUCTION_WEIGHTS)
pd.testing.assert_frame_equal(a,b); assert set(a.geo_id)==set(GEOS); assert not a.geo_id.str.contains("cbsa|metro",case=False).any()
reconstructed=a[["level_contribution","short_contribution","long_contribution"]].sum(axis=1)
assert np.nanmax(np.abs(reconstructed-a.metric_score)) < 1e-12
assert a.groupby("canonical_metric_key")[["configured_level_weight","configured_short_weight","configured_long_weight"]].nunique().max().max()==1
after={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in before}; assert before==after
print("LAUS feature architecture smoke test passed")
