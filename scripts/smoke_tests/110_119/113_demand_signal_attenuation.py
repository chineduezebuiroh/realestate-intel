"""Smoke contracts for the fail-closed Demand attenuation diagnostic."""
from __future__ import annotations

from pathlib import Path
import tempfile

import numpy as np
import pandas as pd

from regime.experiments.demand_signal_attenuation import (
    CORE_DEMAND, DEMAND_DIMENSIONS, GEOS, LABOR, RUN_ID, STRUCTURAL, TOL,
    WEIGHT_POLICIES, build_review, cancellation, effective_contributions,
    recent_36,
)
from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.pandas_compat import MONTH_END

# Exact governed identities and policy family, including the requested 70/15/15.
assert RUN_ID == "macro_regime_v1_0_1_candidate_20260810"
assert len(GEOS) == len(set(GEOS)) == 7
assert not any("cbsa" in g or "__zip" in g for g in GEOS)
assert set(CORE_DEMAND) == set(STRUCTURAL) | set(LABOR)
assert set(STRUCTURAL).isdisjoint(LABOR)
assert DEMAND_DIMENSIONS == ("demand", "price", "affordability", "capital_markets")
assert list(WEIGHT_POLICIES) == ["LAUS-W-25-35-40","LAUS-W-40-30-30","LAUS-W-50-25-25","LAUS-W-60-20-20","LAUS-W-70-15-15","LAUS-W-80-10-10"]
assert WEIGHT_POLICIES["LAUS-W-25-35-40"] == (.25,.35,.40)
assert WEIGHT_POLICIES["LAUS-W-70-15-15"] == (.70,.15,.15)
# The 70/15/15 diagnostic family sums exactly to 1.00.
# production effective-weight denominator, so every policy's effective weights
# sum to one without silently changing the requested configured family.
assert all(abs(sum(np.array(w)/sum(w))-1) <= TOL for w in WEIGHT_POLICIES.values())

# Cancellation boundary math and missingness-renormalization semantics.
assert cancellation(pd.Series([1.,1.])) == (2.,2.,0.)
assert cancellation(pd.Series([1.,-1.])) == (2.,0.,1.)
assert np.isnan(cancellation(pd.Series([0.,0.]))[2])
assert all(np.isnan(x) for x in cancellation(pd.Series([np.nan])))
calc=effective_contributions(pd.Series([1.,np.nan,0.]),pd.Series([.25,.35,.40]))
assert np.isclose(calc.effective_feature_weight.iloc[0],.25/.65)
assert np.isnan(calc.effective_feature_weight.iloc[1])
assert calc.weighted_feature_contribution.iloc[2] == 0  # valid zero is retained

# Recent slicing is inclusive and exactly 36 monthly observations.
dates=pd.date_range("2020-01-31",periods=60,freq=MONTH_END)
assert len(recent_36(pd.DataFrame({"date":dates}))) == 36

# The implementation imports and reuses governed turning-point helpers.
assert callable(detect_turning_points) and callable(match_turning_points)

# An absent authoritative run fails before creating outputs (no fallback and no
# production/config writes). This hosted fixture intentionally has no run data.
with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp); missing=root/RUN_ID; output=root/"review"
    try: build_review(missing,output,Path.cwd())
    except FileNotFoundError as exc: assert "authoritative run absent" in str(exc)
    else: raise AssertionError("absent authoritative run must fail closed")
    assert not output.exists()
    assert not (root/"production").exists() and not (root/"config").exists()

# Governance is fixed in source and production registries are only read.
source=Path("regime/experiments/demand_signal_attenuation.py").read_text()
assert '"human_decision":"pending"' in source
assert '"automated_winner":False' in source
assert '"production_policy_changed":False' in source
assert "detect_turning_points, match_turning_points" in source
assert ".to_csv(output/" in source and "config/" not in source.split("to_csv")[1]

print("Demand signal attenuation smoke test passed")
