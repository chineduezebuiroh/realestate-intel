"""Fast synthetic contract smoke for Structural detector semantics."""
import numpy as np
import pandas as pd
from regime.experiments.demand_signal_attenuation import GEOS
from regime.experiments.structural_turn_detector_semantics import analyze_county
from regime.pandas_compat import MONTH_END

def frame(values):
    return pd.DataFrame({"date":pd.date_range("2020-01-31", periods=len(values), freq=MONTH_END),
                         "structural_score":values})

geo = GEOS[0]
_, accepted, parity = analyze_county(frame([0,1,2,3,4,3,2,1,0]), geo)
assert parity["parity_pass"] and accepted.detector_accept.sum() == 1 and accepted.qualification_pass.sum() == 1
_, low, parity = analyze_county(frame(np.array([0,1,2,3,4,3,2,1,0]) * .001), geo)
assert parity["parity_pass"] and low.detector_accept.sum() == 1 and not low.prominence_pass.all()
_, short, parity = analyze_county(frame([0,0,1,2,3,2,3,2,1,0]), geo)
assert parity["parity_pass"] and (~short.persistence_pass).any()
assert "INSUFFICIENT_PERSISTENCE" in set(short.primary_rejection_reason)
_, monotonic, parity = analyze_county(frame(range(12)), geo)
assert parity["parity_pass"] and monotonic.empty
_, multi, _ = analyze_county(frame(np.array([0,0,1,2,3,2,3,2,1,0]) * .001), geo)
row = multi.loc[(~multi.persistence_pass) & (~multi.prominence_pass)].iloc[0]
assert row.primary_rejection_reason == "INSUFFICIENT_PERSISTENCE" and "INSUFFICIENT_PROMINENCE" in row.failed_criteria
print("Structural turn detector semantics smoke test passed")
