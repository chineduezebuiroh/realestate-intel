"""Smoke 144: Capital Markets Phase-2 closed grid, isolation, propagation, and review."""
from __future__ import annotations
import hashlib, importlib.util, re, tempfile, warnings
from pathlib import Path
import numpy as np

from regime.diagnostics.capital_markets_feature_weight_calibration import (
    ADJACENT, EXPORTS, FEATURES, POLICIES, build, load_run, write_review,
)
from regime.diagnostics.capital_markets_feature_anatomy import EXPECTED_AXIS_WEIGHTS, EXPECTED_WEIGHTS, NATIVE_GEO
from scripts.build_capital_markets_feature_weight_calibration import DEFAULT_RUN

def fixture():
    path=Path(__file__).with_name("143_capital_markets_feature_anatomy.py")
    spec=importlib.util.spec_from_file_location("cm_phase1_smoke",path); module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module.fixture()

def main():
    warnings.simplefilter("error",RuntimeWarning)
    assert DEFAULT_RUN == Path("artifacts/regime/runs/supply_s8_production_20260817")
    assert list(POLICIES)==[f"P{i}" for i in range(8)] and "P8" not in POLICIES
    assert POLICIES["P6"]==(.60,.05,.35) and POLICIES["P7"]==(.35,.10,.55)
    assert all(np.isclose(sum(w),1) for w in POLICIES.values())
    assert ("P2","P6") in ADJACENT and ("P5","P7") in ADJACENT
    protected=[Path("config/feature_registry.csv"),Path("config/metric_dimension_registry.csv"),Path("config/normalization_registry.csv"),Path("config/axis_registry.csv")]
    before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
    tables=build(fixture(),Path(".")); assert set(EXPORTS).issubset(tables)
    assert set(tables["metric_chronology"].geo_id)=={NATIVE_GEO}
    p0=tables["metric_chronology"].query("policy=='P0'"); assert np.allclose(p0.candidate_metric_score,p0.production_metric_score)
    contrib=tables["feature_contributions"]
    assert not (contrib.groupby(["date","metric","feature_type"]).normalized_feature_score.nunique(dropna=False)>1).any()
    assert set(contrib.feature_type)==set(FEATURES)
    assert set(tables["dimension_statistics"].experiment_metric)==set(EXPECTED_WEIGHTS)
    assert set(tables["demand_axis_statistics"].experiment_metric)==set(EXPECTED_WEIGHTS)
    assert set(tables["supply_axis_statistics"].experiment_metric)==set(EXPECTED_WEIGHTS)
    assert set(tables["responsiveness"].policy)>={"P6","P7"}
    assert set(tables["family_consistency"].family)=={"long_term_rates","policy_rate","spreads"}
    assert set(tables["correlation_audit"].correlation_status).issubset({"ok","insufficient_overlap","left_nonfinite","right_nonfinite","both_nonfinite","left_constant","right_constant","both_constant"})
    gov=tables["governance_status"].iloc[0]; assert gov.candidate_grid=="P0-P7" and gov.family_metric_weight_calibration=="not_started"
    assert not gov.production_policy_changed and not gov.feature_weight_policy_changed and not gov.metric_weight_policy_changed
    assert not gov.Demand_changed and not gov.Supply_changed and not gov.Capital_Markets_changed
    with tempfile.TemporaryDirectory() as tmp:
        out=Path(tmp); write_review(tables,out)
        assert all((out/f"capital_markets_phase2_{name}.csv").is_file() for name in EXPORTS)
        svgs=list(out.glob("*.svg")); assert len(svgs)>=58
        assert all("<path" in p.read_text() and not re.search(r"(?:NaN|Inf)",p.read_text(),re.I) for p in svgs)
        assert (out/"capital_markets_phase2_review_index.html").is_file()
    assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
    try: load_run(Path("/absent/supply_s8_production_20260817"))
    except FileNotFoundError as exc: assert "no substitution" in str(exc)
    else: raise AssertionError("authoritative input absence did not fail closed")
    print("Smoke 144 passed: exact P0-P7 grid, national reconstruction, isolated axes, responsiveness, SVGs, immutability, fail closed")

if __name__=="__main__": main()
