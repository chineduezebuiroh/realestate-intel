"""Smoke 146: closed Capital Markets family-weight diagnostic contract."""
from __future__ import annotations
import hashlib, importlib.util, re, tempfile
from pathlib import Path
import numpy as np

from regime.diagnostics.capital_markets_family_weight_calibration import (
    AUTHORITATIVE_RUN, CONTROLLED, EXPORTS, FEDFUNDS_LADDER, FAMILIES,
    NATIVE_POLICIES, POLICIES, RATES_SPREADS_LADDER, build, load_run,
    metric_weights, write_review,
)
from scripts.build_capital_markets_family_weight_calibration import DEFAULT_RUN

def fixture():
    path=Path(__file__).with_name("143_capital_markets_feature_anatomy.py")
    spec=importlib.util.spec_from_file_location("cm_fixture",path); module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module.fixture()

def main():
    assert DEFAULT_RUN==AUTHORITATIVE_RUN==Path("artifacts/regime/runs/capital_markets_feature_policy_corrected_production_20260818")
    assert list(POLICIES)==[f"F{i}" for i in range(10)] and "F10" not in POLICIES
    assert all(np.isclose(sum(v),1) for v in POLICIES.values())
    weights=metric_weights(); assert len(weights)==60 and np.allclose(weights.groupby("policy").configured_metric_weight.sum(),1)
    for policy,w in weights.groupby("policy"):
        assert w[w.family.eq("long_term_rates")].configured_metric_weight.nunique()==1
        assert w[w.family.eq("spreads")].configured_metric_weight.nunique()==1
    assert NATIVE_POLICIES=={"mortgage_30y":"P4","mortgage_15y":"P2","treasury_10y":"P1","fedfunds":"P5","spread_10y_2y":"P6","spread_10y_fedfunds":"P9"}
    assert RATES_SPREADS_LADDER==("F3","F1","F0","F2","F4")
    assert FEDFUNDS_LADDER==("F5","F0","F6","F7","F8") and ("F5","F9") in CONTROLLED
    protected=[Path("config/feature_registry.csv"),Path("config/metric_dimension_registry.csv"),Path("config/normalization_registry.csv"),Path("config/axis_registry.csv"),Path("config/supply_metric_weight_s8_2026_08_17.json")]
    before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
    tables=build(fixture(),Path(".")); assert set(EXPORTS)==set(tables)
    control=tables["incumbent_comparison"].query("policy=='F0'"); assert np.allclose(control.correlation,1)
    assert {"configured_metric_weight","effective_metric_weight","weighted_metric_contribution"}.issubset(tables["contributions"])
    replay=tables["contributions"].groupby(["policy","geo_id","date"]).weighted_metric_contribution.sum(min_count=1)
    actual=tables["dimension_chronology"].set_index(["policy","geo_id","date"]).candidate_capital_markets
    assert np.allclose(replay,actual,equal_nan=True)
    assert len(tables["family_overlap"]) and len(tables["within_family_consistency"])
    assert set(tables["controlled_comparisons"].comparison_group)=={"rates_spreads_ladder","fedfunds_ladder","low_fed_interaction"}
    assert tables["demand_axis_statistics"].configured_capital_markets_weight.eq(.10).all()
    assert tables["supply_axis_statistics"].configured_capital_markets_weight.eq(.15).all()
    assert tables["responsiveness"].threshold_provenance.eq("invariant family monthly-move median").all()
    gov=tables["governance_status"].iloc[0]; assert gov.candidate_grid=="F0-F9" and gov.native_feature_calibration=="closed" and gov.spread_polarity_repair=="closed" and gov.intra_family_metric_weight_calibration=="not_started"
    assert gov.human_decision=="capital_markets_family_weight_review_pending" and gov.family_metric_weight_calibration=="in_review" and gov.promotion_state=="current_production_unchanged"
    assert not gov.prior_invalidated_evidence_reused and len(tables["evaluation_matrix"])==13
    assert set(tables["performance_audit"].stage)>={"artifact load","candidate construction","contribution calculations","stability statistics","responsiveness","overlap/consistency","downstream propagation"}
    assert not gov.production_policy_changed and not gov.metric_weight_policy_changed and not gov.supply_s8_changed
    with tempfile.TemporaryDirectory() as tmp:
        out=Path(tmp); write_review(tables,out)
        assert all((out/f"capital_markets_family_weight_{name}.csv").is_file() for name in EXPORTS)
        assert all("<path" in p.read_text() and not re.search(r"(?:NaN|Inf)",p.read_text(),re.I) for p in out.glob("*.svg"))
        assert (out/"capital_markets_family_weight_review_index.html").is_file()
        assert {"visualization","export/write"}.issubset(set(tables["performance_audit"].stage))
    assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
    try: load_run(Path("/absent/capital_markets_feature_policy_corrected_production_20260818"))
    except FileNotFoundError as exc: assert "no substitution" in str(exc)
    else: raise AssertionError("authoritative absence did not fail closed")
    print("Smoke 146 passed: exact family grid, reconstruction, overlap, axes, SVGs, governance, fail closed")

if __name__=="__main__": main()
