from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

import pandas as pd

import scripts.build_supply_metric_weight_diagnostic as diag

DATE1 = pd.Timestamp("2024-01-31")
DATE2 = pd.Timestamp("2024-02-29")
GEO = diag.REVIEW_GEOS[0]
OTHER = "new_york_ny__cbsa_metro"


def _fixture(root: Path) -> Path:
    src = root / "run"
    src.mkdir()
    rows=[]
    scores={DATE1: {"active_inventory": .2, "permit_activity": .8, "permit_intensity": -.4, "population": .1, "mortgage_30y": .3}, DATE2: {"active_inventory": .4, "permit_activity": None, "permit_intensity": -.2, "population": .2, "mortgage_30y": .1}}
    for geo in [GEO, OTHER]:
        for date, vals in scores.items():
            for metric, score in vals.items():
                rows.append({"geo_id":geo,"evaluation_date":date,"canonical_metric_key":metric,"metric_score":score,"metric_age_days":0})
    pd.DataFrame(rows).to_parquet(src/"aligned_metric_scores.parquet", index=False)
    for name in ["metric_scores","dimension_scores","axis_scores","coordinates","geometry","regime_assignments"]:
        pd.DataFrame().to_parquet(src/f"{name}.parquet", index=False)
    (src/"manifest.json").write_text('{"run_id":"fixture_run"}')
    return src


def _run(root: Path, name: str) -> Path:
    out = root / name
    diag.main(["", str(root/"run"), str(out)])
    return out


def test_contract():
    assert tuple(diag.SUPPLY_METRICS) == ("active_inventory", "permit_activity", "permit_intensity")
    reg = diag._registry()
    assert set(reg.policy_id) == {"incumbent", "challenger_a_60_20_20", "challenger_b_67_165_165"}
    assert all(abs(x - 1.0) < 1e-12 for x in reg.groupby("policy_id").configured_metric_weight.sum())
    assert diag.RECOMMENDATION_STATE == "none" and diag.PROMOTION_STATE == "none"


def test_end_to_end_diagnostic_contract():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td); _fixture(root)
        out1 = _run(root, "out1"); out2 = _run(root, "out2")
        assert hashlib.sha256((out1/diag.CONTRACT_IDENTITY).with_suffix(".zip").read_bytes()).hexdigest() == hashlib.sha256((out2/diag.CONTRACT_IDENTITY).with_suffix(".zip").read_bytes()).hexdigest()
        decomp = pd.read_csv(out1/"metric_to_supply_decomposition.csv")
        chron = pd.read_csv(out1/"supply_chronology.csv")
        coverage = pd.read_csv(out1/"coverage_and_missingness.csv")
        permit = pd.read_csv(out1/"permit_family_influence.csv")
        axis = pd.read_csv(out1/"downstream_axis_propagation.csv")
        trend = pd.read_csv(out1/"trend_responsiveness_diagnostics.csv")
        parity = pd.read_csv(out1/"unaffected_parity.csv")
        assert set(decomp.canonical_metric_key) == set(diag.SUPPLY_METRICS)
        assert set(chron.policy_id) == {"incumbent", "challenger_a_60_20_20", "challenger_b_67_165_165"}
        assert OTHER not in set(chron.geo_id)
        sparse = decomp[(decomp.policy_id.eq("challenger_a_60_20_20")) & (decomp.date.astype(str).str.startswith("2024-02"))]
        assert abs(sparse.available_configured_weight_sum.iloc[0] - 0.8) < 1e-12
        assert abs(sparse[sparse.canonical_metric_key.eq("active_inventory")].effective_weight.iloc[0] - 0.75) < 1e-12
        fam = permit[permit.policy_id.eq("challenger_a_60_20_20")].effective_combined_permit_family_weight_mean.iloc[0]
        assert fam > 0
        assert coverage.all_three_available_observations.max() >= 1 and coverage.one_or_two_metric_observations.max() >= 1
        assert "median_turning_point_delay_days" in trend.columns
        assert axis[axis.axis.eq("supply")].dimension_weight_sum.notna().all()
        assert set(parity.status) == {"pass"}
        assert "recommendation_state: none" in (out1/"index.html").read_text()
        assert (out1/"human_decision_status.json").read_text().find('"promotion_state": "none"') >= 0


def test_duplicate_keys_fail_closed():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td); src = _fixture(root)
        df = pd.read_parquet(src/"aligned_metric_scores.parquet")
        pd.concat([df, df.iloc[[0]]]).to_parquet(src/"aligned_metric_scores.parquet", index=False)
        try:
            _run(root, "bad")
        except ValueError as exc:
            assert "Duplicate governed metric keys" in str(exc)
        else:
            raise AssertionError("duplicate governed keys did not fail closed")

if __name__ == "__main__":
    test_contract(); test_end_to_end_diagnostic_contract(); test_duplicate_keys_fail_closed(); print("supply metric-weight diagnostic smoke passed")
