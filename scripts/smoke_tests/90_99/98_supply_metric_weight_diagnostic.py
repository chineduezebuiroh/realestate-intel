from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

import pandas as pd

from regime.pandas_compat import MONTH_END

import scripts.build_supply_metric_weight_diagnostic as diag

GEO = diag.REVIEW_GEOS[0]
OTHER = "new_york_ny__cbsa_metro"


def _fixture(root: Path, stale: bool = False) -> Path:
    src = root / "run"
    src.mkdir()
    dates = pd.date_range("2024-01-31", periods=15, freq=MONTH_END)
    rows=[]
    for geo in [GEO, OTHER]:
        for i, date in enumerate(dates):
            vals = {"active_inventory": i/10, "permit_activity": (i%5)/10, "permit_intensity": (-i%4)/10, "population": .1, "mortgage_30y": .3}
            if date == dates[3]: vals["permit_activity"] = None
            for metric, score in vals.items():
                if score is not None:
                    rows.append({"geo_id":geo,"evaluation_date":date,"canonical_metric_key":metric,"metric_score":score,"metric_age_days":0})
    pd.DataFrame(rows).to_parquet(src/"aligned_metric_scores.parquet", index=False)
    for name in ["metric_scores","dimension_scores","axis_scores","coordinates","geometry","regime_assignments"]:
        pd.DataFrame().to_parquet(src/f"{name}.parquet", index=False)
    proof = "fixture_run" if stale else "fixture_settled_ma12_run"
    (src/"manifest.json").write_text('{"run_id":"%s"}' % proof)
    return src


def _run(root: Path, name: str) -> Path:
    out = root / name
    diag.main(["", str(root/"run"), str(out)])
    return out


def test_contract():
    assert tuple(diag.SUPPLY_METRICS) == ("active_inventory", "permit_activity", "permit_intensity")
    reg = diag._registry()
    assert set(reg.policy_id) == {"incumbent", "challenger_a_60_20_20", "challenger_b_67_165_165"}
    assert diag.RECOMMENDATION_STATE == "none" and diag.PROMOTION_STATE == "none"


def test_end_to_end_diagnostic_contract():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td); _fixture(root)
        out1 = _run(root, "out1"); out2 = _run(root, "out2")
        assert hashlib.sha256((out1/diag.CONTRACT_IDENTITY).with_suffix(".zip").read_bytes()).hexdigest() == hashlib.sha256((out2/diag.CONTRACT_IDENTITY).with_suffix(".zip").read_bytes()).hexdigest()
        stability = pd.read_csv(out1/"stability_diagnostics.csv")
        trend = pd.read_csv(out1/"trend_responsiveness_diagnostics.csv")
        decomp = pd.read_csv(out1/"metric_to_supply_decomposition.csv")
        chron = pd.read_csv(out1/"supply_chronology.csv")
        parity = pd.read_csv(out1/"unaffected_parity.csv")
        assert set(stability.scope) == set(diag.STABILITY_SCOPES)
        assert stability[stability.scope.eq("all_three_available_dates")].observation_count.max() < stability[stability.scope.eq("all_emitted_dates")].observation_count.max()
        assert "excluded_chronology_gap_comparisons" in stability.columns
        assert OTHER not in set(chron.geo_id)
        assert set(chron.geo_id) == {GEO}
        assert (trend[trend.policy_id.eq("incumbent")].agreement_share.dropna() == 1.0).all()
        assert set(decomp.canonical_metric_key) == set(diag.SUPPLY_METRICS)
        assert set(parity.status) == {"pass"}
        assert "recommendation_state: none" in (out1/"index.html").read_text()
        assert (out1/"human_decision_status.json").read_text().find('"promotion_state": "none"') >= 0


def test_directional_agreement_synthetic_cases():
    dates = pd.date_range("2020-01-31", periods=15, freq=MONTH_END)
    base = pd.DataFrame({"geo_id":GEO,"date":dates,"policy_id":"incumbent","supply_dimension_score":range(15),"available_metric_count":3})
    same = base.assign(policy_id="challenger_a_60_20_20")
    inv = base.assign(policy_id="challenger_b_67_165_165", supply_dimension_score=list(reversed(range(15))))
    detail = diag._directional_detail(pd.concat([base, same, inv]))
    assert (detail[detail.policy_id.eq("incumbent")].agreement_share.dropna() == 1.0).all()
    assert (detail[detail.policy_id.eq("challenger_a_60_20_20")].agreement_share.dropna() == 1.0).all()
    assert detail[(detail.policy_id.eq("challenger_b_67_165_165")) & (detail.horizon_months.eq(1))].disagreements.iloc[0] > 0
    flat = pd.concat([base.assign(supply_dimension_score=1), same.assign(supply_dimension_score=1)])
    assert (diag._directional_detail(flat).agreement_share.dropna() == 1.0).all()
    gap = base.drop(index=[2]).copy()
    assert diag._directional_detail(pd.concat([gap, gap.assign(policy_id="challenger_a_60_20_20")])).excluded_chronology_gaps.max() > 0
    assert detail[detail.horizon_months.eq(12)].valid_comparisons.max() == 3


def test_turning_points_and_matching():
    dates = pd.date_range("2020-01-31", periods=14, freq=MONTH_END)
    vals = [0,1,2,3,2,1,0,1,2,3,2,1,0,-1]
    g = pd.DataFrame({"geo_id":GEO,"date":dates,"policy_id":"incumbent","supply_dimension_score":vals,"available_metric_count":3})
    turns = diag._detect_turns(g)
    assert set(turns.turning_point_type) == {"peak", "trough"}
    noise = g.assign(supply_dimension_score=[0,.01,0,.01,0,.01,0,.01,0,.01,0,.01,0,.01])
    assert diag._detect_turns(noise).empty
    short = g.iloc[:6]
    assert diag._detect_turns(short).empty
    allturns = pd.concat([turns.assign(geo_id=GEO, policy_id="incumbent"), turns.assign(geo_id=GEO, policy_id="challenger_a_60_20_20", turning_point_date=turns.turning_point_date + pd.offsets.MonthEnd(1)), turns.assign(geo_id=GEO, policy_id="challenger_b_67_165_165", turning_point_date=turns.turning_point_date + pd.offsets.MonthEnd(20))], ignore_index=True)
    matches = diag._match_turns(allturns)
    assert matches[matches.policy_id.eq("challenger_a_60_20_20")].absolute_delay_months.notna().sum() == len(turns)
    assert matches[matches.policy_id.eq("challenger_b_67_165_165")].absolute_delay_months.notna().sum() == 0
    summary = diag._turn_summary(allturns, matches)
    assert summary[summary.policy_id.eq("challenger_b_67_165_165")].median_matched_delay_months.isna().all()


def test_duplicate_and_stale_fail_closed():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td); src = _fixture(root)
        df = pd.read_parquet(src/"aligned_metric_scores.parquet")
        pd.concat([df, df.iloc[[0]]]).to_parquet(src/"aligned_metric_scores.parquet", index=False)
        try: _run(root, "bad")
        except ValueError as exc: assert "Duplicate governed metric keys" in str(exc)
        else: raise AssertionError("duplicate governed keys did not fail closed")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td); _fixture(root, stale=True)
        try: _run(root, "stale")
        except ValueError as exc: assert "settled_ma12" in str(exc)
        else: raise AssertionError("stale run did not fail closed")

if __name__ == "__main__":
    test_contract(); test_end_to_end_diagnostic_contract(); test_directional_agreement_synthetic_cases(); test_turning_points_and_matching(); test_duplicate_and_stale_fail_closed(); print("supply metric-weight diagnostic smoke passed")
