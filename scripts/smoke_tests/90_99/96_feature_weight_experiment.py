"""Focused coverage for the diagnostic-only level-biased experiment."""
from pathlib import Path
import hashlib
import tempfile

import numpy as np
import pandas as pd

from regime.artifacts import RegimeArtifactStore
from regime.diagnostics.feature_weight_experiment import (
    ALTERNATIVES, TARGET_METRICS, REVIEW_GEOGRAPHIES, audit_feature_registry,
    build_evidence, build_policy_registry, coverage_diagnostics,
    validate_unaffected_parity, summarize_propagation, write_review_bundle,
)
from scripts.build_feature_weight_experiment import (
    REQUIRED_ARTIFACTS, load_authoritative_inputs, parse_args,
)


def _runner_fixture(root: Path, run_id: str, *, status: str = "complete",
                    bad_schema: bool = False) -> Path:
    store = RegimeArtifactStore(root); store.initialize_run(run_id, experiment_id="authoritative_fixture")
    date = pd.Timestamp("2022-01-31"); geo = REVIEW_GEOGRAPHIES[0]
    frames = {
        "source_metrics": pd.DataFrame([{"geo_id":geo,"date":date,"canonical_metric_key":"active_inventory"}]),
        "features": pd.DataFrame([{"geo_id":geo,"date":date,"canonical_metric_key":"active_inventory","feature_key":"redfin_inventory_level"}]),
        "normalized_features": pd.DataFrame([{"geo_id":geo,"date":date,"canonical_metric_key":"active_inventory","feature_key":"redfin_inventory_level","feature_score":0.}]),
        "metric_scores": pd.DataFrame([{"geo_id":geo,"date":date,"canonical_metric_key":"active_inventory","metric_score":0.}]),
        "aligned_metric_scores": pd.DataFrame([{"geo_id":geo,"evaluation_date":date,"canonical_metric_key":"active_inventory","metric_score":0.}]),
        "dimension_scores": pd.DataFrame([{"geo_id":g,"date":date,"dimension":"supply","dimension_score":0.} for g in REVIEW_GEOGRAPHIES]),
        "axis_scores": pd.DataFrame([{"geo_id":geo,"date":date,"axis":"supply","axis_score":0.}]),
        "coordinates": pd.DataFrame([{"geo_id":geo,"date":date,"x_supply":0.,"y_demand":0.}]),
        "geometry": pd.DataFrame([{"geo_id":geo,"date":date,"major_regime":"recovery","minor_regime":"mid_recovery"}]),
        "regime_assignments": pd.DataFrame([{"geo_id":geo,"date":date,"major_regime":"recovery","minor_regime":"mid_recovery"}]),
    }
    if bad_schema: frames["coordinates"] = frames["coordinates"].drop(columns="x_supply")
    for name in REQUIRED_ARTIFACTS: store.write_dataframe(run_id,name,frames[name])
    store.update_manifest(run_id,status=status)
    return root/run_id


def main() -> None:
    parsed=parse_args(["some-run","some-output"])
    assert parsed.run_directory == Path("some-run") and parsed.output_directory == Path("some-output")
    paths = [Path("config/feature_registry.csv"), Path("config/metric_dimension_registry.csv"),
             Path("config/source_metric_registry.csv")]
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    feature, metric, source = (pd.read_csv(p) for p in paths)
    audit = audit_feature_registry(feature, metric, source)
    assert tuple(sorted(audit.metric.unique())) == tuple(sorted(TARGET_METRICS))
    assert (audit.groupby("metric").feature_type.nunique() == 3).all()
    assert np.allclose(audit.groupby("metric").current_weight.sum(), 1)
    policies = build_policy_registry(audit)
    assert ALTERNATIVES["alternative_a"] == {"level": .5, "short": .25, "long": .25}
    assert ALTERNATIVES["alternative_b"] == {"level": .45, "short": .25, "long": .30}
    assert ALTERNATIVES["alternative_c"] == {"level": .60, "short": .20, "long": .20}
    assert all(np.isclose(sum(weights.values()), 1.0) for weights in ALTERNATIVES.values())
    assert len(policies[policies.status.eq("incumbent")]) == 7
    assert len(policies) == 35
    assert len(policies[policies.status.eq("challenger")]) == 28
    assert tuple(policies.policy.unique()) == ("incumbent", "ma12_incumbent", "alternative_a", "alternative_b", "alternative_c")
    assert set(policies.feature_definition) == {"incumbent", "ma12_structural"}
    assert not policies.policy.astype(str).str.contains("ma6|ma9", case=False).any()
    rows=[]; dates=pd.date_range("2021-01-31", periods=6, freq="M")
    for gi, geo in enumerate(REVIEW_GEOGRAPHIES):
        for mi, metric_name in enumerate(TARGET_METRICS):
            for fi, key in enumerate(audit[audit.metric.eq(metric_name)].sort_values("feature_type").feature_key):
                for di,date in enumerate(dates):
                    rows.append({"geo_id":geo,"date":date,"feature_key":key,
                                 "feature_score":np.sin(di*.7+fi)+gi*.01+mi*.02})
    synthetic = pd.DataFrame(rows)
    synthetic["date"] = pd.to_datetime(synthetic["date"])
    evidence=build_evidence(synthetic,feature,metric,source)
    assert set(evidence.tables) >= {"stability_diagnostics","trend_preservation_diagnostics",
                                    "level_influence_diagnostics","coverage_and_warmup"}
    assert evidence.tables["feature_to_metric_decomposition"].pass_status.all()
    assert evidence.tables["coverage_and_warmup"].pass_status.eq("pass").all()
    # One metric per challenger policy; no production object was changed.
    assert (policies[policies.status.eq("challenger")].groupby("policy_id").metric.nunique()==1).all()
    unchanged=pd.DataFrame({"id":[1,2],"value":[np.nan,3.]})
    parity=validate_unaffected_parity(unchanged,unchanged.copy(),["id"],artifact="sibling_metrics")
    assert parity.pass_status.iat[0] == "pass"
    for bad in (pd.concat([unchanged,unchanged.iloc[[0]]]), unchanged.rename(columns={"value":"other"})):
        try: validate_unaffected_parity(unchanged,bad,["id"],artifact="bad")
        except ValueError: pass
        else: raise AssertionError("invalid parity evidence accepted")
    inc=pd.DataFrame({"id":[1,2],"score":[0.,1.]}); chal=pd.DataFrame({"id":[1,2],"score":[.1,.8]})
    assert summarize_propagation(inc,chal,["id"],"score",artifact="dimension").changed_count.iat[0]==2
    chronology=evidence.tables["metric_chronology_comparison"].copy()
    mask=(chronology.metric.eq(TARGET_METRICS[0]) & chronology.policy.eq("alternative_a") &
          chronology.geo_id.eq(REVIEW_GEOGRAPHIES[0]))
    interior=chronology[~(mask & chronology.date.eq(dates[2]))]
    try: coverage_diagnostics(interior)
    except ValueError: pass
    else: raise AssertionError("interior gap accepted")
    with tempfile.TemporaryDirectory() as tmp:
        one,two=Path(tmp)/"one",Path(tmp)/"two"
        _,z1,count=write_review_bundle(evidence,one,{"unaffected_parity":parity})
        _,z2,_=write_review_bundle(evidence,two,{"unaffected_parity":parity})
        assert z1.read_bytes()==z2.read_bytes() and count < 300
        page=(one/"index.html").read_text()
        for heading in ("Executive Summary","Active Inventory","MA12 Incumbent","Alternative C","Human Decision Status"): assert heading in page
        manifest=(one/"manifest.json").read_text(); assert '"promotion": "none"' in manifest and '"recommendation": "none"' in manifest
        runner_root=Path(tmp)/"runner"
        incomplete=_runner_fixture(runner_root,"incomplete",status="initialized")
        try: load_authoritative_inputs(incomplete)
        except ValueError as exc: assert "not ready" in str(exc)
        else: raise AssertionError("unready authoritative run accepted")
        malformed=_runner_fixture(runner_root,"malformed",bad_schema=True)
        try: load_authoritative_inputs(malformed)
        except ValueError as exc: assert "schema" in str(exc)
        else: raise AssertionError("schema-incompatible run accepted")
        missing=_runner_fixture(runner_root,"missing")
        (missing/"geometry.parquet").unlink()
        try: load_authoritative_inputs(missing)
        except ValueError as exc: assert "verification" in str(exc)
        else: raise AssertionError("missing authoritative artifact accepted")
        identity=_runner_fixture(runner_root,"identity")
        renamed=runner_root/"wrong_identity"; identity.rename(renamed)
        try: load_authoritative_inputs(renamed)
        except Exception as exc: assert "run_id mismatch" in str(exc)
        else: raise AssertionError("source identity mismatch accepted")
    assert before == {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    print("SMOKE TEST 96 — FEATURE-WEIGHT EXPERIMENT: PASS")


if __name__ == "__main__": main()
