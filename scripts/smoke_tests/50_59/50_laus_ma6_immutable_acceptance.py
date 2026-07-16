from __future__ import annotations
# scripts/smoke_tests/50_59/50_laus_ma6_immutable_acceptance.py

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore
from regime._01_feature_engine import _compute_feature

INCUMBENT_RUN_ID = "macro_regime_v1_bps120_sources"
CANDIDATE_RUN_ID = "macro_regime_v1_bps120_laus_ma6"
OUTPUT_DIR = Path("artifacts/regime/comparisons/laus_ma6_immutable_acceptance")
LAUS_METRICS = ("laus_employment", "laus_labor_force", "laus_unemployment_rate")
LAUS_FEATURES = {m: {"level": f"{m}_level", "short": f"{m}_short", "long": f"{m}_long"} for m in LAUS_METRICS}
LAUS_FEATURE_KEYS = {v for d in LAUS_FEATURES.values() for v in d.values()}
AFFECTED_METRIC_KEYS = set(LAUS_METRICS) | {"employment", "labor_force"}
ARTIFACTS = {
    "raw_features": ["geo_id", "date", "canonical_metric_key", "feature_key"],
    "normalized_features": ["geo_id", "date", "canonical_metric_key", "feature_key"],
    "metric_scores": ["geo_id", "evaluation_date", "metric_key"],
    "dimension_scores": ["geo_id", "evaluation_date", "dimension"],
    "axis_scores": ["geo_id", "evaluation_date", "axis"],
    "coordinates": ["geo_id", "evaluation_date"],
    "regime_assignments": ["geo_id", "evaluation_date"],
}


def _date_col(df: pd.DataFrame) -> str:
    for c in ("date", "evaluation_date", "metric_date"):
        if c in df.columns:
            return c
    raise AssertionError(f"No date column in {list(df.columns)}")


def _canon(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if c in {"date", "evaluation_date", "metric_date"}:
            out[c] = pd.to_datetime(out[c]).dt.strftime("%Y-%m-%d")
    return out


def _check_complete_and_verified(store: RegimeArtifactStore, run_id: str) -> dict[str, Any]:
    manifest = store.read_manifest(run_id)
    if not manifest.get("artifacts"):
        raise AssertionError(f"{run_id} manifest has no artifacts")
    verification = store.verify_run(run_id)
    if verification.empty or not verification["exists"].all() or not verification["hash_matches"].all():
        raise AssertionError(f"{run_id} verify_run failed:\n{verification.to_string(index=False)}")
    for key, meta in manifest["artifacts"].items():
        for field in ("relative_path", "sha256", "row_count", "columns"):
            if field not in meta:
                raise AssertionError(f"{run_id} manifest artifact {key} missing {field}")
    return {"artifact_count": len(manifest["artifacts"]), "verification_rows": len(verification)}


def _read(store: RegimeArtifactStore, run_id: str, artifact: str) -> pd.DataFrame:
    return store.read_dataframe(run_id, artifact)


def _laus_recompute(store: RegimeArtifactStore) -> pd.DataFrame:
    source = _read(store, CANDIDATE_RUN_ID, "source_metrics")
    features = _read(store, CANDIDATE_RUN_ID, "raw_features")
    rows = []
    for metric in LAUS_METRICS:
        src = source[source["canonical_metric_key"].eq(metric) | source.get("metric_origin", pd.Series(index=source.index, dtype=str)).eq(metric)].copy()
        if src.empty:
            src = source[source.get("source_metric_key", pd.Series(index=source.index, dtype=str)).eq(metric)].copy()
        src["date"] = pd.to_datetime(src[_date_col(src)])
        val_col = "value" if "value" in src.columns else "raw_value"
        for geo_id, group in src.groupby("geo_id", sort=False):
            base = group[["date", val_col] + (["metric_origin"] if "metric_origin" in group.columns else [])].rename(columns={val_col: "value"})
            for name, transform, window in (("level", "ma_level", "6m"), ("short", "ma_pct_change", "6m/lag3m"), ("long", "ma_pct_change", "6m/lag12m")):
                s = _compute_feature(base, transform, window, LAUS_FEATURES[metric][name])
                for idx, expected in s.items():
                    rows.append({"geo_id": geo_id, "date": base.loc[idx, "date"], "canonical_metric_key": metric, "feature_key": LAUS_FEATURES[metric][name], "expected": expected})
    expected = pd.DataFrame(rows).dropna(subset=["expected"])
    persisted = features[features["feature_key"].isin(LAUS_FEATURE_KEYS)][["geo_id", "date", "canonical_metric_key", "feature_key", "raw_feature_value"]].copy()
    persisted["date"] = pd.to_datetime(persisted["date"])
    merged = expected.merge(persisted, on=["geo_id", "date", "canonical_metric_key", "feature_key"], how="outer", indicator=True)
    bad = merged[(merged["_merge"].ne("both")) | (~np.isclose(merged["expected"], merged["raw_feature_value"], rtol=0.0, atol=0.0, equal_nan=True))]
    return bad.assign(date=lambda d: pd.to_datetime(d["date"]).dt.strftime("%Y-%m-%d"))


def _compare_exact(left: pd.DataFrame, right: pd.DataFrame, keys: list[str], label: str) -> dict[str, Any]:
    left = _canon(left).sort_values(keys).reset_index(drop=True)
    right = _canon(right).sort_values(keys).reset_index(drop=True)
    if left.duplicated(keys).any() or right.duplicated(keys).any():
        raise AssertionError(f"{label} duplicate comparison keys")
    if list(left.columns) != list(right.columns):
        raise AssertionError(f"{label} schema drift: {list(left.columns)} != {list(right.columns)}")
    if len(left) != len(right):
        raise AssertionError(f"{label} row-count difference: {len(left)} != {len(right)}")
    lk, rk = set(map(tuple, left[keys].to_numpy())), set(map(tuple, right[keys].to_numpy()))
    if lk != rk:
        raise AssertionError(f"{label} key-set difference: left_only={len(lk-rk)} right_only={len(rk-lk)}")
    right = right.set_index(keys).loc[pd.MultiIndex.from_frame(left[keys])].reset_index()
    neq = left.ne(right)
    if neq.any().any():
        raise AssertionError(f"{label} value differences: {int(neq.any(axis=1).sum())} rows")
    return {"rows": len(left), "keys": keys}


def _diff_count(left: pd.DataFrame, right: pd.DataFrame, keys: list[str]) -> int:
    common_cols = [c for c in left.columns if c in right.columns]
    value_cols = [c for c in common_cols if c not in keys]
    l = _canon(left[common_cols]).sort_values(keys).reset_index(drop=True)
    r = _canon(right[common_cols]).sort_values(keys).reset_index(drop=True)
    merged = l.merge(
        r,
        on=keys,
        how="outer",
        suffixes=("_incumbent", "_candidate"),
        indicator=True,
    )
    changed = merged["_merge"].ne("both")
    for column in value_cols:
        incumbent = merged[f"{column}_incumbent"]
        candidate = merged[f"{column}_candidate"]
        changed = changed | (incumbent.ne(candidate) & ~(incumbent.isna() & candidate.isna()))
    return int(changed.sum())


def _manifest_checks(manifest: dict[str, Any]) -> dict[str, Any]:
    meta = manifest.get("metadata", {})
    if meta.get("smoothing_experiment_id") is not None:
        raise AssertionError("candidate smoothing_experiment_id is not None")
    if "feature_registry_hash" not in json.dumps(manifest, sort_keys=True):
        raise AssertionError("candidate manifest missing feature_registry hash")
    snapshot = meta.get("ma_transform_policy_snapshot") or manifest.get("ma_transform_policy_snapshot") or []
    text = json.dumps(snapshot, sort_keys=True)
    missing = [f for f in sorted(LAUS_FEATURE_KEYS) if f not in text]
    if missing or text.count("laus_") < 9:
        raise AssertionError(f"candidate manifest missing approved LAUS policy rows: {missing}")
    return {"laus_policy_rows": 9, "smoothing_experiment_id": None}


def run(store: RegimeArtifactStore, output_dir: Path = OUTPUT_DIR) -> dict[str, Any]:
    summary: dict[str, Any] = {"incumbent_run": INCUMBENT_RUN_ID, "candidate_run": CANDIDATE_RUN_ID, "checks": {}}
    summary["checks"]["incumbent_manifest"] = _check_complete_and_verified(store, INCUMBENT_RUN_ID)
    summary["checks"]["candidate_manifest"] = _check_complete_and_verified(store, CANDIDATE_RUN_ID)
    summary["checks"]["candidate_manifest_policy"] = _manifest_checks(store.read_manifest(CANDIDATE_RUN_ID))
    mismatches = _laus_recompute(store)
    if not mismatches.empty:
        output_dir.mkdir(parents=True, exist_ok=True)
        mismatches.to_csv(output_dir / "laus_feature_mismatches.csv", index=False)
        raise AssertionError(f"Persisted LAUS feature mismatches: {len(mismatches)}")
    summary["checks"]["laus_recompute"] = {"mismatches": 0, "tolerance": "exact"}

    reports = {}
    exact = {}
    for artifact, keys in ARTIFACTS.items():
        inc, cand = _read(store, INCUMBENT_RUN_ID, artifact), _read(store, CANDIDATE_RUN_ID, artifact)
        if artifact in {"raw_features", "normalized_features"}:
            inc_exact, cand_exact = inc[~inc["feature_key"].isin(LAUS_FEATURE_KEYS)], cand[~cand["feature_key"].isin(LAUS_FEATURE_KEYS)]
            reports["laus_feature_rows_changed"] = _diff_count(inc[inc["feature_key"].isin(LAUS_FEATURE_KEYS)], cand[cand["feature_key"].isin(LAUS_FEATURE_KEYS)], keys)
        elif artifact == "metric_scores":
            inc_exact, cand_exact = inc[~inc["metric_key"].isin(AFFECTED_METRIC_KEYS)], cand[~cand["metric_key"].isin(AFFECTED_METRIC_KEYS)]
            reports["affected_metric_score_rows_changed"] = _diff_count(inc[inc["metric_key"].isin(AFFECTED_METRIC_KEYS)], cand[cand["metric_key"].isin(AFFECTED_METRIC_KEYS)], keys)
        elif artifact == "dimension_scores":
            inc_exact, cand_exact = inc[~inc["dimension"].eq("Demand")], cand[~cand["dimension"].eq("Demand")]
            reports["demand_dimension_rows_changed"] = _diff_count(inc[inc["dimension"].eq("Demand")], cand[cand["dimension"].eq("Demand")], keys)
        elif artifact == "axis_scores":
            inc_exact, cand_exact = inc[~inc["axis"].eq("Demand")], cand[~cand["axis"].eq("Demand")]
            reports["demand_axis_rows_changed"] = _diff_count(inc[inc["axis"].eq("Demand")], cand[cand["axis"].eq("Demand")], keys)
        else:
            reports[f"{artifact}_rows_changed"] = _diff_count(inc, cand, keys)
            continue
        exact[artifact] = _compare_exact(inc_exact, cand_exact, keys, artifact)
    summary["checks"]["immutable_isolation"] = exact
    summary["reported_changes"] = reports
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "acceptance_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def _write_fixture_run(store: RegimeArtifactStore, run_id: str, candidate: bool) -> None:
    store.initialize_run(run_id, experiment_id="fixture", metadata={"smoothing_experiment_id": None, "feature_registry_hash": "abc", "ma_transform_policy_snapshot": sorted(LAUS_FEATURE_KEYS)}, overwrite=True)
    dates = pd.date_range("2020-01-01", periods=20, freq="MS")
    src = pd.DataFrame([{"geo_id": "g", "date": d, "canonical_metric_key": m, "value": float(i + 10), "metric_origin": m} for m in LAUS_METRICS for i, d in enumerate(dates)])
    store.write_dataframe(run_id, "source_metrics", src, allow_overwrite=True)
    rf = []
    for m in LAUS_METRICS:
        g = src[src["canonical_metric_key"].eq(m)][["date", "value", "metric_origin"]]
        for n, t, w in (("level", "ma_level", "6m"), ("short", "ma_pct_change", "6m/lag3m"), ("long", "ma_pct_change", "6m/lag12m")):
            s = _compute_feature(g, t, w, LAUS_FEATURES[m][n])
            for idx, v in s.dropna().items(): rf.append({"geo_id":"g","date":g.loc[idx,"date"],"canonical_metric_key":m,"feature_key":LAUS_FEATURES[m][n],"raw_feature_value":v})
    rf.append({"geo_id":"g","date":dates[-1],"canonical_metric_key":"redfin_inventory","feature_key":"redfin_inventory_level","raw_feature_value":1.0})
    for artifact in ("raw_features", "normalized_features"):
        store.write_dataframe(run_id, artifact, pd.DataFrame(rf), allow_overwrite=True)
    for artifact, cols, row in [
        ("metric_scores", ["geo_id","evaluation_date","metric_key","metric_score"], ["g",dates[-1],"redfin_inventory",1.0]),
        ("dimension_scores", ["geo_id","evaluation_date","dimension","dimension_score"], ["g",dates[-1],"Supply",1.0]),
        ("axis_scores", ["geo_id","evaluation_date","axis","axis_score"], ["g",dates[-1],"Supply",1.0]),
        ("coordinates", ["geo_id","evaluation_date","x","y"], ["g",dates[-1],1.0,2.0]),
        ("regime_assignments", ["geo_id","evaluation_date","regime"], ["g",dates[-1],"expansion"]),
    ]: store.write_dataframe(run_id, artifact, pd.DataFrame([row], columns=cols), allow_overwrite=True)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    p.add_argument("--output-dir", default=str(OUTPUT_DIR))
    p.add_argument("--fixture-test", action="store_true")
    args = p.parse_args()
    if args.fixture_test:
        tmp = Path(tempfile.mkdtemp())
        try:
            store = RegimeArtifactStore(tmp)
            _write_fixture_run(store, INCUMBENT_RUN_ID, False); _write_fixture_run(store, CANDIDATE_RUN_ID, True)
            run(store, Path(args.output_dir))
        finally:
            shutil.rmtree(tmp)
    else:
        run(RegimeArtifactStore(args.artifact_root), Path(args.output_dir))
    print("[laus_ma6_immutable_acceptance] OK")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
