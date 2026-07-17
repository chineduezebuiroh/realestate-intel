from __future__ import annotations
# scripts/smoke_tests/50_59/50_laus_ma6_immutable_acceptance.py

import argparse
import json
import re
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
LAUS_CANONICAL_METRIC_KEYS = {
    "laus_employment": "employment",
    "laus_labor_force": "labor_force",
    "laus_unemployment_rate": "laus_unemployment_rate",
}
LAUS_FEATURES = {m: {"level": f"{m}_level", "short": f"{m}_short", "long": f"{m}_long"} for m in LAUS_METRICS}
LAUS_FEATURE_KEYS = {v for d in LAUS_FEATURES.values() for v in d.values()}
CES_EMPLOYMENT_FEATURE_KEYS = {
    "ces_total_nonfarm_level",
    "ces_total_nonfarm_short",
    "ces_total_nonfarm_long",
}
LAUS_EMPLOYMENT_FEATURE_KEYS = {
    "laus_employment_level",
    "laus_employment_short",
    "laus_employment_long",
}
EMPLOYMENT_REATTRIBUTION_FEATURE_KEYS = LAUS_EMPLOYMENT_FEATURE_KEYS | CES_EMPLOYMENT_FEATURE_KEYS
AFFECTED_CANONICAL_LABOR_METRICS = {"employment", "labor_force", "laus_unemployment_rate"}
ARTIFACTS = {
    "features": ["geo_id", "date", "canonical_metric_key", "feature_key"],
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


def _bool_array(mask: pd.Series) -> np.ndarray:
    return mask.fillna(False).astype(bool).to_numpy(dtype=bool)


def _source_lineage(source: pd.DataFrame, physical_metric_key: str) -> pd.Series:
    masks = []
    for column in ("source_metric_key", "metric_origin"):
        if column in source.columns:
            masks.append(source[column].astype("string").eq(physical_metric_key))
    if not masks:
        raise AssertionError(
            f"source_metrics has no source_metric_key or metric_origin lineage column for {physical_metric_key}"
        )
    lineage = masks[0].copy()
    for mask in masks[1:]:
        lineage = lineage | mask
    if not lineage.any():
        raise AssertionError(f"source_metrics has no rows for physical LAUS source {physical_metric_key}")
    return lineage.fillna(False)


def _laus_recompute(store: RegimeArtifactStore) -> pd.DataFrame:
    source = _read(store, CANDIDATE_RUN_ID, "source_metrics")
    features = _read(store, CANDIDATE_RUN_ID, "features")
    rows = []
    for physical_metric_key in LAUS_METRICS:
        canonical_metric_key = LAUS_CANONICAL_METRIC_KEYS[physical_metric_key]
        src = source[_source_lineage(source, physical_metric_key)].copy()
        duplicate_keys = ["geo_id", _date_col(src)]
        if src.duplicated(duplicate_keys).any():
            dupes = src.loc[src.duplicated(duplicate_keys, keep=False), duplicate_keys].head(10)
            raise AssertionError(
                f"source_metrics duplicate geo/date rows for {physical_metric_key}:\n"
                f"{dupes.to_string(index=False)}"
            )
        src["date"] = pd.to_datetime(src[_date_col(src)])
        val_col = "value" if "value" in src.columns else "raw_value"
        for geo_id, group in src.groupby("geo_id", sort=False):
            base = group[["date", val_col] + (["metric_origin"] if "metric_origin" in group.columns else [])].rename(columns={val_col: "value"})
            for name, transform, window in (("level", "ma_level", "6m"), ("short", "ma_pct_change", "6m/lag3m"), ("long", "ma_pct_change", "6m/lag12m")):
                feature_key = LAUS_FEATURES[physical_metric_key][name]
                s = _compute_feature(base, transform, window, feature_key)
                for idx, expected in s.items():
                    rows.append(
                        {
                            "geo_id": geo_id,
                            "date": base.loc[idx, "date"],
                            "physical_metric_key": physical_metric_key,
                            "canonical_metric_key": canonical_metric_key,
                            "feature_key": feature_key,
                            "expected": expected,
                        }
                    )
    expected = pd.DataFrame(rows).dropna(subset=["expected"])
    persisted = features[features["feature_key"].isin(LAUS_FEATURE_KEYS)][["geo_id", "date", "canonical_metric_key", "feature_key", "raw_feature_value"]].copy()
    persisted["date"] = pd.to_datetime(persisted["date"])
    merged = expected.merge(persisted, on=["geo_id", "date", "canonical_metric_key", "feature_key"], how="outer", indicator=True)
    merged["absolute_difference"] = (merged["expected"] - merged["raw_feature_value"]).abs()
    bad = merged[(merged["_merge"].ne("both")) | (~np.isclose(merged["expected"], merged["raw_feature_value"], rtol=0.0, atol=0.0, equal_nan=True))]
    bad = bad.rename(columns={"raw_feature_value": "persisted_value", "_merge": "merge_status"})
    columns = [
        "geo_id",
        "date",
        "physical_metric_key",
        "canonical_metric_key",
        "feature_key",
        "expected",
        "persisted_value",
        "merge_status",
        "absolute_difference",
    ]
    return bad.assign(date=lambda d: pd.to_datetime(d["date"]).dt.strftime("%Y-%m-%d"))[columns]



def _employment_source_origin_checks(store: RegimeArtifactStore) -> dict[str, Any]:
    source = _read(store, CANDIDATE_RUN_ID, "source_metrics").copy()
    features = _read(store, CANDIDATE_RUN_ID, "features").copy()
    source["date"] = pd.to_datetime(source[_date_col(source)]).dt.strftime("%Y-%m-%d")
    features["date"] = pd.to_datetime(features["date"]).dt.strftime("%Y-%m-%d")
    employment_source = source[source["canonical_metric_key"].astype("string").eq("employment")].copy()
    if employment_source.empty:
        raise AssertionError("candidate source_metrics has no canonical employment rows")
    if not {"source_metric_key", "metric_origin"} & set(employment_source.columns):
        raise AssertionError("candidate source_metrics has no source_metric_key or metric_origin lineage column")
    source_key = (
        employment_source.get("source_metric_key", pd.Series(pd.NA, index=employment_source.index))
        .astype("string")
        .str.strip()
    )
    metric_origin = (
        employment_source.get("metric_origin", pd.Series(pd.NA, index=employment_source.index))
        .astype("string")
        .str.strip()
    )
    ces_mask = source_key.eq("ces_total_nonfarm").fillna(False) | metric_origin.eq("ces_total_nonfarm").fillna(False)
    laus_mask = source_key.eq("laus_employment").fillna(False) | metric_origin.eq("laus_employment").fillna(False)
    employment_source["origin_family"] = np.select(
        [_bool_array(ces_mask), _bool_array(laus_mask)],
        ["ces", "laus"],
        default="unknown",
    )
    unknown = employment_source[employment_source["origin_family"].eq("unknown")]
    if not unknown.empty:
        columns = [
            c
            for c in ("geo_id", "date", "source_metric_key", "metric_origin", "canonical_metric_key")
            if c in unknown.columns
        ]
        examples = unknown[columns].head(10).to_dict("records")
        raise AssertionError(f"candidate canonical employment source lineage has unknown origins: {examples}")
    if not set(employment_source["origin_family"]) >= {"ces", "laus"}:
        raise AssertionError("candidate canonical employment source lineage must include both CES and LAUS origins")
    source_keys = ["geo_id", "date", "canonical_metric_key"]
    mixed = employment_source.groupby(source_keys)["origin_family"].nunique()
    if (mixed > 1).any():
        examples = mixed[mixed > 1].head(5).reset_index().to_dict("records")
        raise AssertionError(f"candidate employment source lineage is ambiguous for geo/date rows: {examples}")
    employment_features = features[features["canonical_metric_key"].astype("string").eq("employment")].copy()
    attributed = employment_features.merge(
        employment_source[source_keys + ["origin_family"]].drop_duplicates(),
        on=source_keys,
        how="inner",
    )
    ces_rows = attributed[attributed["origin_family"].eq("ces")]
    laus_rows = attributed[attributed["origin_family"].eq("laus")]
    ces_bad = ces_rows[ces_rows["feature_key"].isin(LAUS_EMPLOYMENT_FEATURE_KEYS)]
    laus_bad = laus_rows[laus_rows["feature_key"].isin(CES_EMPLOYMENT_FEATURE_KEYS)]
    if ces_bad.empty is False:
        raise AssertionError(f"LAUS employment features appeared on CES-origin observations: {len(ces_bad)}")
    if laus_bad.empty is False:
        raise AssertionError(f"CES employment features appeared on LAUS-origin observations: {len(laus_bad)}")
    if not set(ces_rows["feature_key"]) & CES_EMPLOYMENT_FEATURE_KEYS:
        raise AssertionError("CES-origin canonical employment rows produced no ces_total_nonfarm_* features")
    if not set(laus_rows["feature_key"]) & LAUS_EMPLOYMENT_FEATURE_KEYS:
        raise AssertionError("LAUS-origin canonical employment rows produced no laus_employment_* features")
    return {
        "ces_origin_feature_rows": int(len(ces_rows[ces_rows["feature_key"].isin(CES_EMPLOYMENT_FEATURE_KEYS)])),
        "laus_origin_feature_rows": int(len(laus_rows[laus_rows["feature_key"].isin(LAUS_EMPLOYMENT_FEATURE_KEYS)])),
        "ces_origin_laus_feature_rows": int(len(ces_bad)),
        "laus_origin_ces_feature_rows": int(len(laus_bad)),
    }


def _employment_source_reattribution_report(store: RegimeArtifactStore) -> list[dict[str, Any]]:
    rows = []
    for run_id, side in ((INCUMBENT_RUN_ID, "incumbent"), (CANDIDATE_RUN_ID, "candidate")):
        features = _read(store, run_id, "features")
        counts = (
            features[features["feature_key"].isin(EMPLOYMENT_REATTRIBUTION_FEATURE_KEYS)]
            .groupby(["canonical_metric_key", "feature_key"])
            .size()
            .rename(side)
            .reset_index()
        )
        rows.append(counts)
    baseline = pd.DataFrame(
        {"canonical_metric_key": "employment", "feature_key": feature_key}
        for feature_key in sorted(EMPLOYMENT_REATTRIBUTION_FEATURE_KEYS)
    )
    report = baseline.merge(rows[0], on=["canonical_metric_key", "feature_key"], how="left").merge(
        rows[1], on=["canonical_metric_key", "feature_key"], how="left"
    ).fillna(0)
    for column in ("incumbent", "candidate"):
        report[column] = report[column].astype(int)
    report["candidate_minus_incumbent"] = report["candidate"] - report["incumbent"]
    return report.sort_values(["canonical_metric_key", "feature_key"]).to_dict("records")


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
        raise AssertionError(
            f"{label} key-set difference: left_only={len(lk-rk)} right_only={len(rk-lk)} "
            f"left_examples={list(sorted(lk-rk))[:5]} right_examples={list(sorted(rk-lk))[:5]}"
        )
    right = right.set_index(keys).loc[pd.MultiIndex.from_frame(left[keys])].reset_index()
    neq = left.ne(right)
    if neq.any().any():
        differing_rows = neq.any(axis=1)
        differing_cols = [c for c in left.columns if bool(neq[c].any())]
        examples = []
        for idx in list(neq.index[differing_rows])[:5]:
            row = {key: left.loc[idx, key] for key in keys}
            row["differing_columns"] = [c for c in differing_cols if bool(neq.loc[idx, c])][:5]
            examples.append(row)
        raise AssertionError(
            f"{label} value differences: {int(differing_rows.sum())} rows; "
            f"columns={differing_cols}; examples={examples}"
        )
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


def _is_demand(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.lower().eq("demand").fillna(False)


def _split_demand(
    incumbent: pd.DataFrame,
    candidate: pd.DataFrame,
    column: str,
    label: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    inc_demand = _is_demand(incumbent[column])
    cand_demand = _is_demand(candidate[column])
    if not inc_demand.any():
        raise AssertionError(f"incumbent {label} is missing Demand {column}")
    if not cand_demand.any():
        raise AssertionError(f"candidate {label} is missing Demand {column}")
    return incumbent[~inc_demand], candidate[~cand_demand], incumbent[inc_demand], candidate[cand_demand]


def _manifest_checks(manifest: dict[str, Any]) -> dict[str, Any]:
    meta = manifest.get("metadata", {})
    if manifest.get("status") != "complete":
        raise AssertionError(f"candidate status is not complete: {manifest.get('status')!r}")
    if meta.get("smoothing_experiment_id") is not None:
        raise AssertionError("candidate smoothing_experiment_id is not None")
    config_hashes = meta.get("config_hashes")
    if not isinstance(config_hashes, dict):
        raise AssertionError("candidate manifest metadata.config_hashes is not a dictionary")
    registry_hash = config_hashes.get("config/feature_registry.csv")
    if not isinstance(registry_hash, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", registry_hash.strip()):
        raise AssertionError("candidate manifest missing valid metadata.config_hashes['config/feature_registry.csv']")
    snapshot = meta.get("ma_transform_policy_snapshot") or manifest.get("ma_transform_policy_snapshot") or []
    text = json.dumps(snapshot, sort_keys=True)
    missing = [f for f in sorted(LAUS_FEATURE_KEYS) if f not in text]
    if missing or text.count("laus_") < 9:
        raise AssertionError(f"candidate manifest missing approved LAUS policy rows: {missing}")
    return {"laus_policy_rows": 9, "smoothing_experiment_id": None, "feature_registry_hash": registry_hash}


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
    summary["checks"]["candidate_employment_source_attribution"] = _employment_source_origin_checks(store)

    reports = {"employment_source_reattribution": _employment_source_reattribution_report(store)}
    exact = {}
    for artifact, keys in ARTIFACTS.items():
        inc, cand = _read(store, INCUMBENT_RUN_ID, artifact), _read(store, CANDIDATE_RUN_ID, artifact)
        if artifact in {"features", "normalized_features"}:
            inc_exact, cand_exact = inc[~inc["canonical_metric_key"].isin(AFFECTED_CANONICAL_LABOR_METRICS)], cand[~cand["canonical_metric_key"].isin(AFFECTED_CANONICAL_LABOR_METRICS)]
            reports.setdefault("affected_labor_feature_rows_changed", {})[artifact] = _diff_count(
                inc[inc["canonical_metric_key"].isin(AFFECTED_CANONICAL_LABOR_METRICS)],
                cand[cand["canonical_metric_key"].isin(AFFECTED_CANONICAL_LABOR_METRICS)],
                keys,
            )
            reports.setdefault("unaffected_feature_isolation", {})[artifact] = {
                "incumbent_rows": int(len(inc_exact)),
                "candidate_rows": int(len(cand_exact)),
            }
        elif artifact == "metric_scores":
            inc_exact, cand_exact = inc[~inc["metric_key"].isin(AFFECTED_CANONICAL_LABOR_METRICS)], cand[~cand["metric_key"].isin(AFFECTED_CANONICAL_LABOR_METRICS)]
            reports["affected_metric_score_rows_changed"] = _diff_count(
                inc[inc["metric_key"].isin(AFFECTED_CANONICAL_LABOR_METRICS)],
                cand[cand["metric_key"].isin(AFFECTED_CANONICAL_LABOR_METRICS)],
                keys,
            )
        elif artifact == "dimension_scores":
            inc_exact, cand_exact, inc_demand, cand_demand = _split_demand(inc, cand, "dimension", artifact)
            reports["demand_dimension_rows_changed"] = _diff_count(inc_demand, cand_demand, keys)
        elif artifact == "axis_scores":
            inc_exact, cand_exact, inc_demand, cand_demand = _split_demand(inc, cand, "axis", artifact)
            reports["demand_axis_rows_changed"] = _diff_count(inc_demand, cand_demand, keys)
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
    store.initialize_run(
        run_id,
        experiment_id="fixture",
        metadata={
            "config_hashes": {
                "config/feature_registry.csv": "0" * 64,
            },
            "smoothing_experiment_id": None,
            "ma_transform_policy_snapshot": sorted(LAUS_FEATURE_KEYS),
        },
        overwrite=True,
    )
    dates = pd.date_range("2020-01-01", periods=20, freq="MS")
    source_rows = [
        {
            "geo_id": "g",
            "date": d,
            "canonical_metric_key": LAUS_CANONICAL_METRIC_KEYS[m],
            "source_metric_key": m,
            "value": float(i + 10),
            "metric_origin": m,
        }
        for m in LAUS_METRICS
        for i, d in enumerate(dates)
    ]
    source_rows.extend(
        {
            "geo_id": "ces_g",
            "date": d,
            "canonical_metric_key": "employment",
            "source_metric_key": "ces_total_nonfarm",
            "value": float(i + 100),
            "metric_origin": "ces_total_nonfarm",
        }
        for i, d in enumerate(dates)
    )
    source_rows.extend(
        [
            {
                "geo_id": "null_source_metric_key_g",
                "date": dates[-1],
                "canonical_metric_key": "employment",
                "source_metric_key": pd.NA,
                "value": 200.0,
                "metric_origin": "ces_total_nonfarm",
            },
            {
                "geo_id": "null_metric_origin_g",
                "date": dates[-1],
                "canonical_metric_key": "employment",
                "source_metric_key": "ces_total_nonfarm",
                "value": 201.0,
                "metric_origin": pd.NA,
            },
        ]
    )
    src = pd.DataFrame(source_rows)
    store.write_dataframe(run_id, "source_metrics", src, allow_overwrite=True)
    rf = []
    for m in LAUS_METRICS:
        canonical_metric_key = LAUS_CANONICAL_METRIC_KEYS[m]
        g = src[src["source_metric_key"].eq(m)][["date", "value", "metric_origin"]]
        for n, t, w in (("level", "ma_level", "6m"), ("short", "ma_pct_change", "6m/lag3m"), ("long", "ma_pct_change", "6m/lag12m")):
            s = _compute_feature(g, t, w, LAUS_FEATURES[m][n])
            for idx, v in s.dropna().items(): rf.append({"geo_id":"g","date":g.loc[idx,"date"],"canonical_metric_key":canonical_metric_key,"feature_key":LAUS_FEATURES[m][n],"raw_feature_value":v})
    ces_g = src[src["source_metric_key"].eq("ces_total_nonfarm")][["date", "value", "metric_origin"]]
    ces_features = {"level": "ces_total_nonfarm_level", "short": "ces_total_nonfarm_short", "long": "ces_total_nonfarm_long"}
    for n, t, w in (("level", "ma_level", "6m"), ("short", "ma_pct_change", "6m/lag3m"), ("long", "ma_pct_change", "6m/lag12m")):
        s = _compute_feature(ces_g, t, w, ces_features[n])
        for idx, v in s.dropna().items():
            rf.append({"geo_id":"ces_g","date":ces_g.loc[idx,"date"],"canonical_metric_key":"employment","feature_key":ces_features[n],"raw_feature_value":v})
    rf.append({"geo_id":"g","date":dates[-1],"canonical_metric_key":"redfin_inventory","feature_key":"redfin_inventory_level","raw_feature_value":1.0})
    for artifact in ("features", "normalized_features"):
        store.write_dataframe(run_id, artifact, pd.DataFrame(rf), allow_overwrite=True)
    for artifact, cols, row in [
        ("metric_scores", ["geo_id","evaluation_date","metric_key","metric_score"], ["g",dates[-1],"redfin_inventory",1.0]),
        ("dimension_scores", ["geo_id","evaluation_date","dimension","dimension_score"], ["g",dates[-1],"supply",1.0]),
        ("axis_scores", ["geo_id","evaluation_date","axis","axis_score"], ["g",dates[-1],"supply",1.0]),
        ("coordinates", ["geo_id","evaluation_date","x","y"], ["g",dates[-1],1.0,2.0]),
        ("regime_assignments", ["geo_id","evaluation_date","regime"], ["g",dates[-1],"expansion"]),
    ]: store.write_dataframe(run_id, artifact, pd.DataFrame([row], columns=cols), allow_overwrite=True)
    for artifact, column, value_column in [
        ("dimension_scores", "dimension", "dimension_score"),
        ("axis_scores", "axis", "axis_score"),
    ]:
        df = store.read_dataframe(run_id, artifact)
        df = pd.concat(
            [df, pd.DataFrame([{"geo_id": "g", "evaluation_date": dates[-1], column: "demand", value_column: 2.0 if candidate else 1.5}])],
            ignore_index=True,
        )
        store.write_dataframe(run_id, artifact, df, allow_overwrite=True)
    store.update_manifest(run_id, status="complete")


def _fixture_assert_unknown_origin_failure(output_dir: Path) -> None:
    tmp = Path(tempfile.mkdtemp())
    try:
        store = RegimeArtifactStore(tmp)
        _write_fixture_run(store, INCUMBENT_RUN_ID, False)
        _write_fixture_run(store, CANDIDATE_RUN_ID, True)
        source = store.read_dataframe(CANDIDATE_RUN_ID, "source_metrics")
        source = pd.concat(
            [
                source,
                pd.DataFrame(
                    [
                        {
                            "geo_id": "unknown_origin_g",
                            "date": pd.Timestamp("2020-01-01"),
                            "canonical_metric_key": "employment",
                            "source_metric_key": pd.NA,
                            "value": 999.0,
                            "metric_origin": pd.NA,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
        store.write_dataframe(CANDIDATE_RUN_ID, "source_metrics", source, allow_overwrite=True)
        try:
            _employment_source_origin_checks(store)
        except AssertionError as exc:
            message = str(exc)
            required = ["unknown origins", "unknown_origin_g", "source_metric_key", "metric_origin", "canonical_metric_key"]
            if not all(token in message for token in required):
                raise AssertionError(f"unknown-origin fixture failure omitted representative lineage details: {message}") from exc
        else:
            raise AssertionError("unknown-origin fixture did not fail explicitly")
    finally:
        shutil.rmtree(tmp)


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
            _fixture_assert_unknown_origin_failure(Path(args.output_dir))
        finally:
            shutil.rmtree(tmp)
    else:
        run(RegimeArtifactStore(args.artifact_root), Path(args.output_dir))
    print("[laus_ma6_immutable_acceptance] OK")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
