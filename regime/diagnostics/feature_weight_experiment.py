"""Diagnostic-only, registry-driven level-biased feature-weight experiment.

This module deliberately stops at evidence construction.  It neither writes a
registry nor selects/promotes a policy.  Production engine outputs can be
passed to :func:`validate_unaffected_parity` and ``summarize_propagation`` so
the experiment remains a causal splice rather than an alternate engine.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping
import hashlib
import html
import json
import time
import zipfile

import numpy as np
import pandas as pd

CONTRACT_VERSION = "ma12_structural_feature_weight_experiment_v1"
TARGET_METRICS = (
    "active_inventory", "permit_activity", "permit_intensity",
    "median_sale_price", "median_ppsf", "price_to_income", "payment_burden",
)
REVIEW_GEOGRAPHIES = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
FEATURE_TYPES = MappingProxyType({
    "level": "level", "short_term_change": "short", "long_term_change": "long",
})
ALTERNATIVES = MappingProxyType({
    "alternative_a": MappingProxyType({"level": .50, "short": .25, "long": .25}),
    "alternative_b": MappingProxyType({"level": .45, "short": .25, "long": .30}),
    "alternative_c": MappingProxyType({"level": .60, "short": .20, "long": .20}),
})
POLICY_ORDER = ("incumbent", "ma12_incumbent", "alternative_a", "alternative_b", "alternative_c")
POLICY_COLORS = MappingProxyType({"incumbent": "#172554", "ma12_incumbent": "#059669", "alternative_a": "#2563eb",
                                  "alternative_b": "#d97706", "alternative_c": "#7c3aed"})
COMPONENT_COLORS = MappingProxyType({"level": "#166534", "short": "#dc2626", "long": "#0891b2"})


@dataclass(frozen=True, slots=True)
class FeatureWeightEvidence:
    contract_version: str
    tables: Mapping[str, pd.DataFrame]


def audit_feature_registry(feature_registry: pd.DataFrame,
                           metric_registry: pd.DataFrame,
                           source_registry: pd.DataFrame) -> pd.DataFrame:
    """Resolve the three governed features for each target; fail closed."""
    required_f = {"feature_key", "metric_key", "feature_type", "feature_weight"}
    required_m = {"metric_key", "canonical_metric_key", "enabled"}
    source_column = "source_id" if "source_id" in source_registry else "source"
    geography_column = "geo_levels" if "geo_levels" in source_registry else "geography_levels"
    required_s = {"metric_key", source_column, geography_column}
    for name, frame, required in (("feature", feature_registry, required_f),
                                  ("metric", metric_registry, required_m),
                                  ("source", source_registry, required_s)):
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(f"{name} registry missing columns: {sorted(missing)}")
    metrics = metric_registry[metric_registry.canonical_metric_key.isin(TARGET_METRICS)].copy()
    if set(metrics.canonical_metric_key) != set(TARGET_METRICS) or metrics.canonical_metric_key.duplicated().any():
        raise ValueError("Target metric ownership is missing, duplicated, or ambiguous")
    enabled = metrics.enabled.astype(str).str.lower().isin(("true", "1", "yes"))
    if not enabled.all():
        raise ValueError("A target metric has unexpected disabled status")
    source = source_registry[source_registry.metric_key.isin(metrics.metric_key)].copy()
    if set(source.metric_key) != set(metrics.metric_key) or source.metric_key.duplicated().any():
        raise ValueError("Target source identity is missing, duplicated, or ambiguous")
    joined = metrics[["metric_key", "canonical_metric_key"]].merge(
        source[["metric_key", source_column, geography_column]], on="metric_key", validate="one_to_one")
    rows: list[dict[str, object]] = []
    for parent in joined.itertuples(index=False):
        family = feature_registry[feature_registry.metric_key.eq(parent.metric_key)].copy()
        family["governed_feature_type"] = family.feature_type.map(FEATURE_TYPES)
        family = family[family.governed_feature_type.notna()]
        if len(family) != 3 or family.governed_feature_type.duplicated().any() or set(family.governed_feature_type) != {"level", "short", "long"}:
            raise ValueError(f"{parent.canonical_metric_key} must own exactly one level, short, and long feature")
        weights = pd.to_numeric(family.feature_weight, errors="coerce")
        if weights.isna().any() or not np.isclose(weights.sum(), 1.0):
            raise ValueError(f"{parent.canonical_metric_key} incumbent weights must sum to 1.0")
        for item, weight in zip(family.itertuples(index=False), weights):
            rows.append({"metric": parent.canonical_metric_key, "feature_key": item.feature_key,
                         "feature_type": item.governed_feature_type, "current_weight": float(weight),
                         "enabled_status": True, "source_identity": getattr(parent, source_column),
                         "source_geography": getattr(parent, geography_column),
                         "registry_metric_key": parent.metric_key,
                         "inclusion_reason": "exact governed target feature"})
    return pd.DataFrame(rows).sort_values(["metric", "feature_type"], kind="mergesort").reset_index(drop=True)


def build_policy_registry(audit: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for metric in TARGET_METRICS:
        current = audit[audit.metric.eq(metric)].set_index("feature_type").current_weight.to_dict()
        for policy in POLICY_ORDER:
            weights = current if policy in {"incumbent", "ma12_incumbent"} else ALTERNATIVES[policy]
            definition = "incumbent" if policy == "incumbent" else "ma12_structural"
            rows.append({"policy_id": f"{metric}__{policy}", "metric": metric,
                         "policy": policy, "status": "incumbent" if policy == "incumbent" else "challenger",
                         "feature_definition": definition,
                         **{f"{kind}_weight": float(weights[kind]) for kind in ("level", "short", "long")},
                         "total_weight": float(sum(weights.values())),
                         "registry_lineage": "config/feature_registry.csv",
                         "diagnostic_only": True, "recommendation": "none", "promotion": "none"})
    result = pd.DataFrame(rows)
    if len(result) != 35 or not np.allclose(result.total_weight, 1.0):
        raise AssertionError("Exactly five unit-weight definition/weight policies are required per target metric")
    return result


def decompose_feature_scores(features: pd.DataFrame, audit: pd.DataFrame,
                             policies: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply weights to already production-normalized incumbent/MA12 features."""
    required = {"geo_id", "date", "feature_key", "feature_score"}
    missing = required.difference(features.columns)
    if missing: raise ValueError(f"Feature scores missing columns: {sorted(missing)}")
    if features.duplicated(["feature_definition", "geo_id", "date", "feature_key"]).any():
        raise ValueError("Duplicate governed normalized feature rows")
    if not set(features.geo_id).issubset(REVIEW_GEOGRAPHIES):
        raise ValueError("Feature evidence contains a non-governed review geography")
    source = features.merge(audit[["metric", "feature_key", "feature_type"]], on="feature_key", validate="many_to_one")
    expected = len(features)
    if len(source) != expected: raise ValueError("Feature evidence includes unresolved feature identity")
    rows = []
    chronology = []
    for policy in policies.itertuples(index=False):
        part = source[(source.metric.eq(policy.metric)) &
                      (source.feature_definition.eq(policy.feature_definition))].copy()
        weight_map = {kind: getattr(policy, f"{kind}_weight") for kind in ("level", "short", "long")}
        part["configured_weight"] = part.feature_type.map(weight_map)
        available = part.feature_score.notna()
        totals = part.configured_weight.where(available, 0).groupby([part.geo_id, part.date]).transform("sum")
        part["effective_weight"] = np.where(available & totals.gt(0), part.configured_weight / totals, np.nan)
        part["weighted_contribution"] = part.feature_score * part.effective_weight
        metric_score = part.groupby(["geo_id", "date"], dropna=False).weighted_contribution.transform("sum", min_count=1)
        part["recomputed_metric_score"] = metric_score
        incumbent = source[(source.metric.eq(policy.metric)) & source.feature_definition.eq("incumbent")].copy()
        incumbent_weights = audit[audit.metric.eq(policy.metric)].set_index("feature_type").current_weight
        inc_w = incumbent.feature_type.map(incumbent_weights); inc_available = incumbent.feature_score.notna()
        inc_total = inc_w.where(inc_available, 0).groupby([incumbent.geo_id, incumbent.date]).transform("sum")
        incumbent["incumbent_metric_score"] = (incumbent.feature_score * inc_w.div(inc_total).where(inc_available)).groupby([incumbent.geo_id, incumbent.date]).transform("sum", min_count=1)
        inc_scores = incumbent.drop_duplicates(["geo_id", "date"]).set_index(["geo_id", "date"]).incumbent_metric_score
        part["incumbent_metric_score"] = pd.MultiIndex.from_frame(part[["geo_id", "date"]]).map(inc_scores)
        part["signed_delta"] = part.recomputed_metric_score - part.incumbent_metric_score
        part["absolute_delta"] = part.signed_delta.abs()
        part["availability_reason"] = np.where(available, "available", "governed_feature_missing")
        residual = part.weighted_contribution.groupby([part.geo_id, part.date]).transform("sum", min_count=1) - metric_score
        part["parent_residual"] = residual
        part["pass_status"] = residual.fillna(0).abs().le(1e-12)
        part["policy_id"] = policy.policy_id; part["policy"] = policy.policy
        rows.append(part)
        chronology.append(part.drop_duplicates(["geo_id", "date"])[
            ["metric", "policy_id", "policy", "geo_id", "date", "recomputed_metric_score",
             "incumbent_metric_score", "signed_delta", "absolute_delta"]])
    return (pd.concat(rows, ignore_index=True), pd.concat(chronology, ignore_index=True))


def coverage_diagnostics(chronology: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (metric, geo), family in chronology.groupby(["metric", "geo_id"], sort=True):
        inc = set(pd.to_datetime(family[family.policy.eq("incumbent")].dropna(subset=["recomputed_metric_score"]).date))
        for policy in POLICY_ORDER[1:]:
            challenger = set(pd.to_datetime(family[family.policy.eq(policy)].dropna(subset=["recomputed_metric_score"]).date))
            overlap = inc & challenger; inc_only = sorted(inc - challenger); ch_only = challenger - inc
            leading = [d for d in inc_only if not challenger or d < min(challenger)]
            interior = [d for d in inc_only if challenger and min(challenger) < d < max(challenger)]
            trailing = [d for d in inc_only if challenger and d > max(challenger)]
            status = "pass" if not interior and not trailing and not ch_only else "fail"
            rows.append({"metric": metric, "policy": policy, "geo_id": geo,
                         "first_incumbent_date": min(inc) if inc else pd.NaT,
                         "first_challenger_date": min(challenger) if challenger else pd.NaT,
                         "overlap_count": len(overlap), "challenger_only_dates": len(ch_only),
                         "incumbent_only_dates": len(inc_only), "leading_warmup_loss": len(leading),
                         "interior_gaps": len(interior), "trailing_gaps": len(trailing),
                         "coverage_retention_ratio": len(overlap) / len(inc) if inc else np.nan,
                         "pass_status": status})
    result = pd.DataFrame(rows)
    if not result.empty and result.pass_status.eq("fail").any():
        raise ValueError("Challenger coverage has interior/trailing gaps or extra dates")
    return result


def diagnostic_tables(decomposition: pd.DataFrame, chronology: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    stability, influence, shares = [], [], []
    for keys, frame in chronology.groupby(["metric", "policy", "geo_id"], sort=True):
        metric, policy, geo = keys; frame = frame.sort_values("date")
        score = frame.recomputed_metric_score; changes = score.diff().abs().dropna()
        flips = int(((score * score.shift(1)) < 0).sum()); comparisons = max(int(score.notna().sum()) - 1, 0)
        stability.append({"metric": metric, "policy": policy, "geo_id": geo,
            "standard_deviation": score.std(), "median_absolute_mom_change": changes.median(),
            "p90_absolute_mom_change": changes.quantile(.90), "p99_absolute_mom_change": changes.quantile(.99),
            "sign_flip_count": flips, "sign_flip_rate": flips/comparisons if comparisons else np.nan,
            "correlation_with_incumbent": score.corr(frame.incumbent_metric_score),
            "mean_absolute_delta": frame.absolute_delta.mean(), "maximum_absolute_delta": frame.absolute_delta.max()})
        comp = decomposition[(decomposition.metric.eq(metric)) & (decomposition.policy.eq(policy)) & (decomposition.geo_id.eq(geo))]
        vol = comp.groupby("feature_type").weighted_contribution.std().to_dict()
        stability[-1].update({f"{k}_contribution_volatility": vol.get(k, np.nan) for k in ("level", "short", "long")})
        wide = comp.pivot(index="date", columns="feature_type", values="feature_score")
        metric_series = comp.drop_duplicates("date").set_index("date").recomputed_metric_score
        abs_contrib = comp.groupby("feature_type").weighted_contribution.apply(lambda x: x.abs().sum())
        denominator = abs_contrib.sum()
        share = {k: abs_contrib.get(k, 0)/denominator if denominator else np.nan for k in ("level", "short", "long")}
        shares.append({"metric": metric, "policy": policy, "geo_id": geo, **{f"{k}_absolute_contribution_share": v for k,v in share.items()}})
        inc = comp.drop_duplicates("date").set_index("date").incumbent_metric_score
        distance = (metric_series-wide.level).abs(); inc_distance = (inc-wide.level).abs()
        material = 1e-9
        influence.append({"metric": metric, "policy": policy, "geo_id": geo,
            "metric_level_correlation": metric_series.corr(wide.level), **shares[-1],
            "different_sign_dates": int(((metric_series*inc)<0).sum()),
            "materially_closer_to_level_dates": int((distance < inc_distance-material).sum()),
            "materially_farther_from_level_dates": int((distance > inc_distance+material).sum()),
            "interpretation": "descriptive_only_closer_is_not_automatically_better"})
    return pd.DataFrame(stability), pd.DataFrame(influence), pd.DataFrame(shares)


def validate_unaffected_parity(incumbent: pd.DataFrame, challenger: pd.DataFrame,
                               keys: list[str], *, artifact: str) -> pd.DataFrame:
    """Exact, duplicate-safe, schema- and null-safe parity for a preserved artifact."""
    if list(incumbent.columns) != list(challenger.columns):
        raise ValueError(f"{artifact}: schema mismatch")
    if incumbent.duplicated(keys).any() or challenger.duplicated(keys).any():
        raise ValueError(f"{artifact}: duplicate parity keys")
    left = incumbent.sort_values(keys, kind="mergesort").reset_index(drop=True)
    right = challenger.sort_values(keys, kind="mergesort").reset_index(drop=True)
    try: pd.testing.assert_frame_equal(left, right, check_exact=True, check_dtype=True)
    except AssertionError as exc: raise ValueError(f"{artifact}: unaffected parity failure: {exc}") from exc
    return pd.DataFrame([{"artifact": artifact, "incumbent_rows": len(left), "challenger_rows": len(right),
                          "duplicate_rows": 0, "missing_rows": 0, "extra_rows": 0,
                          "schema_match": True, "null_safe_exact_match": True, "pass_status": "pass"}])


def summarize_propagation(incumbent: pd.DataFrame, challenger: pd.DataFrame,
                          keys: list[str], value: str, *, artifact: str) -> pd.DataFrame:
    merged = incumbent[keys+[value]].merge(challenger[keys+[value]], on=keys, suffixes=("_incumbent", "_challenger"), validate="one_to_one")
    if len(merged) != len(incumbent) or len(merged) != len(challenger): raise ValueError(f"{artifact}: propagation key mismatch")
    merged["delta"] = merged[f"{value}_challenger"] - merged[f"{value}_incumbent"]
    return pd.DataFrame([{"artifact": artifact, "mean_absolute_delta": merged.delta.abs().mean(),
        "maximum_absolute_delta": merged.delta.abs().max(), "changed_count": int(merged.delta.ne(0).sum()),
        "changed_share": merged.delta.ne(0).mean(), "row_count": len(merged)}])


def build_evidence(features: pd.DataFrame, feature_registry: pd.DataFrame,
                   metric_registry: pd.DataFrame, source_registry: pd.DataFrame) -> FeatureWeightEvidence:
    started = time.perf_counter(); audit = audit_feature_registry(feature_registry, metric_registry, source_registry)
    print(f"[feature-weight] registry audit: {time.perf_counter()-started:.3f}s")
    if "feature_definition" not in features:
        features = pd.concat([features.assign(feature_definition="incumbent"),
                              features.assign(feature_definition="ma12_structural")], ignore_index=True)
    policies = build_policy_registry(audit)
    decomposition, chronology = decompose_feature_scores(features, audit, policies)
    coverage = coverage_diagnostics(chronology)
    stability, influence, shares = diagnostic_tables(decomposition, chronology)
    trend = stability[["metric", "policy", "geo_id", "correlation_with_incumbent",
                       "sign_flip_count", "sign_flip_rate", "mean_absolute_delta",
                       "maximum_absolute_delta"]].copy()
    trend["interpretation"] = "descriptive_trend_preservation_only"
    decision = pd.DataFrame([{"contract_version": CONTRACT_VERSION, "decision": "pending_human_review",
                              "recommendation": "none", "promotion": "none", "diagnostic_only": True}])
    tables = {"registry_audit": audit, "policy_registry": policies,
              "feature_to_metric_decomposition": decomposition, "metric_chronology_comparison": chronology,
              "feature_contribution_shares": shares, "stability_diagnostics": stability,
              "trend_preservation_diagnostics": trend,
              "level_influence_diagnostics": influence, "coverage_and_warmup": coverage,
              "human_decision_status": decision}
    return FeatureWeightEvidence(CONTRACT_VERSION, MappingProxyType(tables))


def _svg_lines(frame: pd.DataFrame, series_column: str, value_column: str,
               colors: Mapping[str, str], widths: Mapping[str, float], title: str) -> str:
    width,height=900,320; left,right,top,bottom=65,875,35,275
    work=frame.dropna(subset=[value_column]).copy(); work["date"]=pd.to_datetime(work.date)
    if work.empty: raise ValueError(f"Cannot render empty figure: {title}")
    start,end=work.date.min(),work.date.max(); span=max((end-start).total_seconds(),1)
    low=min(-1.,float(work[value_column].min())); high=max(1.,float(work[value_column].max()))
    def point(date: pd.Timestamp,value: float) -> tuple[float,float]:
        return left+(date-start).total_seconds()/span*(right-left), bottom-(value-low)/(high-low)*(bottom-top)
    paths=[]
    for name,group in work.groupby(series_column,sort=False):
        previous=None; commands=[]
        for row in group.sort_values("date").itertuples(index=False):
            date=pd.Timestamp(row.date); x,y=point(date,float(getattr(row,value_column)))
            gap=previous is None or (date.year*12+date.month)-(previous.year*12+previous.month)>1
            commands.append(("M" if gap else "L")+f" {x:.2f} {y:.2f}"); previous=date
        paths.append(f"<path d='{' '.join(commands)}' fill='none' stroke='{colors[str(name)]}' stroke-width='{widths[str(name)]}'/>")
    zero=point(start,0)[1]
    legend="".join(f"<text x='{80+i*190}' y='20' fill='{colors[str(name)]}'>{html.escape(str(name))}</text>" for i,name in enumerate(work[series_column].drop_duplicates()))
    return (f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' role='img'><title>{html.escape(title)}</title>"
            f"<rect width='100%' height='100%' fill='white'/><line x1='{left}' y1='{zero:.2f}' x2='{right}' y2='{zero:.2f}' stroke='#94a3b8'/>"
            f"<line x1='{left}' y1='{top}' x2='{left}' y2='{bottom}' stroke='#334155'/><line x1='{left}' y1='{bottom}' x2='{right}' y2='{bottom}' stroke='#334155'/>"
            +"".join(paths)+legend+"</svg>")


def _write_figures(evidence: FeatureWeightEvidence, output: Path) -> Mapping[str, list[str]]:
    figures=output/"figures"; figures.mkdir(exist_ok=True); result={}
    chronology=evidence.tables["metric_chronology_comparison"]
    decomposition=evidence.tables["feature_to_metric_decomposition"]
    for metric in TARGET_METRICS:
        paths=[]; chrono=chronology[chronology.metric.eq(metric)].groupby(["policy","date"],as_index=False,sort=False).recomputed_metric_score.mean()
        path=figures/f"{metric}__chronology.svg"; path.write_text(_svg_lines(chrono,"policy","recomputed_metric_score",POLICY_COLORS,
            {p:2.8 if p=="incumbent" else 1.8 for p in POLICY_ORDER},f"{metric} policy chronology"),encoding="utf-8",newline="\n"); paths.append(path.name)
        for policy in POLICY_ORDER:
            contrib=decomposition[(decomposition.metric.eq(metric)) & (decomposition.policy.eq(policy))].groupby(["feature_type","date"],as_index=False,sort=False).weighted_contribution.mean()
            path=figures/f"{metric}__{policy}__contributions.svg"; path.write_text(_svg_lines(contrib,"feature_type","weighted_contribution",COMPONENT_COLORS,
                {k:1.9 for k in COMPONENT_COLORS},f"{metric} {policy} feature contributions"),encoding="utf-8",newline="\n"); paths.append(path.name)
        result[metric]=paths
    return result


def write_review_bundle(evidence: FeatureWeightEvidence, output: Path,
                        extra_tables: Mapping[str, pd.DataFrame] | None = None,
                        manifest_metadata: Mapping[str, object] | None = None) -> tuple[Path, Path, int]:
    started = time.perf_counter(); output.mkdir(parents=True, exist_ok=True)
    tables = dict(evidence.tables); tables.update(extra_tables or {})
    required_extra = ("downstream_dimension_propagation", "downstream_axis_propagation",
                      "coordinate_regime_changes", "unaffected_parity")
    for name in required_extra: tables.setdefault(name, pd.DataFrame([{"status": "not_supplied_fail_closed"}]))
    artifacts = output / "artifacts"; artifacts.mkdir(exist_ok=True)
    for name, table in sorted(tables.items()): table.to_csv(artifacts/f"{name}.csv", index=False, lineterminator="\n", date_format="%Y-%m-%d")
    figure_started=time.perf_counter(); figure_paths=_write_figures(evidence,output)
    print(f"[feature-weight] figure generation: {time.perf_counter()-figure_started:.3f}s")
    html_started=time.perf_counter()
    labels = {m: m.replace("_", " ").title() for m in TARGET_METRICS}
    sections = ["<h2>Executive Summary</h2><p>Diagnostic comparison only. No recommendation or promotion.</p>",
                "<h2>Registry and Current Weights</h2>", "<h2>Experiment Policies</h2>"]
    for metric in TARGET_METRICS:
        images="".join(f"<figure><img src='figures/{name}' alt='{html.escape(metric)} diagnostic'><figcaption>{html.escape(name.removesuffix('.svg'))}</figcaption></figure>" for name in figure_paths[metric])
        sections.append(f"<h2>{html.escape(labels[metric])}</h2><p>Incumbent, MA12 Incumbent, Alternative A, Alternative B, and Alternative C chronology and separate feature-contribution panels. Real timestamps and missing observations are retained; no interpolation is used.</p>{images}")
    sections += ["<h2>Cross-Metric Comparison</h2>", "<h2>Downstream Propagation</h2>",
                 "<h2>Coverage and Warmup</h2>", "<h2>Supporting Artifacts</h2><ul>" + "".join(f"<li><a href='artifacts/{n}.csv'>{n}</a></li>" for n in sorted(tables)) + "</ul>",
                 "<h2>Human Decision Status</h2><p>Pending. Closer to level is not automatically better.</p>"]
    review = output/"index.html"; review.write_text("<!doctype html><html><head><meta charset='utf-8'><title>Level-Biased Feature-Weight Experiment</title><style>body{font-family:sans-serif;max-width:1100px;margin:auto}h2{border-bottom:1px solid #ccc}</style></head><body><h1>Level-Biased Feature-Weight Experiment</h1>"+"".join(sections)+"</body></html>", encoding="utf-8", newline="\n")
    print(f"[feature-weight] HTML assembly: {time.perf_counter()-html_started:.3f}s")
    files = sorted(p for p in output.rglob("*") if p.is_file() and p.name != "manifest.json")
    manifest = {"contract_version": CONTRACT_VERSION, "contract_identity": CONTRACT_VERSION,
                "authoritative_input_identity": None, "metric_count": len(TARGET_METRICS),
                "challenger_count": len(TARGET_METRICS) * len(ALTERNATIVES),
                "diagnostic_only": True, "recommendation": "none", "promotion": "none",
                "recommendation_state": "none", "promotion_state": "none",
                "files": [{"path": p.relative_to(output).as_posix(), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in files]}
    manifest.update(dict(manifest_metadata or {}))
    (output/"manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2)+"\n", encoding="utf-8")
    zip_started=time.perf_counter()
    archive = output.with_suffix(".zip")
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
        for p in sorted(q for q in output.rglob("*") if q.is_file()):
            info=zipfile.ZipInfo(p.relative_to(output).as_posix(), (1980,1,1,0,0,0)); info.compress_type=zipfile.ZIP_DEFLATED; z.writestr(info,p.read_bytes())
    print(f"[feature-weight] ZIP creation: {time.perf_counter()-zip_started:.3f}s")
    count = sum(1 for p in output.rglob("*") if p.is_file())
    if count >= 300: raise ValueError("Review bundle exceeds governed 300-file limit")
    print(f"[feature-weight] HTML/ZIP: {time.perf_counter()-started:.3f}s; files={count}; zip_bytes={archive.stat().st_size}")
    return review, archive, count
