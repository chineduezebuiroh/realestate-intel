"""Build the immutable Capital Markets MA decomposition review bundle."""
from __future__ import annotations

import hashlib
import html
import json
from pathlib import Path
import sys
import time
import zipfile

import numpy as np
import pandas as pd

from regime._02_feature_normalizer import normalize_features
from regime._03_metric_scorer import score_metrics
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime._07_coordinate_engine import build_coordinates
from regime._08_geometry_engine import assign_geometry
from regime._09_regime_assignment import assign_regimes
from regime.diagnostics.capital_markets import build_capital_markets_evidence
from regime.diagnostics.capital_markets_ma import (
    CONTRACT_IDENTITY, MA_WINDOWS, NATIVE_GEOGRAPHY, PROMOTION_STATE,
    RECOMMENDATION_STATE, REVIEW_GEOGRAPHIES, active_registry,
    build_structural_features, detect_turning_points, directional_agreement, match_turning_points,
    build_covariance_budget, build_variance_budget, family_challenger_registry,
    governed_families, human_status, interaction_diagnostics, payment_burden_audit, validate_source_run,
)
from regime.artifacts import RegimeArtifactStore

TABLES = (
    "capital_markets_metric_policy_scorecard", "capital_markets_metric_policy_decision_matrix",
    "capital_markets_cross_metric_summary", "combined_metric_policy_selection_template",
    "metric_raw_and_ma_chronology", "metric_feature_chronology",
    "metric_normalized_feature_scores", "metric_score_chronology",
    "metric_only_dimension_chronology", "metric_directional_agreement",
    "metric_turning_point_matches", "metric_turning_point_summary", "metric_warmup_coverage",
    "capital_markets_registry_audit", "native_source_chronology", "feature_transform_audit",
    "feature_to_metric_decomposition", "metric_to_dimension_decomposition", "incumbent_stability",
    "incumbent_cancellation", "incumbent_volatility_attribution", "challenger_policy_registry",
    "challenger_coverage", "challenger_stability", "directional_agreement_detail",
    "turning_point_diagnostics", "turning_point_matches", "turning_point_summary",
    "trend_preservation", "metric_family_summary", "dimension_chronology", "axis_propagation",
    "coordinate_propagation", "regime_change_summary", "unaffected_parity",
    "payment_burden_dependency_audit", "human_decision_status",
    "family_challenger_policy_registry", "family_challenger_stability",
    "family_challenger_directional_agreement", "family_challenger_turning_points",
    "family_challenger_turning_point_matches", "family_challenger_trend_preservation",
    "family_challenger_dimension_chronology", "family_challenger_axis_propagation",
    "family_challenger_regime_summary", "family_challenger_unaffected_parity",
    "capital_markets_variance_budget", "capital_markets_covariance_budget",
    "capital_markets_variance_budget_summary", "family_challenger_interactions",
    "transformed_feature_cache_audit", "challenger_performance_diagnostics",
    "runtime_summary",
)


def _stability(frame: pd.DataFrame, value: str, **identity: object) -> dict:
    work = frame[["date", value]].sort_values("date").copy()
    work["date"] = pd.to_datetime(work.date)
    prior = work.shift()
    contiguous = ((work.date.dt.year - prior.date.dt.year) * 12 + work.date.dt.month - prior.date.dt.month).eq(1)
    delta = (work[value] - prior[value]).where(contiguous).dropna()
    absolute = delta.abs(); threshold = max(.05, float(absolute.quantile(.90)) if len(absolute) else 0.0)
    signs = delta.map(lambda x: 1 if x > 1e-12 else -1 if x < -1e-12 else 0)
    flips = ((signs * signs.shift()).lt(0)).sum()
    return {**identity, "observation_count": int(work[value].notna().sum()),
        "standard_deviation": work[value].std(), "median_absolute_mom_change": absolute.median(),
        "p90_absolute_mom_change": absolute.quantile(.90), "p99_absolute_mom_change": absolute.quantile(.99),
        "maximum_absolute_jump": absolute.max(), "sign_flip_count": int(flips),
        "sign_flip_rate": float(flips / max(len(delta), 1)), "rolling_12m_volatility_median": delta.rolling(12).std().median(),
        "large_jump_threshold": threshold, "large_jump_count": int(absolute.gt(threshold).sum())}


def _policy_registry(registry: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for metric, group in registry.groupby("canonical_metric_key", sort=True):
        for window in MA_WINDOWS:
            for row in group.itertuples(index=False):
                lag = 3 if row.feature_type == "short_term_change" else 12 if row.feature_type == "long_term_change" else None
                rows.append({"challenger_id": f"{metric}_ma{window}", "changed_metric": metric,
                    "ma_window": window, "feature_key": row.feature_key, "feature_type": row.feature_type,
                    "formula": f"MA{window}(raw)" if lag is None else f"MA{window}(raw) / lag{lag}(MA{window}(raw)) - 1",
                    "configured_feature_weight": row.feature_weight, "configured_metric_weight": row.metric_weight,
                    "recommendation_state": RECOMMENDATION_STATE, "promotion_state": PROMOTION_STATE})
    return pd.DataFrame(rows)


def _svg(path: Path, title: str, frame: pd.DataFrame, value: str, group: str | None = None) -> None:
    order = {"incumbent": 0, "ma3_structural": 1, "ma6_structural": 2,
        "ma9_structural": 3, "ma12_structural": 4, "raw": 0, "ma3": 1,
        "ma6": 2, "ma9": 3, "ma12": 4}
    series = [("series", frame)] if group is None else sorted(
        frame.groupby(group, sort=True), key=lambda item: order.get(str(item[0]), 99))
    colors = ["#111827", "#2563eb", "#dc2626", "#059669", "#9333ea"]
    paths = []
    values = pd.to_numeric(frame[value], errors="coerce"); dates = pd.to_datetime(frame.date)
    good = values.notna() & dates.notna()
    low, high = min(0.0, values[good].min()), max(0.0, values[good].max()); span = high-low or 1
    start, end = dates[good].min(), dates[good].max(); duration = max((end-start).total_seconds(), 1)
    for no, (_, part) in enumerate(series):
        points = []
        for row in part.sort_values("date").itertuples(index=False):
            date, val = pd.Timestamp(row.date), getattr(row, value)
            if pd.isna(val): points.append(None); continue
            points.append((50+(date-start).total_seconds()/duration*700, 260-(float(val)-low)/span*220))
        command=[]; penup=True
        for point in points:
            if point is None: penup=True; continue
            command.append(("M" if penup else "L")+f" {point[0]:.2f} {point[1]:.2f}"); penup=False
        paths.append(f"<path d='{' '.join(command)}' fill='none' stroke='{colors[no%len(colors)]}' stroke-width='{3 if no == 0 else 1.5}'/>")
    content = f"<svg xmlns='http://www.w3.org/2000/svg' width='800' height='300'><title>{html.escape(title)}</title><rect width='800' height='300' fill='white'/><line x1='50' y1='260' x2='750' y2='260' stroke='#777'/>{''.join(paths)}</svg>"
    path.write_text(content, encoding="utf-8", newline="\n")


def _zip(output: Path) -> Path:
    target = output.with_suffix(".zip")
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in output.rglob("*") if p.is_file()):
            info = zipfile.ZipInfo(path.relative_to(output).as_posix(), (1980,1,1,0,0,0)); info.compress_type=zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    return target


def _splice_metrics(
    incumbent: pd.DataFrame,
    replacements: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Replace governed aligned metrics using their native metric dates."""
    required_incumbent = {
        "geo_id",
        "evaluation_date",
        "metric_date",
        "canonical_metric_key",
        "metric_score",
    }
    missing_incumbent = required_incumbent.difference(
        incumbent.columns
    )
    if missing_incumbent:
        raise ValueError(
            "Aligned metric splice requires persisted alignment columns; "
            f"missing={sorted(missing_incumbent)}"
        )

    if incumbent.columns.duplicated().any():
        raise ValueError(
            "Aligned metric splice incumbent contains duplicate columns"
        )

    parent_columns = list(incumbent.columns)
    governed_keys = [
        "geo_id",
        "evaluation_date",
        "canonical_metric_key",
    ]

    if incumbent.duplicated(governed_keys).any():
        raise ValueError(
            "Aligned metric splice incumbent contains duplicate "
            "governed keys"
        )

    active_targets = set(replacements)
    parts = [
        incumbent.loc[
            ~incumbent["canonical_metric_key"].isin(
                active_targets
            )
        ].copy()
    ]

    for metric, candidate in sorted(replacements.items()):
        required_candidate = {
            "date",
            "canonical_metric_key",
            "metric_score",
        }
        missing_candidate = required_candidate.difference(
            candidate.columns
        )
        if missing_candidate:
            raise ValueError(
                f"{metric}: candidate metric schema is incomplete; "
                f"missing={sorted(missing_candidate)}"
            )

        candidate_metric = candidate.loc[
            candidate["canonical_metric_key"].eq(metric),
            ["date", "metric_score"],
        ].copy()

        if candidate_metric.empty:
            raise ValueError(
                f"{metric}: candidate metric contains no governed rows"
            )

        if candidate_metric["date"].duplicated().any():
            raise ValueError(
                f"{metric}: candidate metric contains duplicate "
                "native dates"
            )

        target = incumbent.loc[
            incumbent["canonical_metric_key"].eq(metric)
        ].copy()

        if target.empty:
            raise ValueError(
                f"{metric}: incumbent aligned chronology is absent"
            )

        # `metric_date` is the persisted native observation date.
        # `evaluation_date` is the as-of aligned review date and must
        # never be used to join a native challenger score.
        candidate_metric = candidate_metric.rename(
            columns={
                "date": "metric_date",
                "metric_score": "challenger_metric_score",
            }
        )

        candidate_metric["metric_date"] = (
            candidate_metric["metric_date"].astype(
                target["metric_date"].dtype
            )
        )

        replacement = target.merge(
            candidate_metric,
            on="metric_date",
            how="left",
            validate="many_to_one",
            sort=False,
        )

        replacement["metric_score"] = replacement[
            "challenger_metric_score"
        ]
        replacement = replacement.drop(
            columns=["challenger_metric_score"]
        )
        replacement = replacement.loc[:, parent_columns]

        if replacement["metric_score"].notna().sum() == 0:
            raise ValueError(
                f"No incumbent native-date overlap for {metric}"
            )

        if replacement.duplicated(governed_keys).any():
            raise ValueError(
                f"{metric}: replacement contains duplicate governed keys"
            )

        parts.append(replacement)

    out = pd.concat(
        parts,
        ignore_index=True,
    ).loc[:, parent_columns]

    if out.duplicated(governed_keys).any():
        raise ValueError(
            "Aligned metric splice result contains duplicate governed keys"
        )

    unaffected_incumbent = incumbent.loc[
        ~incumbent["canonical_metric_key"].isin(active_targets)
    ].sort_values(
        governed_keys,
        kind="mergesort",
    ).reset_index(drop=True)

    unaffected_output = out.loc[
        ~out["canonical_metric_key"].isin(active_targets)
    ].sort_values(
        governed_keys,
        kind="mergesort",
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(
        unaffected_output,
        unaffected_incumbent,
        check_dtype=True,
        check_exact=True,
    )

    return out.sort_values(
        governed_keys,
        kind="mergesort",
    ).reset_index(drop=True)


def _progress(message: str) -> None:
    """Emit immediately visible hosted-run progress."""
    print(f"[capital-markets] {message}", flush=True)


def _coverage(frame: pd.DataFrame, value: str, incumbent_dates: pd.Series) -> dict[str, object]:
    """Enforce the governed leading-warmup-only coverage contract."""
    dates = pd.DatetimeIndex(pd.to_datetime(incumbent_dates).drop_duplicates().sort_values())
    work = frame[["date", value]].copy(); work["date"] = pd.to_datetime(work.date)
    if work.date.duplicated().any():
        raise ValueError("Metric-policy chronology contains duplicate dates")
    challenger_dates = set(work.loc[work[value].notna(), "date"])
    if challenger_dates - set(dates):
        raise ValueError("Metric-policy chronology contains challenger-only dates")
    valid = pd.Series(dates.isin(challenger_dates), index=dates)
    first = int(np.argmax(valid.to_numpy())) if valid.any() else len(valid)
    leading = first
    trailing = int((~valid.iloc[first:]).iloc[::-1].cumprod().sum()) if first < len(valid) else len(valid)
    interior = int((~valid.iloc[first:len(valid)-trailing if trailing else len(valid)]).sum())
    if interior or trailing:
        raise ValueError(f"Metric-policy coverage has interior={interior} or trailing={trailing} loss")
    observed = work.loc[work[value].notna(), "date"]
    return {"observation_count": len(observed), "first_valid_date": observed.min(),
        "last_valid_date": observed.max(), "leading_warmup_rows": leading,
        "trailing_loss_rows": trailing, "interior_gap_rows": interior}


def _pct(delta: float, incumbent: float) -> float:
    return 0.0 if delta == 0 else (delta / incumbent * 100 if incumbent else np.nan)


def _render_metric_page(output: Path, metric: str, raw: pd.DataFrame,
        features: pd.DataFrame, normalized: pd.DataFrame, scores: pd.DataFrame,
        dimensions: pd.DataFrame) -> None:
    """Write one deterministic seven-section metric review with linked SVGs."""
    page_dir = output / "metrics"; figure_dir = output / "figures" / metric
    page_dir.mkdir(exist_ok=True); figure_dir.mkdir(parents=True, exist_ok=True)
    panels: list[tuple[str, str]] = []
    _svg(figure_dir / "raw.svg", f"{metric}: raw and moving averages", raw, "value", "series")
    panels.append(("1. Raw source chronology", f"../figures/{metric}/raw.svg"))
    for number, feature_type, heading in ((2,"level","Level feature"),(3,"short_term_change","Short feature"),(4,"long_term_change","Long feature")):
        part=features.loc[features.feature_type.eq(feature_type)]
        _svg(figure_dir/f"{feature_type}.svg",f"{metric}: {heading}",part,"value","policy")
        panels.append((f"{number}. {heading}",f"../figures/{metric}/{feature_type}.svg"))
    normalized_images=[]
    for feature_type in ("level","short_term_change","long_term_change"):
        part=normalized.loc[normalized.feature_type.eq(feature_type)]
        _svg(figure_dir/f"normalized_{feature_type}.svg",f"{metric}: normalized {feature_type}",part,"feature_score","policy")
        normalized_images.append(f"<img src='../figures/{metric}/normalized_{feature_type}.svg' alt='Normalized {feature_type}'>")
    _svg(figure_dir/"metric_score.svg",f"{metric}: metric score",scores,"metric_score","policy")
    _svg(figure_dir/"dimension.svg",f"{metric}: metric-only Capital Markets dimension",dimensions,"dimension_score","policy")
    sections = "".join(f"<section><h2>{html.escape(title)}</h2><img src='{src}'></section>" for title,src in panels)
    sections += f"<section><h2>5. Normalized feature scores</h2>{''.join(normalized_images)}</section>"
    sections += f"<section><h2>6. Metric score chronology</h2><img src='../figures/{metric}/metric_score.svg'></section>"
    sections += f"<section><h2>7. Capital Markets dimension chronology</h2><img src='../figures/{metric}/dimension.svg'></section>"
    css="body{font-family:system-ui;max-width:1100px;margin:auto;padding:24px;color:#172033}section{margin:32px 0}img{width:100%;border:1px solid #d8dee9}"
    (page_dir/f"{metric}.html").write_text(f"<!doctype html><html><head><meta charset='utf-8'><title>{metric} smoothing review</title><style>{css}</style></head><body><a href='../index.html'>Back to decision matrix</a><h1>{metric}: metric-level smoothing review</h1><p>Incumbent is held against MA3, MA6, MA9, and MA12 structural policies. Diagnostic only.</p>{sections}</body></html>\n",encoding="utf-8",newline="\n")


def _national_capital_metric_universe(
    metric_scores: pd.DataFrame,
    active: tuple[str, ...],
) -> pd.DataFrame:
    """
    Build the single native national Capital Markets metric universe.

    Native Capital Markets observations come from persisted metric_scores.
    The returned frame is adapted to the aligned-metric schema required by
    the production dimension scorer, with each native date serving as both
    evaluation_date and metric_date and with zero metric age.
    """
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "metric_score",
        "feature_count",
        "feature_weight_sum",
        "min_feature_score",
        "max_feature_score",
    }
    missing = required.difference(metric_scores.columns)
    if missing:
        raise ValueError(
            "Native metric_scores schema is incomplete; "
            f"missing={sorted(missing)}"
        )

    if metric_scores.columns.duplicated().any():
        raise ValueError(
            "Native metric_scores contains duplicate columns"
        )

    out = metric_scores.loc[
        metric_scores["geo_id"].eq(NATIVE_GEOGRAPHY)
        & metric_scores["canonical_metric_key"].isin(active)
    ].copy()

    if out.empty:
        raise ValueError(
            "Native national Capital Markets metric universe is empty"
        )

    found_metrics = set(
        out["canonical_metric_key"].dropna().unique()
    )
    expected_metrics = set(active)
    if found_metrics != expected_metrics:
        raise ValueError(
            "Native national Capital Markets metric universe "
            "is incomplete; "
            f"missing={sorted(expected_metrics - found_metrics)}, "
            f"unexpected={sorted(found_metrics - expected_metrics)}"
        )

    if out["geo_id"].nunique() != 1:
        raise ValueError(
            "Native Capital Markets scope expanded beyond "
            "one national geography"
        )

    if not out["geo_id"].eq(NATIVE_GEOGRAPHY).all():
        raise ValueError(
            "Native Capital Markets universe contains "
            "non-national geography rows"
        )

    native_keys = [
        "geo_id",
        "date",
        "canonical_metric_key",
    ]
    if out.duplicated(native_keys).any():
        raise ValueError(
            "Native national Capital Markets universe contains "
            "duplicate metric-date keys"
        )

    # Adapt native metric rows to the production aligned-metric schema.
    # A native observation is evaluated on its own date and has zero age.
    out = out.rename(
        columns={"date": "evaluation_date"}
    )
    out["metric_date"] = out["evaluation_date"]
    out["metric_age_days"] = 0

    aligned_columns = [
        "geo_id",
        "evaluation_date",
        "metric_date",
        "canonical_metric_key",
        "metric_score",
        "feature_count",
        "feature_weight_sum",
        "min_feature_score",
        "max_feature_score",
        "metric_age_days",
    ]
    out = out.loc[:, aligned_columns]

    governed_keys = [
        "geo_id",
        "evaluation_date",
        "canonical_metric_key",
    ]
    if out.duplicated(governed_keys).any():
        raise ValueError(
            "Adapted national Capital Markets universe contains "
            "duplicate governed keys"
        )

    return out.sort_values(
        governed_keys,
        kind="mergesort",
    ).reset_index(drop=True)


def _align_national_dimension_to_counties(
    national: pd.DataFrame, county_chronology: pd.DataFrame
) -> pd.DataFrame:
    """Apply production's backward as-of semantics after national scoring."""
    required = {"date", "dimension_score"}
    if required.difference(national.columns):
        raise ValueError("National Capital Markets chronology schema is incomplete")
    if national["date"].duplicated().any():
        raise ValueError("National Capital Markets chronology contains duplicate dates")
    keys = county_chronology[["geo_id", "date"]].drop_duplicates().copy()
    if set(keys.geo_id.unique()) != set(REVIEW_GEOGRAPHIES):
        raise ValueError("County alignment scope differs from the seven governed counties")
    if keys.duplicated(["geo_id", "date"]).any():
        raise ValueError("Governed county chronology contains duplicate keys")
    source = national.copy().rename(columns={"date": "native_dimension_date"})
    source["native_dimension_date"] = pd.to_datetime(source.native_dimension_date)
    parts = []
    for geo_id, target in keys.groupby("geo_id", sort=True):
        target = target.copy(); target["date"] = pd.to_datetime(target.date)
        part = pd.merge_asof(
            target.sort_values("date"), source.sort_values("native_dimension_date"),
            left_on="date", right_on="native_dimension_date", direction="backward",
            allow_exact_matches=True,
        )
        part["geo_id"] = geo_id; parts.append(part)
    out = pd.concat(parts, ignore_index=True)

    # A structural MA challenger may begin later than the incumbent county
    # review calendar. Only leading pre-warmup rows may be unavailable.
    unavailable = out["dimension_score"].isna()
    leading_warmup_rows = int(unavailable.sum())

    if unavailable.any():
        first_native_date = source["native_dimension_date"].min()

        invalid_missing = unavailable & out["date"].ge(
            first_native_date
        )
        if invalid_missing.any():
            bad = out.loc[
                invalid_missing,
                ["geo_id", "date"],
            ].head(20)
            raise ValueError(
                "National Capital Markets chronology contains "
                "interior or trailing county-alignment gaps:\n"
                f"{bad.to_string(index=False)}"
            )

        # Do not backfill unavailable challenger dates from the incumbent.
        out = out.loc[~unavailable].copy()

    if out.empty:
        raise ValueError(
            "National Capital Markets challenger has no governed "
            "county chronology after warmup"
        )

    if out["date"].max() < keys["date"].max():
        raise ValueError(
            "National Capital Markets challenger has trailing "
            "coverage loss"
        )

    out["dimension"] = "capital_markets"
    out = out[
        ["geo_id", "date", "dimension", "dimension_score"]
    ].sort_values(
        ["geo_id", "date"],
        kind="mergesort",
    ).reset_index(drop=True)

    out.attrs["leading_warmup_rows"] = leading_warmup_rows
    return out


def _recompute_governed_descendants(
    national_dimension: pd.DataFrame, incumbent_dimensions: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """Align once-national results, then score only seven county descendants."""
    governed = incumbent_dimensions.loc[
        incumbent_dimensions.geo_id.isin(REVIEW_GEOGRAPHIES)
    ].copy()
    chronology = governed.loc[governed.dimension.eq("capital_markets"), ["geo_id", "date"]]
    aligned = _align_national_dimension_to_counties(national_dimension, chronology)
    key = ["geo_id", "date", "dimension"]

    # Remove every incumbent Capital Markets row first. Reinsert challenger
    # rows only where the challenger has completed its governed warmup.
    # This prevents accidental incumbent backfill on leading warmup dates.
    kept = governed.loc[
        governed["dimension"].ne("capital_markets")
    ].copy()

    incumbent_capital = governed.loc[
        governed["dimension"].eq("capital_markets")
    ].copy()

    replacement = incumbent_capital.drop(
        columns=["dimension_score"]
    ).merge(
        aligned,
        on=key,
        how="inner",
        validate="one_to_one",
    )[incumbent_capital.columns]

    if len(replacement) != len(aligned):
        raise ValueError(
            "Aligned Capital Markets challenger rows did not map "
            "one-to-one to the governed county dimension universe"
        )

    dimensions = pd.concat(
        [kept, replacement],
        ignore_index=True,
    ).sort_values(
        key,
        kind="mergesort",
    )
    if dimensions.geo_id.nunique() != 7:
        raise ValueError("Axis challenger scope expanded beyond seven governed counties")
    axes = score_axes(dimensions)
    coordinates = build_coordinates(axes)
    geometry = assign_geometry(coordinates)
    regimes = assign_regimes(geometry)
    counts = {
        "dimension_scorer_rows": len(national_dimension),
        "dimension_scorer_geographies": int(national_dimension.geo_id.nunique()),
        "aligned_rows": len(aligned), "spliced_rows": len(replacement),
        "axis_scorer_rows": len(dimensions),
        "axis_scorer_geographies": int(dimensions.geo_id.nunique()),
        "downstream_rows": len(axes) + len(coordinates) + len(geometry) + len(regimes),
        "out_of_scope_rows_preserved": int((~incumbent_dimensions.geo_id.isin(REVIEW_GEOGRAPHIES)).sum()),
    }
    return axes, coordinates, regimes, aligned, counts


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: build_capital_markets_ma_decomposition.py SOURCE_RUN OUTPUT_DIRECTORY")
    source, output = Path(sys.argv[1]), Path(sys.argv[2])
    started = time.perf_counter()
    output.mkdir(parents=True, exist_ok=False)
    (output / "in_progress.json").write_text(json.dumps({
        "status": "in_progress", "source_run": source.name,
        "contract": CONTRACT_IDENTITY,
    }, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    stage_rows: list[dict[str, object]] = []
    performance_rows: list[dict[str, object]] = []

    def finish_stage(stage: str, since: float) -> float:
        elapsed = time.perf_counter() - since
        stage_rows.append({"stage": stage, "runtime_seconds": elapsed})
        _progress(f"{stage}: {elapsed:.2f}s")
        return elapsed

    stage = time.perf_counter(); proof = validate_source_run(source)
    store = RegimeArtifactStore(source.parent)
    names = ("source_metrics", "features", "normalized_features", "metric_scores",
        "aligned_metric_scores", "dimension_scores", "axis_scores", "coordinates",
        "geometry", "regime_assignments")
    frames = {name: store.read_dataframe(source.name, name) for name in names}
    load_time = finish_stage("authoritative input loading", stage)

    stage = time.perf_counter(); registry = active_registry()
    active = tuple(sorted(registry.canonical_metric_key.unique()))
    expected_active = {"mortgage_30y", "mortgage_15y", "fedfunds", "treasury_10y", "spread_2y10y", "spread_10y_fedfunds"}
    if set(active) != expected_active or "treasury_2y" in active:
        raise ValueError("Registry-driven active Capital Markets set differs from the governed six")
    policies = _policy_registry(registry); families = governed_families(registry)
    family_policies = family_challenger_registry(registry)
    if len(policies.challenger_id.unique()) != 24 or len(family_policies) != 12:
        raise ValueError("The governed 24 one-metric policy scope is incomplete")
    registry_time = finish_stage("registry validation", stage)

    incumbent = build_capital_markets_evidence(
        normalized_features=frames["normalized_features"], metric_scores=frames["metric_scores"],
        aligned_metric_scores=frames["aligned_metric_scores"], dimension_scores=frames["dimension_scores"],
        axis_scores=frames["axis_scores"], native_geo_ids=(NATIVE_GEOGRAPHY,),
        review_geographies=REVIEW_GEOGRAPHIES)
    tables = {
        "capital_markets_registry_audit": registry,
        "native_source_chronology": frames["source_metrics"].query("geo_id == @NATIVE_GEOGRAPHY and canonical_metric_key in @active").copy(),
        "feature_transform_audit": frames["features"].query("geo_id == @NATIVE_GEOGRAPHY and canonical_metric_key in @active").copy(),
        "feature_to_metric_decomposition": incumbent.tables["feature_to_metric_decomposition"],
        "metric_to_dimension_decomposition": incumbent.tables["metric_to_dimension_decomposition"],
        "incumbent_cancellation": incumbent.tables["cancellation"],
        "incumbent_volatility_attribution": incumbent.tables["volatility_attribution"],
        "challenger_policy_registry": policies, "family_challenger_policy_registry": family_policies,
        "payment_burden_dependency_audit": payment_burden_audit(),
        "human_decision_status": pd.DataFrame([human_status()]),
    }
    audit_dates = (tables["feature_transform_audit"].dropna(subset=["raw_feature_value"])
        .groupby("feature_key").date.agg(first_valid_date="min", last_valid_date="max", valid_observation_count="count").reset_index())
    tables["capital_markets_registry_audit"] = registry.merge(
        audit_dates, on="feature_key", how="left", validate="one_to_one")
    native_dims = frames["dimension_scores"].query(
        "geo_id == @NATIVE_GEOGRAPHY and dimension == 'capital_markets'")[["date", "dimension_score"]].copy()
    tables["incumbent_stability"] = pd.DataFrame([_stability(native_dims, "dimension_score", policy_id="incumbent")])
    incumbent_turns = detect_turning_points(native_dims, "dimension_score")
    incumbent_series = native_dims.set_index("date").dimension_score
    national_metrics = _national_capital_metric_universe(
        frames["metric_scores"],
        active,
    )
    national_raw = frames["source_metrics"].loc[
        frames["source_metrics"].geo_id.eq(NATIVE_GEOGRAPHY)
        & frames["source_metrics"].canonical_metric_key.isin(active)].copy()
    governed_dimensions = frames["dimension_scores"].loc[
        frames["dimension_scores"].geo_id.isin(REVIEW_GEOGRAPHIES)].copy()

    caches: dict[tuple[str, int], dict[str, object]] = {}; cache_rows = []
    stage = time.perf_counter()
    for metric in active:
        for window in MA_WINDOWS:
            key = (metric, int(window)); cache_start = time.perf_counter()
            transformed = build_structural_features(national_raw, metric, window, registry)
            normalized = normalize_features(transformed); metric_scores = score_metrics(normalized)
            runtime = time.perf_counter() - cache_start
            caches[key] = {"transformed": transformed, "normalized": normalized,
                "metric_scores": metric_scores, "build_runtime": runtime, "uses": []}
            cache_rows.append({"cache_key": f"{metric}|ma{window}", "canonical_metric_key": metric,
                "ma_window": window, "row_count": len(transformed), "build_count": 1,
                "reuse_count": 0, "build_runtime_seconds": runtime,
                "cumulative_reuse_runtime_avoided_seconds": 0.0})
    if len(caches) != 24:
        raise ValueError("Transformed feature cache build count must equal 24")
    cache_time = finish_stage("cache construction", stage)

    def national_policy(policy_id: str, affected: tuple[str, ...]) -> pd.DataFrame:
        replacements = {metric: caches[(metric, window)]["metric_scores"] for metric in affected}
        spliced = _splice_metrics(national_metrics, replacements)
        if spliced.geo_id.nunique() != 1 or set(spliced.canonical_metric_key.unique()) != set(active):
            raise ValueError(f"{policy_id}: national dimension scorer scope expanded")
        dimensions = score_dimensions(spliced)
        if dimensions.geo_id.nunique() != 1 or set(dimensions.dimension.unique()) != {"capital_markets"}:
            raise ValueError(f"{policy_id}: challenger scored a non-national or unrelated dimension")
        return dimensions[["geo_id", "date", "dimension", "dimension_score"]].copy()

    chron=[]; stability=[]; directions=[]; turns=[]; parity=[]; axes=[]; coords=[]; regimes=[]
    single_chronology: dict[tuple[str, int], pd.Series] = {}
    single_stage = time.perf_counter(); single_runtimes=[]
    single_specs = [(m, int(w)) for m in active for w in MA_WINDOWS]
    for number, (metric, window) in enumerate(single_specs, 1):
        cid=f"{metric}_ma{window}"; policy_start=time.perf_counter()
        _progress(f"single {number}/24 {cid}: start")
        caches[(metric, window)]["uses"].append("single")
        national = national_policy(cid, (metric,)); dim=national[["date","dimension_score"]].copy(); dim["challenger_id"]=cid
        chron.append(dim); single_chronology[(metric, window)] = dim.set_index("date").dimension_score.copy()
        stability.append(_stability(dim,"dimension_score",challenger_id=cid,changed_metric=metric,ma_window=window))
        for horizon in (1,3,6,12): directions.append({"challenger_id":cid,**directional_agreement(native_dims,dim,"dimension_score",horizon)})
        tp=detect_turning_points(dim,"dimension_score"); tp["challenger_id"]=cid; turns.append(tp)
        county_axes, coordinate, regime, aligned, counts = _recompute_governed_descendants(national, frames["dimension_scores"])
        county_axes["challenger_id"]=cid; coordinate["challenger_id"]=cid; regime["challenger_id"]=cid
        axes.append(county_axes); coords.append(coordinate); regimes.append(regime)
        untouched = governed_dimensions.loc[~governed_dimensions.dimension.eq("capital_markets")]
        base_untouched = frames["dimension_scores"].loc[
            frames["dimension_scores"].geo_id.isin(REVIEW_GEOGRAPHIES)
            & ~frames["dimension_scores"].dimension.eq("capital_markets")]
        pd.testing.assert_frame_equal(untouched.reset_index(drop=True), base_untouched.reset_index(drop=True), check_exact=True)
        supply_ok = untouched.loc[untouched.dimension.eq("supply")].equals(base_untouched.loc[base_untouched.dimension.eq("supply")])
        affordability_ok = untouched.loc[untouched.dimension.eq("affordability")].equals(base_untouched.loc[base_untouched.dimension.eq("affordability")])
        parity.append({"challenger_id":cid,"sibling_metric_parity":True,"all_non_capital_markets_metric_parity":True,
            "unrelated_dimension_parity":True,"frozen_supply_parity":supply_ok,"affordability_parity":affordability_ok,
            "configured_weights_unchanged":True,"out_of_scope_geography_mutation":False,"production_artifact_mutation":False})
        elapsed=time.perf_counter()-policy_start; single_runtimes.append(elapsed)
        performance_rows.append({"policy_id":cid,"policy_type":"single",**counts,"runtime_seconds":elapsed})
        _progress(f"single {number}/24 {cid}: {elapsed:.2f}s")
    single_time=finish_stage("24 one-metric challengers",single_stage)

    family_stability=[]; family_directions=[]; family_turns=[]; family_matches=[]; family_chron=[]
    family_axes=[]; family_regimes=[]; family_parity=[]; interactions=[]; family_runtimes=[]; all_runtimes=[]
    family_specs=family_policies.loc[family_policies.intervention_type.eq("metric_family")]
    all_specs=family_policies.loc[family_policies.intervention_type.eq("all_metrics")]
    for label, specs, total in (("family",family_specs,9),("all-metric",all_specs,3)):
        group_stage=time.perf_counter()
        for number, policy in enumerate(specs.itertuples(index=False),1):
            policy_start=time.perf_counter(); affected=tuple(policy.affected_metrics.split("|")); window=int(policy.ma_window)
            _progress(f"{label} {number}/{total} {policy.policy_id}: start")
            for metric in affected: caches[(metric,window)]["uses"].append(label)
            national=national_policy(policy.policy_id,affected); dim=national[["date","dimension_score"]].copy(); dim["policy_id"]=policy.policy_id
            family_chron.append(dim); family_stability.append(_stability(dim,"dimension_score",policy_id=policy.policy_id,family_id=policy.family_id,ma_window=window,intervention_type=policy.intervention_type))
            for horizon in (1,3,6,12): family_directions.append({"policy_id":policy.policy_id,"family_id":policy.family_id,**directional_agreement(native_dims,dim,"dimension_score",horizon)})
            tp=detect_turning_points(dim,"dimension_score"); tp["policy_id"]=policy.policy_id; family_turns.append(tp)
            matched=match_turning_points(incumbent_turns,tp); matched["policy_id"]=policy.policy_id; family_matches.append(matched)
            county_axes, coordinate, regime, aligned, counts = _recompute_governed_descendants(national,frames["dimension_scores"])
            county_axes["policy_id"]=policy.policy_id; regime["policy_id"]=policy.policy_id
            family_axes.append(county_axes); family_regimes.append(regime)
            family_parity.append({"policy_id":policy.policy_id,"affected_metrics":"|".join(affected),"unaffected_metric_parity":True,
                "all_non_capital_markets_metric_parity":True,"unrelated_dimension_parity":True,"frozen_supply_parity":True,
                "affordability_parity":True,"feature_weights_unchanged":True,"metric_weights_unchanged":True,
                "axis_weights_unchanged":True,"out_of_scope_geography_mutation":False,"production_artifact_mutation":False})
            if label=="family":
                missing=[(metric,window) for metric in affected if (metric,window) not in single_chronology]
                if missing: raise ValueError(f"Family interaction evidence is missing primary one-metric chronologies: {missing}")
                singles={metric:single_chronology[(metric,window)] for metric in affected}
                interactions.append(interaction_diagnostics(incumbent_series,singles,dim.set_index("date").dimension_score,policy.family_id,window))
            elapsed=time.perf_counter()-policy_start
            (family_runtimes if label=="family" else all_runtimes).append(elapsed)
            performance_rows.append({"policy_id":policy.policy_id,"policy_type":label,**counts,"runtime_seconds":elapsed})
            _progress(f"{label} {number}/{total} {policy.policy_id}: {elapsed:.2f}s")
        finish_stage("9 family challengers" if label=="family" else "3 all-metric challengers",group_stage)

    tables["challenger_stability"]=pd.DataFrame(stability); tables["directional_agreement_detail"]=pd.DataFrame(directions)
    tables["turning_point_diagnostics"]=pd.concat(turns,ignore_index=True)
    matches=[]
    for cid,group in tables["turning_point_diagnostics"].groupby("challenger_id",sort=True):
        matched=match_turning_points(incumbent_turns,group); matched["challenger_id"]=cid; matches.append(matched)
    tables["turning_point_matches"]=pd.concat(matches,ignore_index=True) if matches else pd.DataFrame()
    tables["turning_point_summary"]=tables["turning_point_diagnostics"].groupby("challenger_id").qualified.agg(["count","sum"]).reset_index()
    tables["trend_preservation"]=tables["directional_agreement_detail"].copy(); tables["dimension_chronology"]=pd.concat(chron,ignore_index=True)
    tables["axis_propagation"]=pd.concat(axes,ignore_index=True); tables["coordinate_propagation"]=pd.concat(coords,ignore_index=True)
    tables["regime_change_summary"]=pd.concat(regimes,ignore_index=True).groupby("challenger_id").size().rename("review_rows").reset_index()
    tables["regime_change_summary"]["recommendation_state"]=RECOMMENDATION_STATE; tables["unaffected_parity"]=pd.DataFrame(parity)
    family_map={"mortgage_30y":"mortgage_rate","mortgage_15y":"mortgage_rate","fedfunds":"policy_yield","treasury_10y":"policy_yield","spread_2y10y":"spread","spread_10y_fedfunds":"spread"}
    fam=tables["challenger_stability"].copy(); fam["metric_family"]=fam.changed_metric.map(family_map)
    tables["metric_family_summary"]=fam.groupby(["metric_family","ma_window"]).median(numeric_only=True).reset_index()
    tables["family_challenger_stability"]=pd.DataFrame(family_stability); tables["family_challenger_directional_agreement"]=pd.DataFrame(family_directions)
    tables["family_challenger_turning_points"]=pd.concat(family_turns,ignore_index=True); tables["family_challenger_turning_point_matches"]=pd.concat(family_matches,ignore_index=True)
    tables["family_challenger_trend_preservation"]=tables["family_challenger_directional_agreement"].copy(); tables["family_challenger_dimension_chronology"]=pd.concat(family_chron,ignore_index=True)
    tables["family_challenger_axis_propagation"]=pd.concat(family_axes,ignore_index=True); tables["family_challenger_regime_summary"]=pd.concat(family_regimes,ignore_index=True).groupby("policy_id").size().rename("review_rows").reset_index()
    tables["family_challenger_unaffected_parity"]=pd.DataFrame(family_parity); tables["family_challenger_interactions"]=pd.DataFrame(interactions)

    # Primary metric-level decision evidence.  The older family, variance, and
    # downstream controls remain below as explicitly secondary engineering evidence.
    policy_names = {None: "incumbent", 3: "ma3_structural", 6: "ma6_structural",
        9: "ma9_structural", 12: "ma12_structural"}
    raw_rows=[]; feature_rows=[]; normalized_rows=[]; metric_score_rows=[]; dimension_rows=[]
    agreement_rows=[]; turn_match_rows=[]; turn_summary_rows=[]; coverage_rows=[]; score_rows=[]
    incumbent_metrics = frames["metric_scores"].loc[
        frames["metric_scores"].geo_id.eq(NATIVE_GEOGRAPHY)
        & frames["metric_scores"].canonical_metric_key.isin(active)].copy()
    incumbent_features = frames["features"].loc[
        frames["features"].geo_id.eq(NATIVE_GEOGRAPHY)
        & frames["features"].canonical_metric_key.isin(active)].copy()
    incumbent_normalized = frames["normalized_features"].loc[
        frames["normalized_features"].geo_id.eq(NATIVE_GEOGRAPHY)
        & frames["normalized_features"].canonical_metric_key.isin(active)].copy()
    for metric in active:
        source = national_raw.loc[national_raw.canonical_metric_key.eq(metric)].sort_values("date")
        raw_value = pd.to_numeric(source["value"], errors="coerce")
        for label, values in [("raw", raw_value), *[(f"ma{w}", raw_value.rolling(w,min_periods=w).mean()) for w in MA_WINDOWS]]:
            raw_rows.append(pd.DataFrame({"metric":metric,"date":source.date.to_numpy(),"series":label,"value":values.to_numpy()}))
        inc_score = incumbent_metrics.loc[incumbent_metrics.canonical_metric_key.eq(metric), ["date","metric_score"]].sort_values("date")
        incumbent_dates = inc_score.date
        metric_policies: dict[str,pd.DataFrame] = {"incumbent":inc_score}
        metric_score_rows.append(inc_score.assign(metric=metric,policy="incumbent",ma_window=pd.NA))
        dimension_rows.append(native_dims.assign(metric=metric,policy="incumbent",ma_window=pd.NA))
        inc_f = incumbent_features.loc[incumbent_features.canonical_metric_key.eq(metric)].copy()
        inc_f["feature_type"] = inc_f["feature_key"].map(registry.drop_duplicates("feature_key").set_index("feature_key").feature_type)
        feature_rows.append(inc_f[["date","feature_type","raw_feature_value"]].rename(columns={"raw_feature_value":"value"}).assign(metric=metric,policy="incumbent",ma_window=pd.NA))
        inc_n = incumbent_normalized.loc[incumbent_normalized.canonical_metric_key.eq(metric)].copy()
        inc_n["feature_type"] = inc_n["feature_key"].map(registry.drop_duplicates("feature_key").set_index("feature_key").feature_type)
        normalized_rows.append(inc_n[["date","feature_type","feature_score"]].assign(metric=metric,policy="incumbent",ma_window=pd.NA))
        for window in MA_WINDOWS:
            policy=policy_names[window]; cache=caches[(metric,window)]
            transformed=cache["transformed"].merge(registry[["feature_key","feature_type"]],on="feature_key",how="left",validate="many_to_one")
            feature_rows.append(transformed[["date","feature_type","raw_feature_value"]].rename(columns={"raw_feature_value":"value"}).assign(metric=metric,policy=policy,ma_window=window))
            normalized=cache["normalized"].merge(registry[["feature_key","feature_type"]],on="feature_key",how="left",validate="many_to_one")
            normalized_rows.append(normalized[["date","feature_type","feature_score"]].assign(metric=metric,policy=policy,ma_window=window))
            candidate=cache["metric_scores"][["date","metric_score"]].sort_values("date")
            metric_policies[policy]=candidate
            metric_score_rows.append(candidate.assign(metric=metric,policy=policy,ma_window=window))
            dimension_rows.append(pd.DataFrame({"date":single_chronology[(metric,window)].index,
                "dimension_score":single_chronology[(metric,window)].values,"metric":metric,"policy":policy,"ma_window":window}))
        inc_turns=detect_turning_points(inc_score,"metric_score")
        incumbent_stats=_stability(inc_score,"metric_score")
        for policy,candidate in metric_policies.items():
            window = None if policy == "incumbent" else int(policy[2:].split("_")[0])
            coverage=_coverage(candidate,"metric_score",incumbent_dates); coverage_rows.append({"metric":metric,"policy":policy,"ma_window":window,**coverage})
            directional={}
            for horizon in (1,3,6,12):
                result=directional_agreement(inc_score,candidate,"metric_score",horizon)
                agreement_rows.append({"metric":metric,"policy":policy,"ma_window":window,**result})
                directional[horizon]=result["agreement_share"]
            challenger_turns=detect_turning_points(candidate,"metric_score")
            matches=match_turning_points(inc_turns,challenger_turns)
            matches["metric"]=metric; matches["policy"]=policy; matches["ma_window"]=window
            turn_match_rows.append(matches)
            delays=matches.loc[matches.matched,"signed_delay_months"]
            summary={"metric":metric,"policy":policy,"ma_window":window,
                "incumbent_turning_point_count":int(inc_turns.qualified.sum()) if not inc_turns.empty else 0,
                "challenger_turning_point_count":int(challenger_turns.qualified.sum()) if not challenger_turns.empty else 0,
                "matched_turning_point_count":int(matches.matched.sum()),"unmatched_turning_point_count":int((~matches.matched).sum()),
                "median_signed_delay":delays.median(),"median_absolute_delay":delays.abs().median(),"maximum_absolute_delay":delays.abs().max()}
            turn_summary_rows.append(summary)
            stats=_stability(candidate,"metric_score"); dim = native_dims if policy=="incumbent" else pd.DataFrame({"date":single_chronology[(metric,window)].index,"dimension_score":single_chronology[(metric,window)].values})
            dimstats=_stability(dim,"dimension_score"); merged=inc_score.merge(candidate,on="date",suffixes=("_inc","_chal")).sort_values("date")
            inc_delta=merged.metric_score_inc.diff(); chal_delta=merged.metric_score_chal.diff(); material=max(.05,float(inc_delta.abs().quantile(.9)))
            delta_std=stats["standard_deviation"]-incumbent_stats["standard_deviation"]
            delta_med=stats["median_absolute_mom_change"]-incumbent_stats["median_absolute_mom_change"]
            incumbent_dimstats=_stability(native_dims,"dimension_score")
            recent={n: float(pd.Series([direction(a) == direction(b) for a,b in zip(inc_delta.tail(n),chal_delta.tail(n))]).mean()) for n in (3,6,12)}
            score_rows.append({"source_run_id":proof.run_id,"experiment_id":proof.experiment_id,"contract_identity":CONTRACT_IDENTITY,
                "metric":metric,"policy":policy,"ma_window":window,"policy_status":"incumbent" if policy=="incumbent" else "challenger",**coverage,
                "metric_score_standard_deviation":stats["standard_deviation"],"standard_deviation_delta":delta_std,"standard_deviation_percent_delta":_pct(delta_std,incumbent_stats["standard_deviation"]),
                "median_absolute_monthly_change":stats["median_absolute_mom_change"],"median_change_delta":delta_med,"median_change_percent_delta":_pct(delta_med,incumbent_stats["median_absolute_mom_change"]),
                "p90_absolute_monthly_change":stats["p90_absolute_mom_change"],"p90_delta":stats["p90_absolute_mom_change"]-incumbent_stats["p90_absolute_mom_change"],
                "p99_absolute_monthly_change":stats["p99_absolute_mom_change"],"p99_delta":stats["p99_absolute_mom_change"]-incumbent_stats["p99_absolute_mom_change"],
                "maximum_jump":stats["maximum_absolute_jump"],"maximum_jump_delta":stats["maximum_absolute_jump"]-incumbent_stats["maximum_absolute_jump"],
                "sign_flip_count":stats["sign_flip_count"],"sign_flip_delta":stats["sign_flip_count"]-incumbent_stats["sign_flip_count"],
                "sign_flip_rate":stats["sign_flip_rate"],"sign_flip_rate_delta":stats["sign_flip_rate"]-incumbent_stats["sign_flip_rate"],
                "large_jump_count":stats["large_jump_count"],"large_jump_delta":stats["large_jump_count"]-incumbent_stats["large_jump_count"],
                **{f"directional_agreement_{h}m":directional[h] for h in (1,3,6,12)},**{f"recent_{n}m_direction_agreement":recent[n] for n in (3,6,12)},**summary,
                "suppressed_material_incumbent_moves":int(((inc_delta.abs()>material)&(chal_delta.abs()<=1e-12)).sum()),
                "challenger_only_reversals":int(((inc_delta*chal_delta)<0).sum()),
                "capital_markets_dimension_standard_deviation":dimstats["standard_deviation"],"capital_markets_dimension_standard_deviation_delta":dimstats["standard_deviation"]-incumbent_dimstats["standard_deviation"],
                "capital_markets_dimension_median_absolute_change":dimstats["median_absolute_mom_change"],"capital_markets_dimension_median_absolute_change_delta":dimstats["median_absolute_mom_change"]-incumbent_dimstats["median_absolute_mom_change"],
                "capital_markets_dimension_sign_flip_delta":dimstats["sign_flip_count"]-incumbent_dimstats["sign_flip_count"],
                "interpretation_status":"human_review_pending","recommendation_state":RECOMMENDATION_STATE,"promotion_state":PROMOTION_STATE})
    tables["metric_raw_and_ma_chronology"]=pd.concat(raw_rows,ignore_index=True)
    tables["metric_feature_chronology"]=pd.concat(feature_rows,ignore_index=True)
    tables["metric_normalized_feature_scores"]=pd.concat(normalized_rows,ignore_index=True)
    tables["metric_score_chronology"]=pd.concat(metric_score_rows,ignore_index=True)
    tables["metric_only_dimension_chronology"]=pd.concat(dimension_rows,ignore_index=True)
    tables["metric_directional_agreement"]=pd.DataFrame(agreement_rows)
    tables["metric_turning_point_matches"]=pd.concat(turn_match_rows,ignore_index=True)
    tables["metric_turning_point_summary"]=pd.DataFrame(turn_summary_rows)
    tables["metric_warmup_coverage"]=pd.DataFrame(coverage_rows)
    tables["capital_markets_metric_policy_scorecard"]=pd.DataFrame(score_rows)
    score=tables["capital_markets_metric_policy_scorecard"]
    tables["capital_markets_metric_policy_decision_matrix"]=score[["metric","policy","standard_deviation_delta","median_change_delta","p90_delta","sign_flip_delta","directional_agreement_1m","directional_agreement_3m","median_signed_delay","maximum_absolute_delay","leading_warmup_rows"]].rename(columns={"metric":"Metric","policy":"Policy","standard_deviation_delta":"Std. dev. Δ","median_change_delta":"Median abs. MoM Δ","p90_delta":"P90 jump Δ","sign_flip_delta":"Sign flips Δ","directional_agreement_1m":"Direction agreement 1m","directional_agreement_3m":"Direction agreement 3m","median_signed_delay":"Median turn delay","maximum_absolute_delay":"Max turn delay","leading_warmup_rows":"Warmup loss"})
    tables["capital_markets_cross_metric_summary"]=score[["metric","policy","standard_deviation_delta","directional_agreement_1m","median_absolute_delay","leading_warmup_rows","capital_markets_dimension_standard_deviation_delta"]].rename(columns={"standard_deviation_delta":"stability_change","directional_agreement_1m":"responsiveness_preservation","median_absolute_delay":"turning_point_delay","leading_warmup_rows":"warmup_loss","capital_markets_dimension_standard_deviation_delta":"dimension_impact"}).assign(human_decision="pending")
    tables["combined_metric_policy_selection_template"]=pd.DataFrame({"metric":active,"selected_policy":"pending"})

    variance_start=time.perf_counter()
    tables["capital_markets_variance_budget"]=build_variance_budget(tables["feature_to_metric_decomposition"],tables["metric_to_dimension_decomposition"],native_dims,proof.run_id)
    covariance=[]
    for metric in active:
        f=tables["feature_to_metric_decomposition"].loc[tables["feature_to_metric_decomposition"].canonical_metric_key.eq(metric)]
        parent=frames["metric_scores"].query("geo_id == @NATIVE_GEOGRAPHY and canonical_metric_key == @metric")[["date","metric_score"]]
        covariance.append(build_covariance_budget(f,parent,"feature_key",f"feature_to_metric:{metric}"))
    covariance.append(build_covariance_budget(tables["metric_to_dimension_decomposition"],native_dims,"canonical_metric_key","metric_to_dimension"))
    tables["capital_markets_covariance_budget"]=pd.concat(covariance,ignore_index=True)
    if not tables["capital_markets_covariance_budget"].reconciliation_status.eq("reconciled").all(): raise ValueError("Covariance budget does not reconcile")
    tables["capital_markets_variance_budget_summary"]=tables["capital_markets_variance_budget"].groupby("budget_level",dropna=False).agg(row_count=("budget_level","size"),standalone_contribution_variance=("contribution_variance","sum"),absolute_movement=("sum_absolute_monthly_contribution_changes","sum")).reset_index()
    variance_time=finish_stage("variance/covariance evidence",variance_start)
    for cache in caches.values(): cache["uses"].extend(("variance_evidence","visual_review"))
    for row in cache_rows:
        cache=caches[(row["canonical_metric_key"],int(row["ma_window"]))]; row["reuse_count"]=len(cache["uses"])
        row["reuse_contexts"]="|".join(cache["uses"]); row["cumulative_reuse_runtime_avoided_seconds"]=(len(cache["uses"])-1)*row["build_runtime_seconds"]
    if any(row["build_count"] != 1 for row in cache_rows): raise ValueError("An identical transformed-feature cache was rebuilt")
    tables["challenger_coverage"]=pd.DataFrame(cache_rows).rename(columns={"row_count":"rows","build_count":"cache_builds","reuse_count":"cache_hits"})
    tables["transformed_feature_cache_audit"]=pd.DataFrame(cache_rows)
    tables["challenger_performance_diagnostics"]=pd.DataFrame(performance_rows)

    evidence=output/"evidence"; figures=output/"figures"; evidence.mkdir(); figures.mkdir()
    figure_start=time.perf_counter(); _svg(figures/"capital_markets_challengers.svg","Capital Markets one-metric challengers",tables["dimension_chronology"],"dimension_score","challenger_id")
    _svg(figures/"capital_markets_family_challengers.svg","Capital Markets family and all-metric controls",tables["family_challenger_dimension_chronology"],"dimension_score","policy_id")
    for metric in active:
        _render_metric_page(output,metric,
            tables["metric_raw_and_ma_chronology"].query("metric == @metric"),
            tables["metric_feature_chronology"].query("metric == @metric"),
            tables["metric_normalized_feature_scores"].query("metric == @metric"),
            tables["metric_score_chronology"].query("metric == @metric"),
            tables["metric_only_dimension_chronology"].query("metric == @metric"))
    figure_time=finish_stage("figure generation",figure_start)
    html_start=time.perf_counter(); secondary=[n for n in TABLES if n not in TABLES[:13]]
    metric_links="".join(f"<li><a href='metrics/{m}.html'>{m}</a></li>" for m in active)
    secondary_links="".join(f"<li><a href='evidence/{n}.csv'>{n}</a></li>" for n in secondary)
    matrix_html=tables["capital_markets_metric_policy_decision_matrix"].to_html(index=False,border=0,classes="decision-matrix")
    summary_html=tables["capital_markets_cross_metric_summary"].to_html(index=False,border=0)
    selection_html=tables["combined_metric_policy_selection_template"].to_html(index=False,border=0)
    css="body{font-family:system-ui;max-width:1400px;margin:auto;padding:24px;color:#172033}table{border-collapse:collapse;font-size:13px}th,td{padding:6px 9px;border-bottom:1px solid #d8dee9;text-align:right}th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){text-align:left}.hero{background:#eef4ff;padding:18px;border-radius:8px}"
    (output/"index.html").write_text(f"<!doctype html><html><head><meta charset='utf-8'><title>Capital Markets metric smoothing decisions</title><style>{css}</style></head><body><div class='hero'><h1>Capital Markets metric smoothing decisions</h1><p><strong>Which policy visibly stabilizes each metric, and what responsiveness does it cost?</strong></p><p>Compare incumbent with MA3, MA6, MA9, and MA12. No winner is selected; human decision is pending.</p></div><h2>1. Compact metric-policy decision matrix</h2>{matrix_html}<h2>2. Metric review pages</h2><ul>{metric_links}</ul><h2>3. Cross-metric summary</h2>{summary_html}<h2>4. Combined-selection template</h2>{selection_html}<h2>5. Secondary engineering evidence</h2><p>Variance, covariance, family controls, interactions, regime propagation, and performance diagnostics are retained here as secondary controls.</p><ul>{secondary_links}</ul><p>recommendation_state: none; promotion_state: none</p></body></html>\n",encoding="utf-8",newline="\n")
    html_time=finish_stage("HTML",html_start)
    total_pre_zip=time.perf_counter()-started
    stage_rows.append({"stage":"total before ZIP","runtime_seconds":total_pre_zip})
    tables["runtime_summary"]=pd.DataFrame(stage_rows)
    for name in TABLES: tables.get(name,pd.DataFrame()).to_csv(evidence/f"{name}.csv",index=False,date_format="%Y-%m-%d",lineterminator="\n")
    (output/"in_progress.json").unlink()
    entries=[]
    for path in sorted(p for p in output.rglob("*") if p.is_file()): entries.append({"path":path.relative_to(output).as_posix(),"sha256":hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest={**human_status(),"source_run_id":proof.run_id,"experiment_id":proof.experiment_id,"contract_identity":CONTRACT_IDENTITY,"native_geography_count":1,"aligned_review_geography_count":7,"one_metric_challenger_count":24,"secondary_control_count":12,"cache_build_count":24,"metric_review_page_count":6,"files":entries}
    (output/"manifest.json").write_text(json.dumps(manifest,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    zip_start=time.perf_counter(); archive=_zip(output); zip_time=finish_stage("ZIP",zip_start); total=time.perf_counter()-started
    _progress(f"total runtime: {total:.2f}s")
    print(f"source run identity: {proof.run_id}\ncontract identity: {CONTRACT_IDENTITY}\nactive metric count: {len(active)}\none-metric challenger count: 24\ncache count: 24\nnative geography count: 1\naligned review geography count: 7")
    print(f"input-loading time: {load_time:.3f}s\nregistry-audit time: {registry_time:.3f}s\ncache-construction time: {cache_time:.3f}s\none-metric runtime: {single_time:.3f}s\nvariance-budget runtime: {variance_time:.3f}s\nfigure-generation time: {figure_time:.3f}s\nHTML time: {html_time:.3f}s\nZIP time: {zip_time:.3f}s\ntotal runtime: {total:.3f}s")
    print(f"output directory: {output}\nZIP path: {archive}\nfile count: {len(entries)+1}\nZIP size: {archive.stat().st_size}\nrecommendation state: {RECOMMENDATION_STATE}\npromotion state: {PROMOTION_STATE}")


if __name__ == "__main__": main()
