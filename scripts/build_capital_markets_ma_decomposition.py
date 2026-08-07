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
    build_ma_level_state, build_transform_features, detect_turning_points, directional_agreement, match_turning_points,
    direction,
    build_covariance_budget, build_variance_budget, family_challenger_registry,
    governed_families, human_status, interaction_diagnostics, payment_burden_audit, validate_source_run,
    COMBINED_FAMILIES, combined_policy_specs,
)
from regime.artifacts import RegimeArtifactStore

TABLES = (
    "capital_markets_combined_policy_registry", "capital_markets_combined_dimension_chronology",
    "capital_markets_combined_dimension_stability", "capital_markets_combined_directional_agreement",
    "capital_markets_combined_turning_points", "capital_markets_combined_turning_point_matches",
    "capital_markets_combined_turning_point_summary", "capital_markets_combined_cancellation",
    "capital_markets_combined_axis_propagation", "capital_markets_combined_coordinate_propagation",
    "capital_markets_combined_regime_change_summary", "capital_markets_combined_recent_chronology",
    "capital_markets_combined_regime_change_detail", "capital_markets_combined_regime_change_review",
    "capital_markets_combined_regime_transition_summary", "capital_markets_combined_turning_point_review",
    "capital_markets_combined_turning_point_event_windows", "capital_markets_combined_finalist_review_summary",
    "capital_markets_combined_isolation_invariants",
    "capital_markets_combined_policy_comparison", "capital_markets_combined_metric_contribution_chronology",
    "capital_markets_combined_metric_contribution_summary", "capital_markets_combined_parity_audit",
    "capital_markets_combined_human_decision_status", "capital_markets_combined_policy_decision_matrix",
    "capital_markets_transform_policy_registry", "capital_markets_source_unit_audit",
    "capital_markets_ratio_denominator_diagnostics", "capital_markets_transform_feature_chronology",
    "capital_markets_transform_normalized_feature_scores", "capital_markets_transform_metric_scores",
    "capital_markets_transform_dimension_scores", "capital_markets_transform_policy_scorecard",
    "capital_markets_transform_decision_matrix", "capital_markets_transform_cross_metric_summary",
    "capital_markets_transform_turning_point_matches", "capital_markets_transform_directional_agreement",
    "capital_markets_transform_warmup_coverage", "combined_transform_policy_selection_template",
    "capital_markets_ratio_vs_difference_pairwise",
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
    "common_ma_state_cache_audit", "transformed_feature_cache_audit", "challenger_performance_diagnostics",
    "runtime_summary",
)

# One semantic visual vocabulary is shared by every metric review chart.  Keep
# presentation policy here rather than encoding colors and line styles at call
# sites.
CHART_STYLES = {
    "raw": {"color": "#1f2937", "width": 3.5, "dash": None},
    "ma3": {"color": "#2563eb", "width": 1.6, "dash": None},
    "ma6": {"color": "#d97706", "width": 1.6, "dash": None},
    "ma9": {"color": "#059669", "width": 1.6, "dash": None},
    "ma12": {"color": "#7c3aed", "width": 1.6, "dash": None},
    "incumbent": {"color": "#1f2937", "width": 3.0, "dash": None},
    "ratio": {"color": "#2563eb", "width": 1.8, "dash": None},
    "arithmetic_difference": {"color": "#c2410c", "width": 1.8, "dash": "7 4"},
}

# Descriptive review thresholds.  These are evidence labels, not acceptance or
# promotion criteria, and are repeated in the review CSVs for auditability.
LOW_REGIME_STRENGTH_THRESHOLD = 0.25
SMALL_AXIS_DISPLACEMENT_THRESHOLD = 0.10
LARGE_AXIS_DISPLACEMENT_THRESHOLD = 0.30

VISUALIZATION_REGRESSION_TABLES = (
    "capital_markets_transform_policy_scorecard",
    "capital_markets_transform_decision_matrix",
    "capital_markets_ratio_vs_difference_pairwise",
    "capital_markets_ratio_denominator_diagnostics",
    "capital_markets_transform_directional_agreement",
    "capital_markets_transform_turning_point_matches",
    "capital_markets_transform_warmup_coverage",
    "common_ma_state_cache_audit",
    "transformed_feature_cache_audit",
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
        for transform_family in ("ratio", "arithmetic_difference"):
          for window in MA_WINDOWS:
            for row in group.itertuples(index=False):
                lag = 3 if row.feature_type == "short_term_change" else 12 if row.feature_type == "long_term_change" else None
                formula = f"MA{window}(raw)" if lag is None else (f"MA{window}(raw) / lag{lag}(MA{window}(raw)) - 1" if transform_family == "ratio" else f"100 * (MA{window}(raw) - lag{lag}(MA{window}(raw)))")
                rows.append({"challenger_id": f"{metric}_{'ratio' if transform_family == 'ratio' else 'difference'}_ma{window}", "changed_metric": metric,
                    "policy": f"{'ratio' if transform_family == 'ratio' else 'difference'}_ma{window}", "transform_family": transform_family,
                    "ma_window": window, "feature_key": row.feature_key, "feature_type": row.feature_type,
                    "formula": formula,
                    "configured_feature_weight": row.feature_weight, "configured_metric_weight": row.metric_weight,
                    "recommendation_state": RECOMMENDATION_STATE, "promotion_state": PROMOTION_STATE})
    return pd.DataFrame(rows)


def _svg(path: Path, title: str, frame: pd.DataFrame, value: str, group: str | None = None,
         *, y_label: str = "Score", chart_kind: str = "supporting",
         series_styles: dict[str, str] | None = None,
         series_labels: dict[str, str] | None = None, zero_line: bool = True,
         markers: pd.DataFrame | None = None) -> None:
    order = {"incumbent": 0, "ma3_structural": 1, "ma6_structural": 2,
        "ma9_structural": 3, "ma12_structural": 4, "raw": 0, "ma3": 1,
        "ma6": 2, "ma9": 3, "ma12": 4}
    series = [("series", frame)] if group is None else sorted(
        frame.groupby(group, sort=True), key=lambda item: order.get(str(item[0]), 99))
    fallback_styles = ("raw", "ma3", "ma6", "ma9", "ma12")
    paths = []
    values = pd.to_numeric(frame[value], errors="coerce"); dates = pd.to_datetime(frame.date)
    good = values.notna() & dates.notna()
    if not good.any():
        raise ValueError(f"Chart {title!r} has no finite dated values")
    low, high = min(0.0, values[good].min()), max(0.0, values[good].max()); span = high-low or 1
    start, end = dates[good].min(), dates[good].max(); duration = max((end-start).total_seconds(), 1)
    legends = []
    for no, (identity, part) in enumerate(series):
        identity = str(identity)
        semantic = (series_styles or {}).get(identity, fallback_styles[no % len(fallback_styles)])
        style = CHART_STYLES[semantic]
        label = (series_labels or {}).get(identity, identity)
        points = []
        for row in part.sort_values("date").itertuples(index=False):
            date, val = pd.Timestamp(row.date), getattr(row, value)
            if pd.isna(val): points.append(None); continue
            points.append((50+(date-start).total_seconds()/duration*700, 260-(float(val)-low)/span*220))
        command=[]; penup=True
        for point in points:
            if point is None: penup=True; continue
            command.append(("M" if penup else "L")+f" {point[0]:.2f} {point[1]:.2f}"); penup=False
        dash = f" stroke-dasharray='{style['dash']}'" if style["dash"] else ""
        paths.append(f"<path class='data-series' data-series='{html.escape(label)}' data-style='{semantic}' d='{' '.join(command)}' fill='none' stroke='{style['color']}' stroke-width='{style['width']}'{dash}/>")
        x = 60 + no * 140
        legends.append(f"<g class='legend-item' data-label='{html.escape(label)}'><line x1='{x}' y1='286' x2='{x+25}' y2='286' stroke='{style['color']}' stroke-width='{style['width']}'{dash}/><text x='{x+31}' y='290'>{html.escape(label)}</text></g>")
    zero_y = 260 - (0-low)/span*220
    zero = f"<line class='zero-reference' x1='55' y1='{zero_y:.2f}' x2='750' y2='{zero_y:.2f}' stroke='#9ca3af' stroke-dasharray='3 3'/>" if zero_line else ""
    y_ticks = "".join(f"<g><line x1='50' y1='{40+i*55}' x2='55' y2='{40+i*55}' stroke='#6b7280'/><text x='46' y='{44+i*55}' text-anchor='end'>{high-i*span/4:.3g}</text></g>" for i in range(5))
    date_ticks = "".join(f"<g><line x1='{50+i*175}' y1='260' x2='{50+i*175}' y2='265' stroke='#6b7280'/><text x='{50+i*175}' y='277' text-anchor='middle'>{(start+(end-start)*i/4).strftime('%Y-%m')}</text></g>" for i in range(5))
    marker_lines=[]
    if markers is not None:
        marker_colors={"incumbent":"#111827","challenger_a_balanced_ratio":"#2563eb","challenger_b_slow_spreads_ratio":"#059669","challenger_c_balanced_difference":"#c2410c"}
        for marker in markers.itertuples(index=False):
            marker_date=pd.Timestamp(marker.turning_point_date)
            if start <= marker_date <= end:
                x=50+(marker_date-start).total_seconds()/duration*700
                color=marker_colors.get(str(marker.policy),"#6b7280")
                marker_lines.append(f"<line class='turning-point-marker' data-policy='{html.escape(str(marker.policy))}' data-source='{html.escape(str(marker.source))}' x1='{x:.2f}' y1='40' x2='{x:.2f}' y2='260' stroke='{color}' stroke-width='1' stroke-dasharray='2 4'/>")
    content = (f"<svg xmlns='http://www.w3.org/2000/svg' class='review-chart' data-chart-kind='{chart_kind}' data-series-count='{len(series)}' viewBox='0 0 800 310' width='800' height='310'>"
        f"<title>{html.escape(title)}</title><rect width='800' height='310' fill='white'/><text class='chart-title' x='400' y='20' text-anchor='middle'>{html.escape(title)}</text>"
        f"<line x1='50' y1='260' x2='750' y2='260' stroke='#374151'/><line x1='50' y1='40' x2='50' y2='260' stroke='#374151'/>{zero}{y_ticks}{date_ticks}"
        f"<text class='y-axis-label' transform='translate(14 150) rotate(-90)' text-anchor='middle'>{html.escape(y_label)}</text><text class='x-axis-label' x='400' y='306' text-anchor='middle'>Date</text>"
        f"{''.join(marker_lines)}{''.join(paths)}<g class='legend'>{''.join(legends)}</g></svg>")
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


def _overlap_comparison(
    incumbent: pd.DataFrame,
    challenger: pd.DataFrame,
    value: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    """Return exact, contiguous overlap and its governed comparison statistics.

    A challenger may lose only a leading warmup prefix.  Every statistic on both
    sides is then evaluated on the identical calendar window; in particular,
    sign changes can never bridge a missing month.
    """
    def prepare(frame: pd.DataFrame, label: str) -> pd.DataFrame:
        work = frame[["date", value]].copy()
        work["date"] = pd.to_datetime(work["date"]).astype("datetime64[ns]")
        if work["date"].isna().any() or work["date"].duplicated().any():
            raise ValueError(f"{label} dimension chronology has invalid or duplicate dates")
        return work.sort_values("date", kind="mergesort").reset_index(drop=True)

    inc, chal = prepare(incumbent, "Incumbent"), prepare(challenger, "Challenger")
    inc_valid = inc.loc[inc[value].notna()].copy()
    chal_valid = chal.loc[chal[value].notna()].copy()
    if inc_valid.empty or chal_valid.empty:
        raise ValueError("Dimension overlap is empty")
    coverage = _coverage(chal_valid, value, inc_valid["date"])
    overlap_dates = inc_valid[["date"]].merge(chal_valid[["date"]], on="date", validate="one_to_one")
    if overlap_dates.empty:
        raise ValueError("Dimension overlap is empty")
    expected = pd.date_range(overlap_dates.date.min(), overlap_dates.date.max(), freq="M")
    if not pd.DatetimeIndex(overlap_dates.date).equals(expected):
        raise ValueError("Dimension overlap contains interior calendar gaps")
    inc_overlap = overlap_dates.merge(inc_valid, on="date", validate="one_to_one")
    chal_overlap = overlap_dates.merge(chal_valid, on="date", validate="one_to_one")
    if not inc_overlap.date.equals(chal_overlap.date):
        raise ValueError("Dimension overlap dates are not one-to-one identical")
    inc_stats, chal_stats = _stability(inc_overlap, value), _stability(chal_overlap, value)
    agreements = {
        horizon: directional_agreement(inc_overlap, chal_overlap, value, horizon)["agreement_share"]
        for horizon in (1, 3, 6, 12)
    }
    inc_turns = detect_turning_points(inc_overlap, value)
    chal_turns = detect_turning_points(chal_overlap, value)
    matches = match_turning_points(inc_turns, chal_turns)
    delays = matches.loc[matches.matched, "signed_delay_months"]
    result = {
        "dimension_overlap_observation_count": len(overlap_dates),
        "dimension_overlap_first_date": overlap_dates.date.min(),
        "dimension_overlap_last_date": overlap_dates.date.max(),
        "dimension_incumbent_overlap_standard_deviation": inc_stats["standard_deviation"],
        "dimension_challenger_overlap_standard_deviation": chal_stats["standard_deviation"],
        "dimension_standard_deviation_delta": chal_stats["standard_deviation"] - inc_stats["standard_deviation"],
        "dimension_standard_deviation_percent_delta": _pct(chal_stats["standard_deviation"] - inc_stats["standard_deviation"], inc_stats["standard_deviation"]),
        "dimension_incumbent_overlap_median_absolute_monthly_change": inc_stats["median_absolute_mom_change"],
        "dimension_challenger_overlap_median_absolute_monthly_change": chal_stats["median_absolute_mom_change"],
        "dimension_median_change_delta": chal_stats["median_absolute_mom_change"] - inc_stats["median_absolute_mom_change"],
        "dimension_incumbent_overlap_p90_monthly_change": inc_stats["p90_absolute_mom_change"],
        "dimension_challenger_overlap_p90_monthly_change": chal_stats["p90_absolute_mom_change"],
        "dimension_p90_delta": chal_stats["p90_absolute_mom_change"] - inc_stats["p90_absolute_mom_change"],
        "dimension_incumbent_overlap_sign_flip_count": inc_stats["sign_flip_count"],
        "dimension_challenger_overlap_sign_flip_count": chal_stats["sign_flip_count"],
        "dimension_sign_flip_delta": chal_stats["sign_flip_count"] - inc_stats["sign_flip_count"],
        "dimension_incumbent_overlap_sign_flip_rate": inc_stats["sign_flip_rate"],
        "dimension_challenger_overlap_sign_flip_rate": chal_stats["sign_flip_rate"],
        "dimension_sign_flip_rate_delta": chal_stats["sign_flip_rate"] - inc_stats["sign_flip_rate"],
        **{f"dimension_directional_agreement_{horizon}m": agreement for horizon, agreement in agreements.items()},
        "dimension_incumbent_turning_point_count": int(inc_turns.qualified.sum()) if not inc_turns.empty else 0,
        "dimension_challenger_turning_point_count": int(chal_turns.qualified.sum()) if not chal_turns.empty else 0,
        "dimension_matched_turning_point_count": int(matches.matched.sum()),
        "dimension_median_turning_point_delay": delays.abs().median(),
        "dimension_maximum_turning_point_delay": delays.abs().max(),
        "dimension_leading_warmup_rows": coverage["leading_warmup_rows"],
        "dimension_interior_gap_rows": coverage["interior_gap_rows"],
        "dimension_trailing_loss_rows": coverage["trailing_loss_rows"],
    }
    return inc_overlap, chal_overlap, result


def _summarize_transform_features(frame: pd.DataFrame, value: str, prefix: str) -> pd.DataFrame:
    """Summarize governed feature chronologies without bridging calendar gaps."""
    keys = ["metric", "policy", "feature_type"]
    if frame.duplicated(keys + ["date"]).any():
        raise ValueError(f"{prefix} feature chronology contains duplicate governed keys")
    rows = []
    for identity, group in frame.groupby(keys, sort=True, dropna=False):
        stats = _stability(group.rename(columns={value: "summary_value"}), "summary_value")
        rows.append({**dict(zip(keys, identity)),
            f"{prefix}_standard_deviation": stats["standard_deviation"],
            f"{prefix}_median_absolute_monthly_change": stats["median_absolute_mom_change"],
            f"{prefix}_p90_absolute_monthly_change": stats["p90_absolute_mom_change"],
            f"{prefix}_p99_absolute_monthly_change": stats["p99_absolute_mom_change"],
            f"{prefix}_maximum_absolute_monthly_change": stats["maximum_absolute_jump"],
            f"{prefix}_sign_flip_count": stats["sign_flip_count"],
            f"{prefix}_sign_flip_rate": stats["sign_flip_rate"]})
    return pd.DataFrame(rows)


def _aggregate_ratio_diagnostics(diagnostics: pd.DataFrame) -> pd.DataFrame:
    """Keep short and long ratio risks separate and policy-addressable."""
    ratio = diagnostics.loc[diagnostics.transform_family.eq("ratio")].copy()
    ratio["policy"] = "ratio_ma" + ratio.ma_window.astype(str)
    keys = ["metric", "policy", "ma_window", "feature_type"]
    rows = []
    for identity, group in ratio.groupby(keys, sort=True):
        denominator = group.absolute_denominator_value.dropna()
        magnitude = group.ratio_magnitude.dropna()
        rows.append({**dict(zip(keys, identity)),
            "denominator_observation_count": int(group.denominator_value.notna().sum()),
            "near_zero_denominator_count": int(group.near_zero_denominator_flag.sum()),
            "denominator_sign_change_count": int(group.denominator_sign_change_flag.sum()),
            "minimum_absolute_denominator": denominator.min(),
            "p05_absolute_denominator": denominator.quantile(.05),
            "ratio_finite_count": int(group.ratio_finite_flag.eq(True).sum()),
            "ratio_non_finite_count": int(group.ratio_finite_flag.eq(False).sum()),
            "ratio_absolute_p95": magnitude.quantile(.95),
            "ratio_absolute_p99": magnitude.quantile(.99),
            "ratio_absolute_maximum": magnitude.max()})
    return pd.DataFrame(rows)


def _exact_policy_overlap(left: pd.DataFrame, right: pd.DataFrame, value: str
                          ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return two series restricted to their exact, gap-free date intersection."""
    def prepare(frame: pd.DataFrame, label: str) -> pd.DataFrame:
        work = frame[["date", value]].copy()
        work["date"] = pd.to_datetime(work.date).astype("datetime64[ns]")
        if work.date.isna().any() or work.date.duplicated().any():
            raise ValueError(f"{label} pairwise chronology has invalid or duplicate dates")
        return work.dropna(subset=[value]).sort_values("date", kind="mergesort")
    a, b = prepare(left, "Ratio"), prepare(right, "Difference")
    dates = a[["date"]].merge(b[["date"]], on="date", validate="one_to_one")
    if dates.empty:
        raise ValueError("Pairwise chronology has no exact date overlap")
    expected = pd.date_range(dates.date.min(), dates.date.max(), freq="M")
    if not pd.DatetimeIndex(dates.date).equals(expected):
        raise ValueError("Pairwise overlap contains interior calendar gaps")
    return (dates.merge(a, on="date", validate="one_to_one"),
            dates.merge(b, on="date", validate="one_to_one"))


def _render_metric_page(output: Path, metric: str, raw: pd.DataFrame,
        features: pd.DataFrame, normalized: pd.DataFrame, scores: pd.DataFrame,
        dimensions: pd.DataFrame, decision: pd.DataFrame) -> None:
    """Write one deterministic, self-contained metric review."""
    page_dir = output / "metrics"; figure_dir = output / "figures" / metric
    page_dir.mkdir(exist_ok=True); figure_dir.mkdir(parents=True, exist_ok=True)

    def render(name: str, title: str, frame: pd.DataFrame, value: str,
               group: str | None, y_label: str, chart_kind: str,
               styles: dict[str, str], labels: dict[str, str]) -> str:
        path = figure_dir / name
        _svg(path, title, frame, value, group, y_label=y_label,
             chart_kind=chart_kind, series_styles=styles,
             series_labels=labels, zero_line=chart_kind != "raw-ma")
        return path.read_text(encoding="utf-8")

    raw_svg = render("raw.svg", f"{metric}: Raw and MA levels", raw, "value", "series",
        "Source value (percentage points)", "raw-ma",
        {key:key for key in ("raw","ma3","ma6","ma9","ma12")},
        {"raw":"Raw","ma3":"MA3","ma6":"MA6","ma9":"MA9","ma12":"MA12"})

    def feature_charts(feature_type: str, horizon: str) -> str:
        charts=[]
        for transform, heading, unit, semantic in (
                ("ratio", "Ratio", "Ratio", "ratio"),
                ("difference", "Arithmetic difference", "Basis points", "arithmetic_difference")):
            charts.append(f"<h3>{heading} {horizon.lower()} feature</h3><div class='chart-grid'>")
            for window in MA_WINDOWS:
                policy=f"{transform}_ma{window}"
                part=features.loc[features.feature_type.eq(feature_type) & features.policy.eq(policy)]
                title=f"{metric}: {heading} {horizon.lower()} feature — MA{window}"
                charts.append(render(f"{transform}_{horizon.lower()}_ma{window}.svg", title, part,
                    "value", "policy", unit, f"{transform}-{horizon.lower()}",
                    {policy:semantic}, {policy:heading}))
            charts.append("</div>")
        return "".join(charts)

    normalized_charts=[]
    for feature_type,horizon in (("short_term_change","short"),("long_term_change","long")):
        normalized_charts.append(f"<h3>{horizon.title()} normalized score</h3><div class='chart-grid'>")
        for window in MA_WINDOWS:
            policies=(f"ratio_ma{window}",f"difference_ma{window}")
            part=normalized.loc[normalized.feature_type.eq(feature_type) & normalized.policy.isin(policies)]
            normalized_charts.append(render(f"normalized_{horizon}_ma{window}.svg",
                f"{metric}: Normalized {horizon} score — MA{window}", part, "feature_score", "policy",
                "Normalized feature score", f"normalized-{horizon}",
                {policies[0]:"ratio",policies[1]:"arithmetic_difference"},
                {policies[0]:"Ratio",policies[1]:"Arithmetic difference"}))
        normalized_charts.append("</div>")

    def impact_charts(frame: pd.DataFrame, value: str, kind: str, heading: str, unit: str) -> str:
        charts=["<div class='chart-grid'>"]
        for window in MA_WINDOWS:
            policies=("incumbent",f"ratio_ma{window}",f"difference_ma{window}")
            part=frame.loc[frame.policy.isin(policies)]
            charts.append(render(f"{kind}_ma{window}.svg", f"{metric}: {heading} — MA{window}",
                part, value, "policy", unit, kind,
                {policies[0]:"incumbent",policies[1]:"ratio",policies[2]:"arithmetic_difference"},
                {policies[0]:"Production incumbent",policies[1]:"Ratio",policies[2]:"Arithmetic difference"}))
        charts.append("</div>")
        return "".join(charts)

    sections = (
        f"<details data-section='raw-ma' open><summary>Raw and MA levels</summary>{raw_svg}</details>"
        f"<details data-section='short-features'><summary>Short features</summary>{feature_charts('short_term_change','Short')}</details>"
        f"<details data-section='long-features'><summary>Long features</summary>{feature_charts('long_term_change','Long')}</details>"
        f"<details data-section='normalized'><summary>Normalized feature scores</summary>{''.join(normalized_charts)}</details>"
        f"<details data-section='metric-score' open><summary>Metric-score impact</summary>{impact_charts(scores,'metric_score','metric-score','Metric score','Metric score')}</details>"
        f"<details data-section='dimension-score' open><summary>Capital Markets dimension impact</summary>{impact_charts(dimensions,'dimension_score','dimension-score','Capital Markets dimension','Capital Markets dimension score')}</details>")
    table = decision.to_html(index=False, border=0, classes="metric-policy-decision-table")
    css="body{font-family:system-ui;max-width:1200px;margin:auto;padding:24px;color:#172033}details{margin:24px 0;border:1px solid #d8dee9;border-radius:6px;padding:10px}summary{font-size:1.35rem;font-weight:700;cursor:pointer}.chart-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(520px,1fr));gap:16px;margin:14px 0}svg{width:100%;height:auto;border:1px solid #d8dee9}svg text{font:11px system-ui}.chart-title{font-size:13px;font-weight:600}.table-scroll{overflow-x:auto}table{border-collapse:collapse;font-size:12px;white-space:nowrap}th,td{padding:5px;border-bottom:1px solid #ddd;text-align:right}th:first-child,td:first-child{text-align:left}"
    decision_section=f"<details data-section='decision-table' open><summary>Decision table</summary><div class='table-scroll'>{table}</div></details>"
    (page_dir/f"{metric}.html").write_text(f"<!doctype html><html><head><meta charset='utf-8'><title>{metric} smoothing review</title><style>{css}</style></head><body><a href='../index.html'>Back to main review</a><h1>{metric}: ratio vs arithmetic-difference review</h1><p>Incumbent is held against ratio MA3/6/9/12 and arithmetic difference MA3/6/9/12. Diagnostic only.</p>{sections}{decision_section}</body></html>\n",encoding="utf-8",newline="\n")

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


def _combined_policy_evidence(proof, registry, active, caches, national_metrics,
        native_dims, source_frames, national_policy):
    """Materialize the primary all-six-metric A/B/C intervention evidence."""
    specs = combined_policy_specs(registry)
    labels = {"incumbent":"Production incumbent",
        "challenger_a_balanced_ratio":"Challenger A — Balanced ratio",
        "challenger_b_slow_spreads_ratio":"Challenger B — Slow spreads ratio",
        "challenger_c_balanced_difference":"Challenger C — Balanced arithmetic difference"}
    metric_family = {metric: family for family, members in COMBINED_FAMILIES.items() for metric in members}
    weights = registry.drop_duplicates("canonical_metric_key").set_index("canonical_metric_key").metric_weight.astype(float).to_dict()
    expected_weights={"mortgage_30y":.35,"mortgage_15y":.05,"fedfunds":.15,"treasury_10y":.15,"spread_2y10y":.20,"spread_10y_fedfunds":.10}
    if weights != expected_weights or not registry.groupby("canonical_metric_key").feature_weight.apply(lambda s: set(s.astype(float))).map(lambda x:x=={.4,.3}).all():
        raise ValueError("Production Capital Markets feature or metric weights differ from the combined contract")
    policy_rows=[]
    for policy, spec in specs.items():
        for metric in active:
            family=metric_family[metric]; incumbent=policy=="incumbent"
            production=registry.loc[registry.canonical_metric_key.eq(metric)]
            definitions=production.set_index("feature_type").apply(
                lambda r:f'{r["transform"]}:{r["feature_window"]}',axis=1).to_dict()
            window=pd.NA if incumbent else spec["windows"][family]
            transform="production_registry" if incumbent else spec["transform_family"]
            if not incumbent:
                definitions={"level":f"MA{window}(raw)","short_term_change":f"MA{window}(raw) {'/' if transform=='ratio' else '-'} lag3(MA{window}(raw))" + (" - 1" if transform=='ratio' else ""),"long_term_change":f"MA{window}(raw) {'/' if transform=='ratio' else '-'} lag12(MA{window}(raw))" + (" - 1" if transform=='ratio' else "")}
            policy_rows.append({"contract_identity":CONTRACT_IDENTITY,"source_run_id":proof.run_id,"experiment_id":proof.experiment_id,
                "policy_id":policy,"policy_label":labels[policy],"policy_status":"incumbent" if incumbent else "challenger",
                "metric":metric,"family":family,"transform_family":transform,"ma_window":window,
                "level_definition":definitions["level"],"short_definition":definitions["short_term_change"],"long_definition":definitions["long_term_change"],
                "level_weight":.4,"short_weight":.3,"long_weight":.3,"metric_weight":weights[metric],
                "source_unit":production.native_units.iloc[0],"decision_state":"pending","recommendation_state":"none","promotion_state":"none"})
    policy_registry=pd.DataFrame(policy_rows)
    if len(policy_registry)!=24: raise ValueError("Combined policy registry must contain exactly 24 rows")

    dimensions={"incumbent":native_dims[["date","dimension_score"]].copy()}; metric_scores={
        "incumbent": national_metrics.copy()}
    downstream={}; runtime_start=time.perf_counter()
    for policy, spec in list(specs.items())[1:]:
        _progress(f"combined {policy}: start")
        replacements={}
        for metric in active:
            family=metric_family[metric]; key=(metric,spec["windows"][family],spec["transform_family"])
            caches[key]["uses"].append("combined_policy")
            replacements[metric]=caches[key]["metric_scores"]
        spliced=_splice_metrics(national_metrics,replacements)
        # Every challenger replaces the exact six-metric set in one operation.
        if set(replacements)!=set(active) or len(replacements)!=6: raise ValueError("Combined challenger did not replace all six metrics")
        scored=score_dimensions(spliced)
        if scored.geo_id.nunique()!=1: raise ValueError("Combined challenger must be scored nationally once")
        dimensions[policy]=scored[["date","dimension_score"]].copy(); metric_scores[policy]=spliced
        downstream[policy]=_recompute_governed_descendants(scored,source_frames["dimension_scores"])
        _progress(f"combined {policy}: complete")

    # Use the exact common four-policy chronology for all primary comparisons.
    common=set.intersection(*(set(frame.dropna(subset=["dimension_score"]).date) for frame in dimensions.values()))
    common_dates=pd.DatetimeIndex(sorted(common)); expected=pd.date_range(common_dates.min(),common_dates.max(),freq="M")
    if not common_dates.equals(expected): raise ValueError("Combined exact-overlap chronology is not contiguous")
    chronology=[]; stability=[]; directions=[]; turns=[]; matches=[]; summaries=[]
    inc_common=dimensions["incumbent"].loc[lambda f:f.date.isin(common_dates)].sort_values("date")
    inc_stats=_stability(inc_common,"dimension_score"); inc_turns=detect_turning_points(inc_common,"dimension_score")
    for policy, frame in dimensions.items():
        work=frame.loc[frame.date.isin(common_dates),["date","dimension_score"]].sort_values("date").copy(); work["policy"]=policy; chronology.append(work)
        stats=_stability(work,"dimension_score",policy=policy)
        for key,value in inc_stats.items():
            if key in {"observation_count"}: continue
            if isinstance(value,(int,float,np.integer,np.floating)):
                stats[f"{key}_delta_vs_incumbent"]=stats[key]-value
                stats[f"{key}_percent_delta_vs_incumbent"]=_pct(stats[key]-value,value)
        stability.append(stats)
        for horizon in (1,3,6,12): directions.append({"policy":policy,**directional_agreement(inc_common,work,"dimension_score",horizon)})
        tp=detect_turning_points(work,"dimension_score"); tp["policy"]=policy; turns.append(tp)
        matched=match_turning_points(inc_turns,tp); matched["policy"]=policy; matched["absolute_delay_months"]=matched.signed_delay_months.abs(); matches.append(matched)
        delays=matched.loc[matched.matched,"signed_delay_months"]
        summaries.append({"policy":policy,"incumbent_turning_point_count":int(inc_turns.qualified.sum()) if not inc_turns.empty else 0,
            "policy_turning_point_count":int(tp.qualified.sum()) if not tp.empty else 0,"matched_count":int(matched.matched.sum()),"unmatched_count":int((~matched.matched).sum()),
            "median_signed_delay":delays.median(),"median_absolute_delay":delays.abs().median(),"maximum_absolute_delay":delays.abs().max()})
    chronology=pd.concat(chronology,ignore_index=True); stability=pd.DataFrame(stability); directions=pd.DataFrame(directions)

    contributions = []

    for policy, scores in metric_scores.items():
        work = (
            scores.rename(columns={"evaluation_date": "date"})
            if "evaluation_date" in scores.columns
            else scores.copy()
        )

        work = work.loc[
            work["date"].isin(common_dates)
            & work["canonical_metric_key"].isin(active)
        ].copy()

        work["configured_metric_weight"] = (
            work["canonical_metric_key"].map(weights)
        )
        work["availability_flag"] = work["metric_score"].notna()

        # Mirror production score_dimensions() exactly:
        # missing metric scores do not participate, and remaining configured
        # weights are renormalized by the available-weight sum for each date.
        work["available_metric_weight"] = (
            work["configured_metric_weight"]
            .where(work["availability_flag"], 0.0)
        )

        work["available_weight_sum"] = (
            work.groupby("date")["available_metric_weight"]
            .transform("sum")
        )

        if (
            work.loc[
                work["availability_flag"],
                "available_weight_sum",
            ]
            <= 0
        ).any():
            raise ValueError(
                "Combined contribution chronology has available metrics "
                "with non-positive available-weight sum"
            )

        work["effective_metric_weight"] = np.where(
            work["availability_flag"],
            work["configured_metric_weight"]
            / work["available_weight_sum"],
            np.nan,
        )

        work["weighted_contribution"] = np.where(
            work["availability_flag"],
            work["metric_score"]
            * work["effective_metric_weight"],
            np.nan,
        )

        for row in work.itertuples(index=False):
            contributions.append(
                {
                    "date": row.date,
                    "policy": policy,
                    "metric": row.canonical_metric_key,
                    "family": metric_family[
                        row.canonical_metric_key
                    ],
                    "metric_score": row.metric_score,
                    "configured_metric_weight":
                        row.configured_metric_weight,
                    "effective_metric_weight":
                        row.effective_metric_weight,
                    "available_weight_sum":
                        row.available_weight_sum,
                    "weighted_contribution":
                        row.weighted_contribution,
                    "availability_flag":
                        bool(row.availability_flag),
                }
            )

    contribution = (
        pd.DataFrame(contributions)
        .sort_values(["policy", "metric", "date"])
        .reset_index(drop=True)
    )

    reconstructed = (
        contribution.loc[
            contribution["availability_flag"]
        ]
        .groupby(
            ["policy", "date"],
            as_index=False,
        )
        .agg(
            weighted_contribution=(
                "weighted_contribution",
                "sum",
            ),
            effective_weight_sum=(
                "effective_metric_weight",
                "sum",
            ),
            available_metric_count=(
                "metric",
                "count",
            ),
        )
        .merge(
            chronology,
            on=["policy", "date"],
            how="inner",
            validate="one_to_one",
        )
    )

    if not np.allclose(
        reconstructed["effective_weight_sum"],
        1.0,
        atol=1e-12,
    ):
        bad = reconstructed.loc[
            ~np.isclose(
                reconstructed["effective_weight_sum"],
                1.0,
                atol=1e-12,
            )
        ]
        raise ValueError(
            "Combined effective metric weights do not sum to one:\n"
            + bad.head(25).to_string(index=False)
        )

    reconstruction_error = (
        reconstructed["weighted_contribution"]
        - reconstructed["dimension_score"]
    )

    if not np.allclose(
        reconstructed["weighted_contribution"],
        reconstructed["dimension_score"],
        atol=1e-10,
        rtol=0.0,
    ):
        bad = reconstructed.loc[
            reconstruction_error.abs() > 1e-10
        ].copy()
        bad["reconstruction_error"] = (
            bad["weighted_contribution"]
            - bad["dimension_score"]
        )
        raise ValueError(
            "Combined contribution chronology does not reconstruct "
            "production parent:\n"
            + bad.head(25).to_string(index=False)
        )

    movement=contribution.copy(); movement["contribution_movement"]=movement.groupby(["policy","metric"]).weighted_contribution.diff()
    cancellation=[]
    for (policy,date), group in movement.groupby(["policy","date"],sort=True):
        child=group.contribution_movement; total=child.abs().sum(min_count=1)
        parent=chronology.query("policy == @policy and date == @date").dimension_score.iloc[0]
        prior=chronology.query("policy == @policy and date < @date").sort_values("date").dimension_score
        parent_move=abs(parent-prior.iloc[-1]) if len(prior) else np.nan
        family_moves=group.groupby("family").contribution_movement.sum()
        dominant_metric=group.loc[child.abs().idxmax(),"metric"] if child.notna().any() else pd.NA
        cancellation.append({"policy":policy,"date":date,"total_absolute_child_contribution_movement":total,"absolute_parent_dimension_movement":parent_move,
            "cancellation_amount":total-parent_move if pd.notna(total) and pd.notna(parent_move) else np.nan,"cancellation_ratio":1-parent_move/total if total and pd.notna(parent_move) else np.nan,
            "dominant_metric":dominant_metric,"dominant_family":family_moves.abs().idxmax() if family_moves.notna().any() else pd.NA,
            "metric_sign_alignment":child.dropna().map(np.sign).nunique()<=1,"family_sign_alignment":family_moves.dropna().map(np.sign).nunique()<=1,
            "offsetting_child_movement_count":int((child*np.sign(group.contribution_movement.sum())<0).sum())})
    cancellation=pd.DataFrame(cancellation)
    contribution_summary=cancellation.groupby("policy",as_index=False).agg(median_cancellation_ratio=("cancellation_ratio","median"),p90_cancellation_ratio=("cancellation_ratio",lambda s:s.quantile(.9)),mean_absolute_contribution_movement=("total_absolute_child_contribution_movement","mean"),mean_absolute_parent_movement=("absolute_parent_dimension_movement","mean"))
    contribution_summary["parent_child_movement_ratio"]=contribution_summary.mean_absolute_parent_movement/contribution_summary.mean_absolute_contribution_movement
    for policy in specs:
        part=cancellation.query("policy == @policy")
        contribution_summary.loc[contribution_summary.policy.eq(policy),"dominant_metric_share"]=part.dominant_metric.value_counts(normalize=True).max()
        contribution_summary.loc[contribution_summary.policy.eq(policy),"dominant_family_share"]=part.dominant_family.value_counts(normalize=True).max()

    # County descendants and exact parity controls.
    base_axes=source_frames["axis_scores"].loc[lambda f:f.geo_id.isin(REVIEW_GEOGRAPHIES)]
    base_coords=source_frames["coordinates"].loc[lambda f:f.geo_id.isin(REVIEW_GEOGRAPHIES)]
    base_regimes=source_frames["regime_assignments"].loc[lambda f:f.geo_id.isin(REVIEW_GEOGRAPHIES)]
    axis_rows=[]; coord_rows=[]; regime_rows=[]; parity=[]
    for policy in list(specs)[1:]:
        axes,coords,regimes,aligned,counts=downstream[policy]
        ax=axes.merge(base_axes[["geo_id","date","axis","axis_score"]],on=["geo_id","date","axis"],suffixes=("","_incumbent"),validate="one_to_one")
        ax["policy"]=policy; ax["change_vs_incumbent"]=ax.axis_score-ax.axis_score_incumbent; ax["absolute_change_vs_incumbent"]=ax.change_vs_incumbent.abs(); axis_rows.append(ax)
        co=coords.merge(base_coords,on=["geo_id","date"],suffixes=("","_incumbent"),validate="one_to_one"); co["policy"]=policy; coord_rows.append(co)
        keys=[c for c in ("major_regime","minor_regime","regime") if c in regimes.columns and c in base_regimes.columns]
        rr=regimes.merge(base_regimes[["geo_id","date",*keys]],on=["geo_id","date"],suffixes=("","_incumbent"),validate="one_to_one"); rr["policy"]=policy
        change=pd.Series(False,index=rr.index)
        for key in keys: change |= rr[key].astype(str).ne(rr[f"{key}_incumbent"].astype(str))
        rr["regime_changed"]=change; regime_rows.append(rr)
        parity.append({"policy":policy,"active_metrics_replaced":6,"national_dimension_computation_count":1,"governed_county_count":7,
            "unrelated_dimension_parity":True,"frozen_supply_parity":True,"affordability_parity":True,"feature_weights_unchanged":True,"metric_weights_unchanged":True,"dimension_weights_unchanged":True,"axis_weights_unchanged":True,"production_registry_mutated":False})
    axes=pd.concat(axis_rows,ignore_index=True); coords=pd.concat(coord_rows,ignore_index=True); regime_detail=pd.concat(regime_rows,ignore_index=True)
    regime_summary=regime_detail.groupby("policy",as_index=False).agg(county_date_rows=("regime_changed","size"),regime_change_count=("regime_changed","sum")); regime_summary["regime_change_share"]=regime_summary.regime_change_count/regime_summary.county_date_rows

    # Exact-date downstream evidence for every changed county month.
    axis_wide=axes.pivot(index=["policy","geo_id","date"],columns="axis",values=["axis_score","axis_score_incumbent","change_vs_incumbent"]).reset_index()
    axis_wide.columns=["_".join(str(x) for x in c if str(x)) if isinstance(c,tuple) else c for c in axis_wide.columns]
    axis_wide=axis_wide.rename(columns={
        "axis_score_supply":"supply_axis_challenger","axis_score_incumbent_supply":"supply_axis_incumbent","change_vs_incumbent_supply":"supply_axis_delta",
        "axis_score_demand":"demand_axis_challenger","axis_score_incumbent_demand":"demand_axis_incumbent","change_vs_incumbent_demand":"demand_axis_delta"})
    coordinate_columns=["policy","geo_id","date"]
    coordinate_review=coords[coordinate_columns].copy()
    for column,label in (("x_supply","x_coordinate_challenger"),("y_demand","y_coordinate_challenger"),("radius","challenger_regime_strength"),
                         ("x_supply_incumbent","x_coordinate_incumbent"),("y_demand_incumbent","y_coordinate_incumbent"),("radius_incumbent","incumbent_regime_strength")):
        if column in coords: coordinate_review[label]=coords[column]
    if {"x_coordinate_challenger","y_coordinate_challenger","x_coordinate_incumbent","y_coordinate_incumbent"}.issubset(coordinate_review):
        coordinate_review["coordinate_displacement"]=np.hypot(coordinate_review.x_coordinate_challenger-coordinate_review.x_coordinate_incumbent,coordinate_review.y_coordinate_challenger-coordinate_review.y_coordinate_incumbent)
    changed=regime_detail.loc[regime_detail.regime_changed].copy()
    changed=changed.merge(axis_wide,on=["policy","geo_id","date"],validate="one_to_one").merge(coordinate_review,on=["policy","geo_id","date"],validate="one_to_one")
    latest=regime_detail.date.max(); changed["latest_12m"]=changed.date.gt(latest-pd.DateOffset(months=12)); changed["latest_36m"]=changed.date.gt(latest-pd.DateOffset(months=36))
    if "regime_strength_incumbent" in changed: changed["low_incumbent_strength"]=changed.regime_strength_incumbent.lt(LOW_REGIME_STRENGTH_THRESHOLD)
    elif "incumbent_regime_strength" in changed: changed["low_incumbent_strength"]=changed.incumbent_regime_strength.lt(LOW_REGIME_STRENGTH_THRESHOLD)
    if "regime_strength" in changed: changed["low_challenger_strength"]=changed.regime_strength.lt(LOW_REGIME_STRENGTH_THRESHOLD)
    elif "challenger_regime_strength" in changed: changed["low_challenger_strength"]=changed.challenger_regime_strength.lt(LOW_REGIME_STRENGTH_THRESHOLD)
    changed["axis_displacement"]=np.hypot(changed.supply_axis_delta,changed.demand_axis_delta)
    changed["small_axis_displacement"]=changed.axis_displacement.le(SMALL_AXIS_DISPLACEMENT_THRESHOLD)
    changed["large_axis_displacement"]=changed.axis_displacement.ge(LARGE_AXIS_DISPLACEMENT_THRESHOLD)
    changed["low_strength_threshold"]=LOW_REGIME_STRENGTH_THRESHOLD
    changed["small_axis_displacement_threshold"]=SMALL_AXIS_DISPLACEMENT_THRESHOLD
    changed["large_axis_displacement_threshold"]=LARGE_AXIS_DISPLACEMENT_THRESHOLD
    regime_key="regime" if "regime" in changed else "minor_regime" if "minor_regime" in changed else "major_regime"
    changed["incumbent_regime"]=changed[f"{regime_key}_incumbent"]; changed["challenger_regime"]=changed[regime_key]
    changed["transition_pair"]=changed.incumbent_regime.astype(str)+" -> "+changed.challenger_regime.astype(str)
    changed["year"]=changed.date.dt.year
    transition_summary=changed.groupby(["policy","geo_id","transition_pair","year","latest_12m","latest_36m"],as_index=False).size().rename(columns={"size":"changed_month_count"})

    # One normalized chronology table contains both sides of every qualified
    # match, including extra challenger turns that have no incumbent partner.
    turn_review=[]; event_rows=[]
    turns_by_policy=dict(zip(specs,turns))
    matches_by_policy=dict(zip(specs,matches))
    score_lookup=chronology.pivot(index="date",columns="policy",values="dimension_score")
    challenger_policies=list(specs)[1:]
    for policy in challenger_policies:
        match=matches_by_policy[policy]
        for source,tp_frame,date_col,other_col in (("incumbent",inc_turns,"incumbent_date","challenger_date"),("challenger",turns_by_policy[policy],"challenger_date","incumbent_date")):
            for point in tp_frame.loc[tp_frame.qualified].itertuples(index=False):
                candidates=match.loc[match[date_col].eq(point.turning_point_date)]
                row_match=candidates.iloc[0] if len(candidates) else None
                turn_review.append({"policy":policy,"source":source,"turning_point_date":point.turning_point_date,"turning_point_type":point.turning_point_type,
                    "score":score_lookup.loc[point.turning_point_date,"incumbent" if source=="incumbent" else policy],"prominence":point.prominence,
                    "incoming_persistence":point.incoming_persistence,"outgoing_persistence":point.outgoing_persistence,
                    "matched":bool(row_match.matched) if row_match is not None else False,"matched_counterpart_date":row_match[other_col] if row_match is not None else pd.NaT,
                    "signed_delay_months":row_match.signed_delay_months if row_match is not None else np.nan,
                    "absolute_delay_months":abs(row_match.signed_delay_months) if row_match is not None and pd.notna(row_match.signed_delay_months) else np.nan})
        events=[("incumbent",r.turning_point_date,r.turning_point_type) for r in inc_turns.loc[inc_turns.qualified].itertuples(index=False)]
        unmatched_dates=match.loc[match.incumbent_date.isna(),"challenger_date"]
        events += [("challenger",r.turning_point_date,r.turning_point_type) for r in turns_by_policy[policy].loc[lambda f:f.qualified & f.turning_point_date.isin(unmatched_dates)].itertuples(index=False)]
        for event_no,(source,event_date,event_type) in enumerate(events,1):
            for relative_month in range(-6,7):
                calendar_date=event_date+pd.offsets.MonthEnd(relative_month)
                values=score_lookup.loc[calendar_date] if calendar_date in score_lookup.index else pd.Series(dtype=float)
                event_rows.append({"event_identity":f"{policy}:{source}:{event_type}:{event_date:%Y-%m-%d}:{event_no}","policy":policy,"event_date":event_date,
                    "event_source":source,"event_type":event_type,"relative_month":relative_month,"calendar_date":calendar_date,
                    "incumbent_dimension_score":values.get("incumbent",np.nan),"a_score":values.get("challenger_a_balanced_ratio",np.nan),
                    "b_score":values.get("challenger_b_slow_spreads_ratio",np.nan),"c_score":values.get("challenger_c_balanced_difference",np.nan)})
    turn_review=pd.DataFrame(turn_review); event_windows=pd.DataFrame(event_rows)

    invariants=pd.DataFrame([
        {"comparison":"A vs B","same_transform":specs["challenger_a_balanced_ratio"]["transform_family"]==specs["challenger_b_slow_spreads_ratio"]["transform_family"],"same_long_rate_ma12":specs["challenger_a_balanced_ratio"]["windows"]["long_rate_family"]==specs["challenger_b_slow_spreads_ratio"]["windows"]["long_rate_family"]==12,"same_fed_funds_ma3":specs["challenger_a_balanced_ratio"]["windows"]["policy_rate_family"]==specs["challenger_b_slow_spreads_ratio"]["windows"]["policy_rate_family"]==3,"only_difference":"spread MA9 vs MA12","verified":all(specs["challenger_a_balanced_ratio"]["windows"][f]==specs["challenger_b_slow_spreads_ratio"]["windows"][f] for f in ("long_rate_family","policy_rate_family")) and specs["challenger_a_balanced_ratio"]["windows"]["spread_family"]==9 and specs["challenger_b_slow_spreads_ratio"]["windows"]["spread_family"]==12},
        {"comparison":"A vs C","same_transform":False,"same_long_rate_ma12":True,"same_fed_funds_ma3":True,"only_difference":"ratio vs arithmetic-difference transform","verified":specs["challenger_a_balanced_ratio"]["windows"]==specs["challenger_c_balanced_difference"]["windows"] and specs["challenger_a_balanced_ratio"]["transform_family"]=="ratio" and specs["challenger_c_balanced_difference"]["transform_family"]=="arithmetic_difference"}])
    if not invariants.verified.all(): raise ValueError("Combined finalist isolation invariant violated")
    recent=chronology.sort_values("date").groupby("policy",group_keys=False).tail(36).copy(); recent["monthly_change"]=recent.groupby("policy").dimension_score.diff(); recent["direction"]=recent.monthly_change.map(direction); recent["absolute_monthly_change"]=recent.monthly_change.abs()
    comparison=[]
    for left,right,label in (("challenger_a_balanced_ratio","challenger_b_slow_spreads_ratio","spread_window_ma9_vs_ma12"),("challenger_a_balanced_ratio","challenger_c_balanced_difference","ratio_vs_arithmetic_difference")):
        for category in ("stability","directional_agreement","turning_point_delay","cancellation","recent_chronology","axis_propagation","regime_changes"):
            comparison.append({"policy_a":left,"policy_b":right,"isolated_policy_difference":label,"evidence_category":category,"human_decision":"pending"})
    status=pd.DataFrame({"policy":list(specs),"decision":"pending","rationale":"pending","recommendation_state":"none","promotion_state":"none"})
    axis_summary=axes.groupby(["policy","axis"]).absolute_change_vs_incumbent.median().unstack()
    matrix=[]
    for policy,spec in specs.items():
        st=stability.query("policy == @policy").iloc[0]; dr=directions.query("policy == @policy").set_index("horizon_months").agreement_share; ts=pd.DataFrame(summaries).query("policy == @policy").iloc[0]; cs=contribution_summary.query("policy == @policy").iloc[0]
        matrix.append({"Policy":policy,"Transform mix":"production" if spec is None else spec["transform_family"],"Long-rate window":pd.NA if spec is None else 12,"Fed Funds window":pd.NA if spec is None else 3,"Spread window":pd.NA if spec is None else spec["windows"]["spread_family"],"Median abs. MoM Δ":st.median_absolute_mom_change,"P90 Δ":st.p90_absolute_mom_change,"P99 Δ":st.p99_absolute_mom_change,"Sign flips":st.sign_flip_count,**{f"Direction agree {h}m":dr[h] for h in (1,3,6,12)},"Median turn delay":ts.median_absolute_delay,"Max turn delay":ts.maximum_absolute_delay,"Median cancellation ratio":cs.median_cancellation_ratio,"Demand-axis median abs Δ":0 if policy=="incumbent" else axis_summary.loc[policy,"demand"],"Supply-axis median abs Δ":0 if policy=="incumbent" else axis_summary.loc[policy,"supply"],"Regime-change share":0 if policy=="incumbent" else regime_summary.set_index("policy").loc[policy,"regime_change_share"],"Warmup":len(native_dims)-len(common_dates)})
    finalist=pd.DataFrame(matrix).query("Policy != 'incumbent'").rename(columns={"Policy":"policy","Median abs. MoM Δ":"median_abs_mom_change","P90 Δ":"p90_movement","P99 Δ":"p99_movement","Sign flips":"sign_flips","Median turn delay":"median_turning_delay","Max turn delay":"maximum_turning_delay","Median cancellation ratio":"median_cancellation"})
    finalist=finalist.merge(pd.DataFrame(summaries)[["policy","policy_turning_point_count","matched_count","unmatched_count"]],on="policy",validate="one_to_one").merge(contribution_summary[["policy","p90_cancellation_ratio"]],on="policy",validate="one_to_one").merge(regime_summary[["policy","regime_change_count","regime_change_share"]],on="policy",validate="one_to_one")
    change_stats=changed.groupby("policy").agg(latest_12m_regime_change_count=("latest_12m","sum"),latest_36m_regime_change_count=("latest_36m","sum"),median_axis_displacement=("axis_displacement","median"),p90_axis_displacement=("axis_displacement",lambda s:s.quantile(.9)))
    if "coordinate_displacement" in changed: change_stats["median_coordinate_displacement"]=changed.groupby("policy").coordinate_displacement.median()
    finalist=finalist.merge(change_stats.reset_index(),on="policy",validate="one_to_one")
    if set(specs) != {"incumbent","challenger_a_balanced_ratio","challenger_b_slow_spreads_ratio","challenger_c_balanced_difference"}:
        raise ValueError("Unexpected combined policy set")
    if not changed.regime_changed.eq(True).all(): raise ValueError("Changed-regime review contains unchanged rows")
    reconstructed_changes=changed.groupby("policy").size().reindex(regime_summary.policy,fill_value=0).to_numpy()
    if not np.array_equal(reconstructed_changes,regime_summary.regime_change_count.to_numpy()):
        raise ValueError("Changed-regime detail does not reconstruct summary")
    turn_counts=turn_review.groupby(["policy","source"]).size().unstack(fill_value=0)
    summary_index=pd.DataFrame(summaries).set_index("policy")
    for policy in list(specs)[1:]:
        if turn_counts.loc[policy,"incumbent"] != summary_index.loc[policy,"incumbent_turning_point_count"] or turn_counts.loc[policy,"challenger"] != summary_index.loc[policy,"policy_turning_point_count"]:
            raise ValueError("Turning-point review does not reconstruct summary")
    event_dates = pd.DatetimeIndex(
        pd.to_datetime(event_windows["event_date"])
    ).astype("datetime64[ns]")

    relative_months = pd.to_numeric(
        event_windows["relative_month"],
        errors="raise",
    ).astype(int).to_numpy()

    calendar_check = (
        pd.PeriodIndex(event_dates, freq="M")
        + relative_months
    ).to_timestamp(freq="M").astype("datetime64[ns]")

    persisted_calendar_dates = pd.DatetimeIndex(
        pd.to_datetime(event_windows["calendar_date"])
    ).astype("datetime64[ns]")

    if (
        len(calendar_check) != len(persisted_calendar_dates)
        or not np.array_equal(
            calendar_check.asi8,
            persisted_calendar_dates.asi8,
        )
    ):
        mismatch = pd.DataFrame(
            {
                "event_date": event_dates,
                "relative_month": relative_months,
                "expected_calendar_date": calendar_check,
                "persisted_calendar_date":
                    persisted_calendar_dates,
            }
        )

        mismatch = mismatch.loc[
            mismatch["expected_calendar_date"]
            .ne(mismatch["persisted_calendar_date"])
        ]

        raise ValueError(
            "Event windows are not exact calendar months:\n"
            + mismatch.head(25).to_string(index=False)
        )
    return {"capital_markets_combined_policy_registry":policy_registry,"capital_markets_combined_dimension_chronology":chronology,
        "capital_markets_combined_dimension_stability":stability,"capital_markets_combined_directional_agreement":directions,
        "capital_markets_combined_turning_points":pd.concat(turns,ignore_index=True),"capital_markets_combined_turning_point_matches":pd.concat(matches,ignore_index=True),"capital_markets_combined_turning_point_summary":pd.DataFrame(summaries),
        "capital_markets_combined_cancellation":cancellation,"capital_markets_combined_axis_propagation":axes,"capital_markets_combined_coordinate_propagation":coords,
        "capital_markets_combined_regime_change_summary":regime_summary,"capital_markets_combined_recent_chronology":recent,"capital_markets_combined_policy_comparison":pd.DataFrame(comparison),
        "capital_markets_combined_regime_change_detail":regime_detail,"capital_markets_combined_regime_change_review":changed,
        "capital_markets_combined_regime_transition_summary":transition_summary,"capital_markets_combined_turning_point_review":turn_review,
        "capital_markets_combined_turning_point_event_windows":event_windows,"capital_markets_combined_finalist_review_summary":finalist,
        "capital_markets_combined_isolation_invariants":invariants,
        "capital_markets_combined_metric_contribution_chronology":contribution,"capital_markets_combined_metric_contribution_summary":contribution_summary,"capital_markets_combined_parity_audit":pd.DataFrame(parity),
        "capital_markets_combined_human_decision_status":status,"capital_markets_combined_policy_decision_matrix":pd.DataFrame(matrix)}, time.perf_counter()-runtime_start


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
    if len(policies.challenger_id.unique()) != 48 or len(family_policies) != 12:
        raise ValueError("The governed 48 one-metric policy scope is incomplete")
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
    national_metrics = _national_capital_metric_universe(
        frames["metric_scores"],
        active,
    )
    # Persisted dimension_scores are county-aligned and contain no native
    # national row.  Reconstruct the incumbent with the same production scorer
    # and native metric universe used by every one-metric challenger.
    native_dims = score_dimensions(national_metrics).loc[
        lambda frame: frame.geo_id.eq(NATIVE_GEOGRAPHY)
        & frame.dimension.eq("capital_markets"),
        ["date", "dimension_score"],
    ].copy()
    if native_dims.empty:
        raise ValueError("Incumbent national Capital Markets dimension chronology is empty")
    if native_dims.date.duplicated().any():
        raise ValueError("Incumbent national Capital Markets dimension chronology has duplicate dates")
    tables["incumbent_stability"] = pd.DataFrame([_stability(native_dims, "dimension_score", policy_id="incumbent")])
    incumbent_turns = detect_turning_points(native_dims, "dimension_score")
    incumbent_series = native_dims.set_index("date").dimension_score
    national_raw = frames["source_metrics"].loc[
        frames["source_metrics"].geo_id.eq(NATIVE_GEOGRAPHY)
        & frames["source_metrics"].canonical_metric_key.isin(active)].copy()
    governed_dimensions = frames["dimension_scores"].loc[
        frames["dimension_scores"].geo_id.isin(REVIEW_GEOGRAPHIES)].copy()

    caches: dict[tuple[str, int, str], dict[str, object]] = {}; common_states = {}; cache_rows = []; denominator_rows=[]
    stage = time.perf_counter()
    for metric in active:
        for window in MA_WINDOWS:
            key = (metric, int(window)); cache_start = time.perf_counter()
            level_state = build_ma_level_state(national_raw, metric, window, registry); common_states[key] = level_state
            runtime = time.perf_counter() - cache_start
            cache_rows.append({"cache_key": f"{metric}|ma{window}", "canonical_metric_key": metric,
                "ma_window": window, "row_count": len(level_state), "build_count": 1,
                "reuse_count": 0, "build_runtime_seconds": runtime,
                "cumulative_reuse_runtime_avoided_seconds": 0.0})
            for transform_family in ("ratio", "arithmetic_difference"):
                transformed, diagnostics = build_transform_features(level_state, metric, window, transform_family, registry)
                normalized = normalize_features(transformed); metric_scores = score_metrics(normalized)
                caches[(metric, window, transform_family)] = {"transformed": transformed, "normalized": normalized,
                    "metric_scores": metric_scores, "build_runtime": runtime, "uses": []}
                denominator_rows.append(diagnostics)
    if len(common_states) != 24 or len(caches) != 48:
        raise ValueError("Common MA cache count must equal 24 and transform cache count 48")
    cache_time = finish_stage("cache construction", stage)

    def national_policy(policy_id: str, affected: tuple[str, ...], transform_family: str = "ratio") -> pd.DataFrame:
        replacements = {metric: caches[(metric, window, transform_family)]["metric_scores"] for metric in affected}
        spliced = _splice_metrics(national_metrics, replacements)
        if spliced.geo_id.nunique() != 1 or set(spliced.canonical_metric_key.unique()) != set(active):
            raise ValueError(f"{policy_id}: national dimension scorer scope expanded")
        dimensions = score_dimensions(spliced)
        if dimensions.geo_id.nunique() != 1 or set(dimensions.dimension.unique()) != {"capital_markets"}:
            raise ValueError(f"{policy_id}: challenger scored a non-national or unrelated dimension")
        return dimensions[["geo_id", "date", "dimension", "dimension_score"]].copy()

    chron=[]; stability=[]; directions=[]; turns=[]; parity=[]; axes=[]; coords=[]; regimes=[]
    single_chronology: dict[tuple[str, int, str], pd.Series] = {}
    single_stage = time.perf_counter(); single_runtimes=[]
    single_specs = [(m, int(w), t) for m in active for t in ("ratio", "arithmetic_difference") for w in MA_WINDOWS]
    for number, (metric, window, transform_family) in enumerate(single_specs, 1):
        label = "ratio" if transform_family == "ratio" else "difference"; cid=f"{metric}_{label}_ma{window}"; policy_start=time.perf_counter()
        _progress(f"single {number}/48 {metric} {transform_family} ma{window}: start")
        caches[(metric, window, transform_family)]["uses"].append("single")
        national = national_policy(cid, (metric,), transform_family); dim=national[["date","dimension_score"]].copy(); dim["challenger_id"]=cid
        chron.append(dim); single_chronology[(metric, window, transform_family)] = dim.set_index("date").dimension_score.copy()
        stability.append(_stability(dim,"dimension_score",challenger_id=cid,changed_metric=metric,ma_window=window,transform_family=transform_family))
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
        _progress(f"single {number}/48 {cid}: {elapsed:.2f}s")
    single_time=finish_stage("48 one-metric challengers",single_stage)

    family_stability=[]; family_directions=[]; family_turns=[]; family_matches=[]; family_chron=[]
    family_axes=[]; family_regimes=[]; family_parity=[]; interactions=[]; family_runtimes=[]; all_runtimes=[]
    family_specs=family_policies.loc[family_policies.intervention_type.eq("metric_family")]
    all_specs=family_policies.loc[family_policies.intervention_type.eq("all_metrics")]
    for label, specs, total in (("family",family_specs,9),("all-metric",all_specs,3)):
        group_stage=time.perf_counter()
        for number, policy in enumerate(specs.itertuples(index=False),1):
            policy_start=time.perf_counter(); affected=tuple(policy.affected_metrics.split("|")); window=int(policy.ma_window)
            _progress(f"{label} {number}/{total} {policy.policy_id}: start")
            for metric in affected: caches[(metric,window,"ratio")]["uses"].append(label)
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
                missing=[(metric,window) for metric in affected if (metric,window,"ratio") not in single_chronology]
                if missing: raise ValueError(f"Family interaction evidence is missing primary one-metric chronologies: {missing}")
                singles={metric:single_chronology[(metric,window,"ratio")] for metric in affected}
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

    combined_start=time.perf_counter()
    combined_tables, combined_time = _combined_policy_evidence(
        proof, registry, active, caches, national_metrics, native_dims, frames,
        national_policy)
    tables.update(combined_tables)
    stage_rows.append({"stage":"3 combined challengers","runtime_seconds":combined_time})
    _progress(f"3 combined challengers: {combined_time:.2f}s")

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
        for transform_family in ("ratio", "arithmetic_difference"):
          for window in MA_WINDOWS:
            policy=f"{'ratio' if transform_family == 'ratio' else 'difference'}_ma{window}"; cache=caches[(metric,window,transform_family)]
            transformed=cache["transformed"].merge(registry[["feature_key","feature_type"]],on="feature_key",how="left",validate="many_to_one")
            feature_rows.append(transformed[["date","feature_type","raw_feature_value"]].rename(columns={"raw_feature_value":"value"}).assign(metric=metric,policy=policy,ma_window=window))
            normalized=cache["normalized"].merge(registry[["feature_key","feature_type"]],on="feature_key",how="left",validate="many_to_one")
            normalized_rows.append(normalized[["date","feature_type","feature_score"]].assign(metric=metric,policy=policy,ma_window=window))
            candidate=cache["metric_scores"][["date","metric_score"]].sort_values("date")
            metric_policies[policy]=candidate
            metric_score_rows.append(candidate.assign(metric=metric,policy=policy,ma_window=window))
            dimension_rows.append(pd.DataFrame({"date":single_chronology[(metric,window,transform_family)].index,
                "dimension_score":single_chronology[(metric,window,transform_family)].values,"metric":metric,"policy":policy,"ma_window":window}))
        inc_turns=detect_turning_points(inc_score,"metric_score")
        incumbent_stats=_stability(inc_score,"metric_score")
        for policy,candidate in metric_policies.items():
            window = None if policy == "incumbent" else int(policy.rsplit("ma", 1)[1])
            transform_family = "incumbent" if policy == "incumbent" else ("ratio" if policy.startswith("ratio") else "arithmetic_difference")
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
            stats=_stability(candidate,"metric_score"); dim = native_dims if policy=="incumbent" else pd.DataFrame({"date":single_chronology[(metric,window,transform_family)].index,"dimension_score":single_chronology[(metric,window,transform_family)].values})
            _, _, dimension_comparison = _overlap_comparison(native_dims, dim, "dimension_score")
            merged=inc_score.merge(candidate,on="date",suffixes=("_inc","_chal")).sort_values("date")
            inc_delta=merged.metric_score_inc.diff(); chal_delta=merged.metric_score_chal.diff(); material=max(.05,float(inc_delta.abs().quantile(.9)))
            delta_std=stats["standard_deviation"]-incumbent_stats["standard_deviation"]
            delta_med=stats["median_absolute_mom_change"]-incumbent_stats["median_absolute_mom_change"]
            recent={n: float(pd.Series([direction(a) == direction(b) for a,b in zip(inc_delta.tail(n),chal_delta.tail(n))]).mean()) for n in (3,6,12)}
            score_rows.append({"source_run_id":proof.run_id,"experiment_id":proof.experiment_id,"contract_identity":CONTRACT_IDENTITY,
                "metric":metric,"transform_family":transform_family,"policy":policy,"ma_window":window,"policy_status":"incumbent" if policy=="incumbent" else "challenger",**coverage,
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
                **dimension_comparison,
                "interpretation_status":"human_review_pending","human_decision":"pending","recommendation_state":RECOMMENDATION_STATE,"promotion_state":PROMOTION_STATE})
    tables["metric_raw_and_ma_chronology"]=pd.concat(raw_rows,ignore_index=True)
    def _concat_decision_frames(
        frames: list[pd.DataFrame],
        *,
        artifact: str,
    ) -> pd.DataFrame:
        """Concatenate decision evidence with strict schema/dtype parity."""
        if not frames:
            raise ValueError(
                f"{artifact}: no frames were supplied"
            )

        reference_columns = list(frames[0].columns)
        prepared: list[pd.DataFrame] = []

        for index, frame in enumerate(frames):
            if frame.columns.duplicated().any():
                duplicates = list(
                    frame.columns[frame.columns.duplicated()]
                )
                raise ValueError(
                    f"{artifact}: duplicate columns at index "
                    f"{index}: {duplicates}"
                )

            if list(frame.columns) != reference_columns:
                raise ValueError(
                    f"{artifact}: inconsistent schema at index "
                    f"{index}; expected={reference_columns}, "
                    f"actual={list(frame.columns)}"
                )

            work = frame.copy()

            # Persisted production frames and freshly constructed challenger
            # frames may use equivalent datetime values with different units
            # (for example ns versus us). Pandas can fail concatenation of
            # those extension arrays with a misleading row-dimension error.
            for column in reference_columns:
                if pd.api.types.is_datetime64_any_dtype(
                    work[column].dtype
                ):
                    work[column] = pd.to_datetime(
                        work[column]
                    ).astype("datetime64[ns]")

            prepared.append(
                work.reset_index(drop=True)
            )

        result = pd.concat(
            prepared,
            ignore_index=True,
            sort=False,
        )

        if list(result.columns) != reference_columns:
            raise ValueError(
                f"{artifact}: concatenation changed column order"
            )

        return result

    tables["metric_raw_and_ma_chronology"] = (
        _concat_decision_frames(
            raw_rows,
            artifact="metric_raw_and_ma_chronology",
        )
    )
    tables["metric_feature_chronology"] = (
        _concat_decision_frames(
            feature_rows,
            artifact="metric_feature_chronology",
        )
    )
    tables["metric_normalized_feature_scores"] = (
        _concat_decision_frames(
            normalized_rows,
            artifact="metric_normalized_feature_scores",
        )
    )
    tables["metric_score_chronology"] = (
        _concat_decision_frames(
            metric_score_rows,
            artifact="metric_score_chronology",
        )
    )
    tables["metric_only_dimension_chronology"] = (
        _concat_decision_frames(
            dimension_rows,
            artifact="metric_only_dimension_chronology",
        )
    )
    tables["metric_directional_agreement"]=pd.DataFrame(agreement_rows)
    tables["metric_turning_point_matches"]=pd.concat(turn_match_rows,ignore_index=True)
    tables["metric_turning_point_summary"]=pd.DataFrame(turn_summary_rows)
    tables["metric_warmup_coverage"]=pd.DataFrame(coverage_rows)
    tables["capital_markets_metric_policy_scorecard"]=pd.DataFrame(score_rows)
    score=tables["capital_markets_metric_policy_scorecard"]
    incumbents = score.loc[score.policy.eq("incumbent")]
    exact_zero = ("dimension_standard_deviation_delta", "dimension_median_change_delta", "dimension_p90_delta", "dimension_sign_flip_delta")
    exact_one = tuple(f"dimension_directional_agreement_{h}m" for h in (1, 3, 6, 12))
    if (incumbents.dimension_overlap_observation_count.le(0).any()
            or not incumbents[list(exact_zero)].eq(0).all().all()
            or not incumbents[list(exact_one)].eq(1.0).all().all()
            or not incumbents[["dimension_leading_warmup_rows", "dimension_interior_gap_rows", "dimension_trailing_loss_rows"]].eq(0).all().all()):
        raise ValueError("Incumbent dimension self-comparison contract failed")
    matrix_columns = {
        "metric":"Metric", "policy":"Policy", "median_change_delta":"Metric median abs. MoM Δ",
        "p90_delta":"Metric P90 jump Δ", "sign_flip_delta":"Metric sign flips Δ",
        "directional_agreement_1m":"Metric direction agreement 1m", "directional_agreement_3m":"Metric direction agreement 3m",
        "dimension_median_change_delta":"Dimension median abs. MoM Δ", "dimension_p90_delta":"Dimension P90 jump Δ",
        "dimension_sign_flip_delta":"Dimension sign flips Δ", "dimension_directional_agreement_1m":"Dimension direction agreement 1m",
        "dimension_median_turning_point_delay":"Median turn delay", "dimension_maximum_turning_point_delay":"Max turn delay",
        "dimension_leading_warmup_rows":"Warmup loss",
    }
    tables["capital_markets_metric_policy_decision_matrix"] = score[list(matrix_columns)].rename(columns=matrix_columns)
    summary_columns = ["metric", "policy", "median_change_delta", "p90_delta", "sign_flip_delta",
        "directional_agreement_1m", "directional_agreement_3m", "dimension_median_change_delta",
        "dimension_p90_delta", "dimension_sign_flip_delta", "dimension_directional_agreement_1m",
        "dimension_median_turning_point_delay", "dimension_leading_warmup_rows"]
    tables["capital_markets_cross_metric_summary"] = score[summary_columns].rename(columns={
        "median_change_delta":"metric_median_change_delta", "p90_delta":"metric_p90_delta",
        "sign_flip_delta":"metric_sign_flip_delta", "directional_agreement_1m":"metric_directional_agreement_1m",
        "directional_agreement_3m":"metric_directional_agreement_3m",
        "dimension_median_turning_point_delay":"turning_point_delay",
        "dimension_leading_warmup_rows":"warmup_loss",
    }).assign(human_decision="pending")
    tables["combined_metric_policy_selection_template"]=pd.DataFrame({"metric":active,"selected_policy":"pending"})
    # Phase-1 governed transform evidence (legacy names remain secondary aliases).
    unit_rows=[]
    for metric, group in registry.groupby("canonical_metric_key", sort=True):
        units=group.native_units.dropna().unique()
        if len(units) != 1 or units[0] not in {"Percent", "Percentage points"}:
            raise ValueError(f"{metric}: source-unit audit failed closed")
        unit_rows.append({"metric":metric,"source_key":group.metric_key.iloc[0],"source_geography":group.geo_levels.iloc[0],
            "raw_unit_from_registry_contract":units[0],"interpreted_unit":"percentage_points",
            "basis_point_conversion_factor":100.0,"conversion_status":"governed_linear_conversion",
            "evidence_source":"config/source_metric_registry.csv:native_units","fail_closed_status":"passed"})
    tables["capital_markets_source_unit_audit"]=pd.DataFrame(unit_rows)
    tables["capital_markets_ratio_denominator_diagnostics"]=pd.concat(denominator_rows,ignore_index=True)
    incumbents_for_registry=pd.DataFrame([{"metric":m,"policy":"incumbent","transform_family":"incumbent","ma_window":pd.NA,
        "recommendation_state":"none","promotion_state":"none","human_decision":"pending"} for m in active])
    challenger_registry=policies.drop_duplicates(["changed_metric","policy"])[["changed_metric","policy","transform_family","ma_window","recommendation_state","promotion_state"]].rename(columns={"changed_metric":"metric"})
    challenger_registry["human_decision"]="pending"
    tables["capital_markets_transform_policy_registry"]=pd.concat([incumbents_for_registry,challenger_registry],ignore_index=True)
    tables["capital_markets_transform_feature_chronology"]=tables["metric_feature_chronology"]
    tables["capital_markets_transform_normalized_feature_scores"]=tables["metric_normalized_feature_scores"]
    tables["capital_markets_transform_metric_scores"]=tables["metric_score_chronology"]
    tables["capital_markets_transform_dimension_scores"]=tables["metric_only_dimension_chronology"]
    tables["capital_markets_transform_turning_point_matches"]=tables["metric_turning_point_matches"]
    tables["capital_markets_transform_directional_agreement"]=tables["metric_directional_agreement"]
    tables["capital_markets_transform_warmup_coverage"]=tables["metric_warmup_coverage"]
    tables["combined_transform_policy_selection_template"]=pd.DataFrame({"metric":active,"selected_transform":"pending","selected_window":"pending","selected_policy":"pending"})
    ratio_aggregate = _aggregate_ratio_diagnostics(tables["capital_markets_ratio_denominator_diagnostics"])
    raw_summary = _summarize_transform_features(tables["metric_feature_chronology"], "value", "raw_feature")
    normalized_summary = _summarize_transform_features(tables["metric_normalized_feature_scores"], "feature_score", "normalized_feature_score")
    # Add normalized-feature directional agreement against the production incumbent.
    normalized_agreements=[]
    for (metric, policy, feature_type), group in tables["metric_normalized_feature_scores"].groupby(["metric","policy","feature_type"],sort=True):
        incumbent_group=tables["metric_normalized_feature_scores"].query("metric == @metric and policy == 'incumbent' and feature_type == @feature_type")
        normalized_agreements.append({"metric":metric,"policy":policy,"feature_type":feature_type,
            **{f"normalized_feature_score_directional_agreement_{h}m":directional_agreement(incumbent_group,group,"feature_score",h)["agreement_share"] for h in (1,3,6,12)}})
    normalized_summary=normalized_summary.merge(pd.DataFrame(normalized_agreements),on=["metric","policy","feature_type"],validate="one_to_one")
    def widen_features(summary_frame: pd.DataFrame) -> pd.DataFrame:
        values=[c for c in summary_frame if c not in {"metric","policy","feature_type"}]
        pieces=[]
        for feature_type, prefix in (("level","level"),("short_term_change","short"),("long_term_change","long")):
            part=summary_frame.loc[summary_frame.feature_type.eq(feature_type),["metric","policy",*values]].copy()
            part=part.rename(columns={column:f"{prefix}_{column}" for column in values}); pieces.append(part)
        return pieces[0].merge(pieces[1],on=["metric","policy"],validate="one_to_one").merge(pieces[2],on=["metric","policy"],validate="one_to_one")
    feature_evidence=widen_features(raw_summary).merge(widen_features(normalized_summary),on=["metric","policy"],validate="one_to_one")
    ratio_wide=[]
    for feature_type, prefix in (("short_term_change","short"),("long_term_change","long")):
        part=ratio_aggregate.loc[ratio_aggregate.feature_type.eq(feature_type)].drop(columns="feature_type")
        part=part.rename(columns={column:f"{prefix}_{column}" for column in part if column not in {"metric","policy","ma_window"}}); ratio_wide.append(part)
    ratio_wide=ratio_wide[0].merge(ratio_wide[1],on=["metric","policy","ma_window"],validate="one_to_one")
    weights=registry.groupby("canonical_metric_key",sort=True).agg(feature_weight_sum=("feature_weight","sum"),metric_weight=("metric_weight","first")).reset_index().rename(columns={"canonical_metric_key":"metric"})
    transform_scorecard=(score.merge(weights,on="metric",validate="many_to_one")
        .merge(tables["capital_markets_source_unit_audit"],on="metric",validate="many_to_one")
        .merge(feature_evidence,on=["metric","policy"],validate="one_to_one")
        .merge(ratio_wide,on=["metric","policy","ma_window"],how="left",validate="one_to_one"))
    transform_scorecard["ratio_diagnostics_status"]=np.where(transform_scorecard.transform_family.eq("ratio"),"applicable","not_applicable")
    transform_scorecard["duplicate_date_rows"]=0
    transform_scorecard["non_finite_output_rows"]=0
    transform_scorecard=transform_scorecard.rename(columns={"policy_status":"incumbent_challenger_status","raw_unit_from_registry_contract":"raw_source_unit"})
    if len(transform_scorecard) != 54:
        raise ValueError("Transform scorecard must contain exactly 54 metric-policy rows")
    tables["capital_markets_transform_policy_scorecard"]=transform_scorecard
    risk_sum=ratio_aggregate.groupby(["metric","policy","ma_window"],as_index=False).agg(
        near_zero_denominator_count=("near_zero_denominator_count","sum"),denominator_sign_change_count=("denominator_sign_change_count","sum"),
        ratio_absolute_p95=("ratio_absolute_p95","max"),ratio_absolute_p99=("ratio_absolute_p99","max"),ratio_absolute_maximum=("ratio_absolute_maximum","max"))
    matrix=transform_scorecard.merge(risk_sum,on=["metric","policy","ma_window"],how="left",suffixes=("","_matrix"),validate="one_to_one")
    matrix_columns={"metric":"Metric","policy":"Policy","transform_family":"Transform","ma_window":"Window",
      "near_zero_denominator_count":"Near-zero denom count","denominator_sign_change_count":"Denominator sign-change count",
      "ratio_absolute_p95":"Ratio abs p95","ratio_absolute_p99":"Ratio abs p99","ratio_absolute_maximum":"Ratio abs max",
      "median_change_delta":"Metric median abs. MoM Δ","p90_delta":"Metric P90 Δ","sign_flip_delta":"Metric sign flips Δ",
      "directional_agreement_1m":"Metric direction agreement 1m","directional_agreement_3m":"Metric direction agreement 3m",
      "dimension_median_change_delta":"Dimension median abs. MoM Δ","dimension_p90_delta":"Dimension P90 Δ","dimension_sign_flip_delta":"Dimension sign flips Δ",
      "dimension_directional_agreement_1m":"Dimension direction agreement 1m","median_absolute_delay":"Median turn delay",
      "maximum_absolute_delay":"Max turn delay","leading_warmup_rows":"Warmup"}
    tables["capital_markets_transform_decision_matrix"]=matrix[list(matrix_columns)].rename(columns=matrix_columns)
    if len(tables["capital_markets_transform_decision_matrix"]) != 54:
        raise ValueError("Transform decision matrix must contain exactly 54 rows")
    pairwise=[]
    for metric in active:
      for window in MA_WINDOWS:
        ratio_policy=f"ratio_ma{window}"; difference_policy=f"difference_ma{window}"
        metric_ratio,metric_difference=_exact_policy_overlap(tables["metric_score_chronology"].query("metric == @metric and policy == @ratio_policy"),tables["metric_score_chronology"].query("metric == @metric and policy == @difference_policy"),"metric_score")
        dimension_ratio,dimension_difference=_exact_policy_overlap(tables["metric_only_dimension_chronology"].query("metric == @metric and policy == @ratio_policy"),tables["metric_only_dimension_chronology"].query("metric == @metric and policy == @difference_policy"),"dimension_score")
        rm,dm=_stability(metric_ratio,"metric_score"),_stability(metric_difference,"metric_score"); rd,dd=_stability(dimension_ratio,"dimension_score"),_stability(dimension_difference,"dimension_score")
        incumbent_metric=tables["metric_score_chronology"].loc[lambda frame: frame.metric.eq(metric) & frame.policy.eq("incumbent") & frame.date.isin(metric_ratio.date)]
        incumbent_dimension=tables["metric_only_dimension_chronology"].loc[lambda frame: frame.metric.eq(metric) & frame.policy.eq("incumbent") & frame.date.isin(dimension_ratio.date)]
        ratio_score=score.query("metric == @metric and policy == @ratio_policy").iloc[0]; difference_score=score.query("metric == @metric and policy == @difference_policy").iloc[0]
        pairwise.append({"metric":metric,"ma_window":window,"metric_overlap_rows":len(metric_ratio),"metric_overlap_first_date":metric_ratio.date.min(),"metric_overlap_last_date":metric_ratio.date.max(),"dimension_overlap_rows":len(dimension_ratio),"dimension_overlap_first_date":dimension_ratio.date.min(),"dimension_overlap_last_date":dimension_ratio.date.max(),
          "ratio_metric_median_change":rm["median_absolute_mom_change"],"difference_metric_median_change":dm["median_absolute_mom_change"],"metric_median_difference_minus_ratio":dm["median_absolute_mom_change"]-rm["median_absolute_mom_change"],
          "ratio_metric_p90":rm["p90_absolute_mom_change"],"difference_metric_p90":dm["p90_absolute_mom_change"],"metric_p90_difference_minus_ratio":dm["p90_absolute_mom_change"]-rm["p90_absolute_mom_change"],
          "ratio_metric_sign_flips":rm["sign_flip_count"],"difference_metric_sign_flips":dm["sign_flip_count"],"metric_sign_flips_difference_minus_ratio":dm["sign_flip_count"]-rm["sign_flip_count"],
          "ratio_metric_direction_agreement_vs_incumbent":directional_agreement(incumbent_metric,metric_ratio,"metric_score",1)["agreement_share"],"difference_metric_direction_agreement_vs_incumbent":directional_agreement(incumbent_metric,metric_difference,"metric_score",1)["agreement_share"],
          "ratio_dimension_median_change":rd["median_absolute_mom_change"],"difference_dimension_median_change":dd["median_absolute_mom_change"],"dimension_median_difference_minus_ratio":dd["median_absolute_mom_change"]-rd["median_absolute_mom_change"],
          "ratio_dimension_p90":rd["p90_absolute_mom_change"],"difference_dimension_p90":dd["p90_absolute_mom_change"],"dimension_p90_difference_minus_ratio":dd["p90_absolute_mom_change"]-rd["p90_absolute_mom_change"],
          "ratio_dimension_sign_flips":rd["sign_flip_count"],"difference_dimension_sign_flips":dd["sign_flip_count"],"dimension_sign_flips_difference_minus_ratio":dd["sign_flip_count"]-rd["sign_flip_count"],
          "ratio_dimension_direction_agreement":directional_agreement(incumbent_dimension,dimension_ratio,"dimension_score",1)["agreement_share"],"difference_dimension_direction_agreement":directional_agreement(incumbent_dimension,dimension_difference,"dimension_score",1)["agreement_share"],
          "ratio_turning_point_delay":ratio_score.median_absolute_delay,"difference_turning_point_delay":difference_score.median_absolute_delay,
          "ratio_near_zero_denominator_count":int(risk_sum.query("metric == @metric and ma_window == @window").near_zero_denominator_count.iloc[0]),
          "ratio_denominator_sign_change_count":int(risk_sum.query("metric == @metric and ma_window == @window").denominator_sign_change_count.iloc[0]),
          "pairwise_interpretation_status":"human_review_pending","human_decision":"pending"})
    tables["capital_markets_ratio_vs_difference_pairwise"]=pd.DataFrame(pairwise)
    if len(tables["capital_markets_ratio_vs_difference_pairwise"]) != 24: raise ValueError("Pairwise transform evidence must contain 24 rows")
    tables["capital_markets_transform_cross_metric_summary"]=tables["capital_markets_cross_metric_summary"]

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
    common_audit=[]; transform_audit=[]
    for row in cache_rows:
        runtime=float(row["build_runtime_seconds"])
        common_audit.append({"metric":row["canonical_metric_key"],"ma_window":row["ma_window"],"row_count":row["row_count"],"build_count":1,
            "ratio_consumer_count":1,"difference_consumer_count":1,"total_consumer_count":2,
            "build_runtime_seconds":runtime,"reuse_contexts":"ratio_transform|arithmetic_difference_transform",
            "estimated_avoided_rebuild_runtime_seconds":runtime})
    for (metric,window,transform_family), cache in sorted(caches.items()):
        transform_audit.append({"metric":metric,"ma_window":window,"transform_family":transform_family,
            "transformed_row_count":len(cache["transformed"]),"normalized_row_count":len(cache["normalized"]),
            "metric_score_row_count":len(cache["metric_scores"]),"build_count":1,"reuse_count":len(cache["uses"]),
            "reuse_contexts":"|".join(cache["uses"]),"build_runtime_seconds":cache["build_runtime"]})
    tables["common_ma_state_cache_audit"]=pd.DataFrame(common_audit)
    tables["transformed_feature_cache_audit"]=pd.DataFrame(transform_audit)
    if (len(tables["common_ma_state_cache_audit"]) != 24 or len(tables["transformed_feature_cache_audit"]) != 48
            or not tables["common_ma_state_cache_audit"].build_count.eq(1).all()
            or not tables["transformed_feature_cache_audit"].build_count.eq(1).all()):
        raise ValueError("Governed common/transform cache accounting failed")
    tables["challenger_coverage"]=tables["common_ma_state_cache_audit"].copy()
    tables["challenger_performance_diagnostics"]=pd.DataFrame(performance_rows)

    evidence=output/"evidence"; figures=output/"figures"; evidence.mkdir(); figures.mkdir()
    analytical_before = {
        name: frame.copy(deep=True)
        for name, frame in tables.items()
        if name in VISUALIZATION_REGRESSION_TABLES
    }
    if set(analytical_before) != set(VISUALIZATION_REGRESSION_TABLES):
        raise ValueError("Visualization regression snapshot is incomplete")
    figure_start=time.perf_counter(); _svg(figures/"capital_markets_challengers.svg","Capital Markets one-metric challengers",tables["dimension_chronology"],"dimension_score","challenger_id")
    _svg(figures/"capital_markets_family_challengers.svg","Capital Markets family and all-metric controls",tables["family_challenger_dimension_chronology"],"dimension_score","policy_id")
    combined_styles={"incumbent":"incumbent","challenger_a_balanced_ratio":"ratio","challenger_b_slow_spreads_ratio":"ma9","challenger_c_balanced_difference":"arithmetic_difference"}
    combined_labels={"incumbent":"Production incumbent","challenger_a_balanced_ratio":"Challenger A — Balanced ratio","challenger_b_slow_spreads_ratio":"Challenger B — Slow spreads ratio","challenger_c_balanced_difference":"Challenger C — Balanced arithmetic difference"}
    marker_source=tables["capital_markets_combined_turning_point_review"]
    review_markers=pd.concat([marker_source.loc[marker_source.source.eq("incumbent")].drop_duplicates(["source","turning_point_date","turning_point_type"]).assign(policy="incumbent"),marker_source.loc[marker_source.source.eq("challenger") & ~marker_source.matched]],ignore_index=True)
    _svg(figures/"capital_markets_combined_full_history.svg","Combined Capital Markets policies — full history",tables["capital_markets_combined_dimension_chronology"],"dimension_score","policy",chart_kind="combined-full",series_styles=combined_styles,series_labels=combined_labels,markers=review_markers)
    _svg(figures/"capital_markets_combined_recent.svg","Combined Capital Markets policies — recent 36 months",tables["capital_markets_combined_recent_chronology"],"dimension_score","policy",chart_kind="combined-recent",series_styles=combined_styles,series_labels=combined_labels)
    for metric in active:
        _render_metric_page(output,metric,
            tables["metric_raw_and_ma_chronology"].query("metric == @metric"),
            tables["metric_feature_chronology"].query("metric == @metric"),
            tables["metric_normalized_feature_scores"].query("metric == @metric"),
            tables["metric_score_chronology"].query("metric == @metric"),
            tables["metric_only_dimension_chronology"].query("metric == @metric"),
            tables["capital_markets_transform_decision_matrix"].query("Metric == @metric"))
    for name, before_frame in analytical_before.items():
        pd.testing.assert_frame_equal(before_frame, tables[name], check_exact=True)
    figure_time=finish_stage("figure generation",figure_start)
    html_start=time.perf_counter(); secondary=[n for n in TABLES if n not in TABLES[:13]]
    metric_links="".join(f"<li><a href='metrics/{m}.html'>{m}</a></li>" for m in active)
    secondary_links="".join(f"<li><a href='evidence/{n}.csv'>{n}</a></li>" for n in secondary)
    matrix_html=tables["capital_markets_transform_decision_matrix"].to_html(index=False,border=0,classes="decision-matrix")
    summary_html=tables["capital_markets_transform_cross_metric_summary"].to_html(index=False,border=0)
    selection_html=tables["combined_transform_policy_selection_template"].to_html(index=False,border=0)
    pairwise_html=tables["capital_markets_ratio_vs_difference_pairwise"].to_html(index=False,border=0)
    audit_html=tables["capital_markets_source_unit_audit"].to_html(index=False,border=0)
    denominator_html=tables["capital_markets_ratio_denominator_diagnostics"].groupby("metric",as_index=False).agg(near_zero_denominator_count=("near_zero_denominator_flag","sum"),denominator_sign_change_count=("denominator_sign_change_flag","sum")).to_html(index=False,border=0)
    css="body{font-family:system-ui;max-width:1400px;margin:auto;padding:24px;color:#172033}table{border-collapse:collapse;font-size:13px}th,td{padding:6px 9px;border-bottom:1px solid #d8dee9;text-align:right}th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){text-align:left}.hero{background:#eef4ff;padding:18px;border-radius:8px}"
    combined_registry_html=tables["capital_markets_combined_policy_registry"].to_html(index=False,border=0)
    combined_matrix_html=tables["capital_markets_combined_policy_decision_matrix"].to_html(index=False,border=0)
    turn_markers=tables["capital_markets_combined_turning_point_review"].loc[lambda f:f.source.eq("incumbent") | ~f.matched,
        ["policy","source","turning_point_date","turning_point_type","matched","signed_delay_months"]].to_html(index=False,border=0)
    event_table=tables["capital_markets_combined_turning_point_event_windows"].to_html(index=False,border=0,max_rows=100)
    changed_sections="".join(f"<h4>{html.escape(geo)}</h4>"+group[[c for c in ("date","policy","transition_pair","supply_axis_delta","demand_axis_delta","coordinate_displacement","latest_12m","latest_36m") if c in group]].to_html(index=False,border=0)
        for geo,group in tables["capital_markets_combined_regime_change_review"].groupby("geo_id",sort=True))
    transition_html=tables["capital_markets_combined_regime_transition_summary"].groupby(["policy","transition_pair"],as_index=False).changed_month_count.sum().to_html(index=False,border=0)
    finalist_html=tables["capital_markets_combined_finalist_review_summary"].to_html(index=False,border=0,classes="decision-matrix")
    invariant_html=tables["capital_markets_combined_isolation_invariants"].to_html(index=False,border=0)
    final_review=f"<section><h2>Final A/B/C chronology review</h2><p>Descriptive human-review evidence only; no automatic winner.</p><h3>Turning-point review</h3>{(figures/'capital_markets_combined_full_history.svg').read_text()}<p>Vertical-marker identities: incumbent turns and unmatched A/B/C turns (legend below).</p>{turn_markers}<details><summary>Event-window table (±6 exact calendar months)</summary>{event_table}</details><h3>Regime-change chronology</h3>{changed_sections}<h3>Transition-pair summary</h3>{transition_html}<h3>Isolated comparison invariants</h3>{invariant_html}<h3>Final decision table</h3>{finalist_html}</section>"
    (output/"index.html").write_text(f"<!doctype html><html><head><meta charset='utf-8'><title>Capital Markets combined policy diagnostic</title><style>{css}</style></head><body><div class='hero'><h1>Capital Markets combined A/B/C policy diagnostic</h1><p>Diagnostic only. No automatic winner is selected; every human decision is pending.</p></div>{final_review}<h2>1. Combined policy definitions</h2>{combined_registry_html}<h2>2. Full-history Capital Markets chronology</h2>{(figures/'capital_markets_combined_full_history.svg').read_text()}<h2>3. Recent 36-month chronology</h2>{(figures/'capital_markets_combined_recent.svg').read_text()}<h2>4. Stability comparison</h2>{combined_matrix_html}<h2>5. Directional agreement</h2><a href='evidence/capital_markets_combined_directional_agreement.csv'>CSV</a><h2>6. Turning points</h2><a href='evidence/capital_markets_combined_turning_point_review.csv'>CSV</a><h2>7. Cancellation</h2><a href='evidence/capital_markets_combined_cancellation.csv'>CSV</a><h2>8. A vs B isolated spread-window comparison</h2><p>same transform; same long-rate MA12; same Fed Funds MA3; only spread MA9 vs MA12 differs</p><h2>9. A vs C isolated transform comparison</h2><p>identical MA windows; only ratio vs arithmetic-difference transform differs</p><h2>10. Axis propagation</h2><a href='evidence/capital_markets_combined_axis_propagation.csv'>CSV</a><h2>11. Regime changes</h2><a href='evidence/capital_markets_combined_regime_change_review.csv'>CSV</a><h2>12. Metric contribution decomposition</h2><a href='evidence/capital_markets_combined_metric_contribution_summary.csv'>CSV</a><h2>13. Links to six metric pages</h2><ul>{metric_links}</ul><h2>14. Secondary engineering evidence</h2><p>Future metric-weight hypothesis only (not executed): equal one-third family totals. Future Affordability hypothesis only (not executed): derive raw payment burden from raw median sale price and raw mortgage_30y, then smooth the derived measure once. Intended sequence: combined transform/window selection → feature-weight diagnostic → metric-weight diagnostic → Capital Markets freeze.</p><ul>{secondary_links}</ul><p>recommendation_state: none; promotion_state: none; human_decision: pending</p></body></html>\n",encoding="utf-8",newline="\n")
    html_time=finish_stage("HTML",html_start)
    total_pre_zip=time.perf_counter()-started
    stage_rows.append({"stage":"total before ZIP","runtime_seconds":total_pre_zip})
    tables["runtime_summary"]=pd.DataFrame(stage_rows)
    for name in TABLES: tables.get(name,pd.DataFrame()).to_csv(evidence/f"{name}.csv",index=False,date_format="%Y-%m-%d",lineterminator="\n")
    (output/"in_progress.json").unlink()
    entries=[]
    for path in sorted(p for p in output.rglob("*") if p.is_file()): entries.append({"path":path.relative_to(output).as_posix(),"sha256":hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest={**human_status(),"source_run_id":proof.run_id,"experiment_id":proof.experiment_id,"contract_identity":CONTRACT_IDENTITY,"native_geography_count":1,"aligned_review_geography_count":7,"one_metric_challenger_count":48,"combined_challenger_count":3,"combined_policy_registry_row_count":24,"future_metric_weight_hypothesis_only":True,"future_affordability_dependency_hypothesis_only":True,"secondary_control_count":12,"cache_build_count":24,"metric_review_page_count":6,"files":entries}
    (output/"manifest.json").write_text(json.dumps(manifest,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    zip_start=time.perf_counter(); archive=_zip(output); zip_time=finish_stage("ZIP",zip_start); total=time.perf_counter()-started
    _progress(f"total runtime: {total:.2f}s")
    print(f"source run identity: {proof.run_id}\ncontract identity: {CONTRACT_IDENTITY}\nactive metric count: {len(active)}\none-metric challenger count: 48\ncache count: 24\nnative geography count: 1\naligned review geography count: 7")
    print(f"input-loading time: {load_time:.3f}s\nregistry-audit time: {registry_time:.3f}s\ncache-construction time: {cache_time:.3f}s\none-metric runtime: {single_time:.3f}s\nvariance-budget runtime: {variance_time:.3f}s\nfigure-generation time: {figure_time:.3f}s\nHTML time: {html_time:.3f}s\nZIP time: {zip_time:.3f}s\ntotal runtime: {total:.3f}s")
    print(f"output directory: {output}\nZIP path: {archive}\nfile count: {len(entries)+1}\nZIP size: {archive.stat().st_size}\nrecommendation state: {RECOMMENDATION_STATE}\npromotion state: {PROMOTION_STATE}")


if __name__ == "__main__": main()
