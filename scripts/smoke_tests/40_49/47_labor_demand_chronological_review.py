from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
# scripts/smoke_tests/40_49/47_labor_demand_chronological_review.py


import numpy as np

from regime.experiments.labor_demand_chronological_review import (
    DECISION_START_DATE,
    EVENT_MATCH_WINDOW_MONTHS,
    FOCUS_GEOS,
    RUN_ORDER,
    build_labor_demand_chronological_review,
)


def _assert_nonempty_file(path: Path) -> None:
    if not path.exists():
        raise AssertionError(
            f"Expected output missing: {path}"
        )

    if path.stat().st_size <= 0:
        raise AssertionError(
            f"Expected output empty: {path}"
        )


def main() -> int:
    print(
        "[labor_demand_chronology] "
        "building chronological review..."
    )

    result = build_labor_demand_chronological_review()

    complete_coverage = result["complete_coverage"]
    dimension_panel = result["dimension_panel"]
    axis_panel = result["axis_panel"]
    monthly_panel = result["monthly_panel"]
    sign_change_events = result["sign_change_events"]
    event_matches = result["event_matches"]
    matched_summary = result["matched_event_summary"]
    window_summary = result["window_summary"]
    largest_disagreements_full = result["largest_disagreements_full"]
    decision_disagreements = result["decision_disagreements"]
    focused_windows = result["focused_event_windows"]

    for frame_name, frame in (
        ("complete coverage", complete_coverage),
        ("dimension panel", dimension_panel),
        ("axis panel", axis_panel),
        ("monthly panel", monthly_panel),
        ("sign-change events", sign_change_events),
        ("event matches", event_matches),
        ("matched-event summary", matched_summary),
        ("window summary", window_summary),
        ("full-history disagreements", largest_disagreements_full),
        ("decision disagreements", decision_disagreements),
        ("focused event windows", focused_windows),
    ):
        if frame.empty:
            raise AssertionError(
                f"{frame_name} is empty"
            )

    coverage_start = (
        complete_coverage.groupby(
            "geo_id"
        )[
            "date"
        ].min()
    )
    
    if (
        coverage_start
        < np.datetime64(
            "2009-09-01"
        )
    ).any():
        raise AssertionError(
            "Complete coverage starts before "
            "MA6 labor metrics are available"
        )

    expected_geos = set(FOCUS_GEOS)

    if set(
        monthly_panel["geo_id"].unique()
    ) != expected_geos:
        raise AssertionError(
            "Chronological panel is missing "
            "a focus geography"
        )

    for prefix in (
        "dimension_score",
        "axis_score",
    ):
        for run_role in RUN_ORDER:
            column = f"{prefix}_{run_role}"

            if column not in monthly_panel.columns:
                raise AssertionError(
                    f"Missing chronology column: {column}"
                )

            if not np.isfinite(
                monthly_panel[column]
            ).all():
                raise AssertionError(
                    f"{column} contains non-finite values"
                )

    if not set(
        sign_change_events["run_role"].unique()
    ).issubset(set(RUN_ORDER)):
        raise AssertionError(
            "Unexpected sign-change run role"
        )

    matched = event_matches[
        event_matches["matched_within_limit"]
    ]

    if matched.empty:
        raise AssertionError(
            "No challenger sign-change events "
            "matched baseline events"
        )

    if (
        matched[
            "absolute_lag_months"
        ].gt(
            EVENT_MATCH_WINDOW_MONTHS
        ).any()
    ):
        raise AssertionError(
            "Matched event exceeded the hard "
            "lag window"
        )
    
    duplicate_baseline_matches = (
        matched.duplicated(
            subset=[
                "geo_id",
                "series_name",
                "transition",
                "run_role",
                "baseline_event_date",
            ],
            keep=False,
        )
    )
    
    if duplicate_baseline_matches.any():
        raise AssertionError(
            "A baseline event was matched to "
            "multiple challenger events within "
            "the same challenger run"
        )
    
    if decision_disagreements[
        "date"
    ].lt(
        DECISION_START_DATE
    ).any():
        raise AssertionError(
            "Decision table contains pre-2019 rows"
        )

    expected_plot_count = len(FOCUS_GEOS) * 3

    if len(
        result["plot_paths"]
    ) != expected_plot_count:
        raise AssertionError(
            "Unexpected chronology plot count"
        )

    for path in result["plot_paths"]:
        _assert_nonempty_file(Path(path))

    for path in result["csv_outputs"].values():
        _assert_nonempty_file(Path(path))

    print(
        "[labor_demand_chronology] "
        f"output root: {result['output_root']}"
    )
    print(
        "[labor_demand_chronology] "
        f"common-coverage rows: {len(monthly_panel)}"
    )
    print(
        "[labor_demand_chronology] "
        f"sign-change events: {len(sign_change_events)}"
    )
    print(
        "[labor_demand_chronology] "
        f"event matches: {len(event_matches)}"
    )

    print(
        "\n[labor_demand_chronology] "
        "matched-only event lag summary:"
    )
    
    print(
        matched_summary.sort_values(
            [
                "geo_id",
                "series_name",
                "run_role",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[labor_demand_chronology] "
        "historical-window summary:"
    )

    print(
        window_summary.sort_values(
            [
                "geo_id",
                "window",
                "series_name",
                "challenger",
            ]
        ).to_string(index=False)
    )

    display_columns = [
        "geo_id",
        "date",
        "dimension_score_baseline",
        (
            "dimension_score_"
            "labor_ma6_momentum_lag3"
        ),
        "dimension_ma6_delta",
        "dimension_ma6_sign_diff",
        "axis_score_baseline",
        (
            "axis_score_"
            "labor_ma6_momentum_lag3"
        ),
        "axis_ma6_delta",
        "axis_ma6_sign_diff",
    ]

    print(
        "\n[labor_demand_chronology] "
        "largest post-2019 MA6 disagreements:"
    )

    print(
        decision_disagreements[
            display_columns
        ].to_string(index=False)
    )

    print(
        "\n[labor_demand_chronology] "
        f"focused event-window rows: "
        f"{len(focused_windows)}"
    )

    print(
        "\n[labor_demand_chronology] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
