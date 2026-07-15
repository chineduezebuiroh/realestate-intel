from __future__ import annotations
# scripts/smoke_tests/40_49/45_demand_visual_review.py

from pathlib import Path

from regime.experiments.demand_visual_review import (
    FOCUS_GEOS,
    LABOR_METRICS,
    build_demand_visual_review,
)


def _assert_nonempty_file(
    path: Path,
) -> None:
    if not path.exists():
        raise AssertionError(
            f"Expected output does not exist: {path}"
        )

    if path.stat().st_size <= 0:
        raise AssertionError(
            f"Expected output is empty: {path}"
        )


def main() -> int:
    print(
        "[demand_visual_review] "
        "building visual review..."
    )

    result = (
        build_demand_visual_review()
    )

    plot_paths = result[
        "plot_paths"
    ]

    expected_plot_count = (
        len(FOCUS_GEOS)
        * (
            2
            + len(
                LABOR_METRICS
            )
        )
    )

    if (
        result[
            "plot_count"
        ]
        != expected_plot_count
    ):
        raise AssertionError(
            "Unexpected visual output count. "
            f"Expected {expected_plot_count}, "
            f"found {result['plot_count']}"
        )

    for path in plot_paths:
        _assert_nonempty_file(
            Path(path)
        )

    for key in (
        "contribution_csv",
        "labor_csv",
        "cancellation_csv",
    ):
        _assert_nonempty_file(
            Path(
                result[key]
            )
        )

    print(
        "[demand_visual_review] "
        f"output root: {result['output_root']}"
    )

    print(
        "[demand_visual_review] "
        f"plots: {result['plot_count']}"
    )

    for path in plot_paths:
        print(
            f"[demand_visual_review] {path}"
        )

    print(
        "[demand_visual_review] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
