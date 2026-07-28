from __future__ import annotations
# scripts/smoke_tests/30_39/36_inventory_chronological_review.py

from pathlib import Path

from regime.experiments.inventory_chronological_review import (
    BASELINE_RUN_ID,
    CHALLENGER_RUN_ID,
    DEFAULT_OUTPUT_DIR,
    FOCUS_GEOS,
    build_inventory_chronological_review,
    write_inventory_chronological_review,
)


def main() -> int:
    review = (
        build_inventory_chronological_review(
            baseline_run_id=(
                BASELINE_RUN_ID
            ),
            challenger_run_id=(
                CHALLENGER_RUN_ID
            ),
            geo_ids=FOCUS_GEOS,
        )
    )

    output_paths = (
        write_inventory_chronological_review(
            review,
            output_dir=(
                DEFAULT_OUTPUT_DIR
            ),
        )
    )

    panel = review.tables["monthly_panel"]
    changed = review.tables["changed_months"]
    event_windows = review.tables["major_event_windows"]
    summary = review.tables["changed_month_summary"]

    print(
        "[inventory_chronology] baseline:",
        BASELINE_RUN_ID,
    )

    print(
        "[inventory_chronology] challenger:",
        CHALLENGER_RUN_ID,
    )

    print(
        "[inventory_chronology] "
        "review contract:",
        type(review).__name__,
    )

    print(
        "[inventory_chronology] "
        "challenger materialization:",
        review.metadata[
            "challenger_materialization"
        ],
    )

    print(
        "[inventory_chronology] "
        "challenger assignment rows:",
        review.metadata[
            "challenger_assignment_rows"
        ],
    )

    print(
        "[inventory_chronology] "
        "smoothing lineage rows:",
        review.metadata[
            "smoothing_lineage_rows"
        ],
    )

    print(
        "[inventory_chronology] panel rows:",
        len(panel),
    )

    print(
        "[inventory_chronology] changed months:",
        len(changed),
    )

    print(
        "[inventory_chronology] "
        "major event-window rows:",
        len(event_windows),
    )

    print(
        "\n[inventory_chronology] "
        "changed-month summary:"
    )

    print(
        summary.sort_values(
            [
                "geo_id",
                "period",
                "assignment_group",
            ]
        ).to_string(
            index=False
        )
    )

    major_changed = changed[
        changed[
            "major_assignment_changed"
        ]
    ].copy()

    print(
        "\n[inventory_chronology] "
        "major assignment changes:"
    )

    print(
        major_changed.to_string(
            index=False
        )
    )

    recent_major = major_changed[
        major_changed["date"].ge(
            "2023-01-01"
        )
    ]

    print(
        "\n[inventory_chronology] "
        "2023–2026 major changes:"
    )

    print(
        recent_major.to_string(
            index=False
        )
    )

    print(
        "\n[inventory_chronology] tables:"
    )

    for path in output_paths[
        "tables"
    ]:
        print(path)

    print(
        "\n[inventory_chronology] plots:"
    )

    for path in output_paths[
        "plots"
    ]:
        print(path)

    required_outputs = {
        "monthly_panel",
        "changed_months",
        "major_event_windows",
        "changed_month_summary",
    }

    if set(
        review.tables
    ) != required_outputs:
        raise AssertionError(
            "Chronological review output set "
            "does not match its contract"
        )

    for name in required_outputs:
        if review.tables[name].empty:
            raise AssertionError(
                f"Expected non-empty output: {name}"
            )
        
    expected_metadata = {
        "review_type": (
            "inventory_chronological_review"
        ),
        "baseline_run_id": (
            BASELINE_RUN_ID
        ),
        "challenger_run_id": (
            CHALLENGER_RUN_ID
        ),
        "challenger_materialization": (
            "in_memory"
        ),
        "challenger_persisted": False,
    }

    for key, expected_value in (
        expected_metadata.items()
    ):
        actual_value = (
            review.metadata.get(key)
        )

        if actual_value != expected_value:
            raise AssertionError(
                "Review metadata mismatch for "
                f"{key}. Expected "
                f"{expected_value!r}, found "
                f"{actual_value!r}"
            )

    for key in (
        "challenger_feature_rows",
        "challenger_assignment_rows",
        "smoothing_lineage_rows",
    ):
        value = review.metadata.get(
            key
        )

        if (
            not isinstance(value, int)
            or value <= 0
        ):
            raise AssertionError(
                "Expected positive integer "
                f"metadata for {key}, found "
                f"{value!r}"
            )
    
    if set(
        panel["geo_id"]
    ) != set(
        FOCUS_GEOS
    ):
        raise AssertionError(
            "Focus-geography mismatch"
        )

    if panel.duplicated(
        subset=[
            "geo_id",
            "date",
        ]
    ).any():
        raise AssertionError(
            "Monthly panel contains duplicate "
            "geo/date rows"
        )

    if not panel[
        "major_assignment_changed"
    ].any():
        raise AssertionError(
            "No major assignment changes were found"
        )

    if recent_major.empty:
        raise AssertionError(
            "Expected at least one 2023–2026 "
            "major assignment change"
        )

    expected_table_count = 4

    if len(
        output_paths["tables"]
    ) != expected_table_count:
        raise AssertionError(
            "Unexpected number of CSV outputs"
        )

    expected_plot_count = (
        len(FOCUS_GEOS) * 4
    )

    if len(
        output_paths["plots"]
    ) != expected_plot_count:
        raise AssertionError(
            "Unexpected number of plot outputs"
        )

    for path in [
        *output_paths["tables"],
        *output_paths["plots"],
    ]:
        if not Path(
            path
        ).exists():
            raise AssertionError(
                f"Expected output file was not "
                f"created: {path}"
            )

    print(
        "\n[inventory_chronology] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
