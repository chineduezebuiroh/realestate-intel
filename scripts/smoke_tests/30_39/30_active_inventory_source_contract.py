from __future__ import annotations
# scripts/smoke_tests/30_39/30_active_inventory_source_contract.py

from typing import Any

import pandas as pd

from regime.artifacts import (
    RegimeArtifactStore,
)


RUN_ID = "macro_regime_v1_bps120"

TARGET_METRIC = "active_inventory"

METRIC_KEY_COLUMNS = (
    "canonical_metric_key",
    "metric_key",
    "source_metric_key",
)

FEATURE_KEY_COLUMNS = (
    "feature_key",
)

VALUE_COLUMN_CANDIDATES = (
    "raw_value",
    "metric_value",
    "value",
    "observation_value",
    "source_value",
    "raw_feature_value",
    "normalized_feature_value",
    "metric_score",
)

DATE_COLUMN_CANDIDATES = (
    "date",
    "observation_date",
    "metric_date",
    "evaluation_date",
    "source_observation_date",
)


def _artifact_keys(
    manifest: dict[str, Any],
) -> list[str]:
    artifacts = manifest.get(
        "artifacts",
        {}
    )

    if not isinstance(
        artifacts,
        dict,
    ):
        raise ValueError(
            "Manifest artifacts must be "
            "stored as a dictionary"
        )

    return sorted(
        artifacts.keys()
    )


def _matching_rows(
    frame: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    str | None,
    str | None,
]:
    """
    Find active-inventory rows using either a canonical metric key
    or a feature-key prefix.

    Returns:
        matching rows
        matched key column
        match type
    """
    for column in METRIC_KEY_COLUMNS:
        if column not in frame.columns:
            continue

        values = (
            frame[column]
            .astype(str)
            .str.strip()
        )

        mask = values.eq(
            TARGET_METRIC
        )

        if mask.any():
            return (
                frame.loc[
                    mask
                ].copy(),
                column,
                "metric_key",
            )

    for column in FEATURE_KEY_COLUMNS:
        if column not in frame.columns:
            continue

        values = (
            frame[column]
            .astype(str)
            .str.strip()
        )

        mask = values.str.startswith(
            f"{TARGET_METRIC}_",
            na=False,
        )

        if mask.any():
            return (
                frame.loc[
                    mask
                ].copy(),
                column,
                "feature_key",
            )

    return (
        pd.DataFrame(),
        None,
        None,
    )


def _available_candidates(
    frame: pd.DataFrame,
    candidates: tuple[str, ...],
) -> list[str]:
    return [
        column
        for column in candidates
        if column in frame.columns
    ]


def _sample_columns(
    frame: pd.DataFrame,
    *,
    matched_key_column: str | None,
) -> list[str]:
    columns: list[str] = []

    for column in (
        "geo_id",
        matched_key_column,
        *_available_candidates(
            frame,
            DATE_COLUMN_CANDIDATES,
        ),
        *_available_candidates(
            frame,
            VALUE_COLUMN_CANDIDATES,
        ),
        "feature_component",
        "source_family",
        "source_geography",
        "source_geo_id",
        "carry_forward_status",
    ):
        if (
            column
            and column in frame.columns
            and column not in columns
        ):
            columns.append(column)

    return columns


def main() -> int:
    store = RegimeArtifactStore()

    manifest = store.read_manifest(
        RUN_ID
    )

    if (
        manifest.get("status")
        != "complete"
    ):
        raise AssertionError(
            f"Run {RUN_ID!r} is not complete"
        )

    artifact_keys = _artifact_keys(
        manifest
    )

    print(
        "[active_inventory_contract] run:",
        RUN_ID,
    )

    print(
        "[active_inventory_contract] "
        "artifacts:",
        len(artifact_keys),
    )

    findings: list[
        dict[str, object]
    ] = []

    for artifact_key in artifact_keys:
        metadata = manifest[
            "artifacts"
        ][artifact_key]

        artifact_format = str(
            metadata.get(
                "format",
                "",
            )
        ).lower()

        if artifact_format not in {
            "parquet",
            "",
        }:
            continue

        try:
            frame = store.read_dataframe(
                RUN_ID,
                artifact_key,
            )
        except Exception as exc:
            print(
                "\n[active_inventory_contract] "
                f"SKIP {artifact_key}: "
                f"{type(exc).__name__}: {exc}"
            )
            continue

        (
            matched,
            key_column,
            match_type,
        ) = _matching_rows(
            frame
        )

        if matched.empty:
            continue

        value_columns = (
            _available_candidates(
                matched,
                VALUE_COLUMN_CANDIDATES,
            )
        )

        date_columns = (
            _available_candidates(
                matched,
                DATE_COLUMN_CANDIDATES,
            )
        )

        feature_keys = []

        if "feature_key" in matched.columns:
            feature_keys = sorted(
                matched[
                    "feature_key"
                ]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )

        finding = {
            "artifact_key": artifact_key,
            "match_type": match_type,
            "matched_key_column": (
                key_column
            ),
            "rows": len(matched),
            "geographies": (
                matched["geo_id"].nunique()
                if "geo_id"
                in matched.columns
                else None
            ),
            "date_columns": (
                ",".join(date_columns)
            ),
            "value_columns": (
                ",".join(value_columns)
            ),
            "feature_keys": (
                ",".join(feature_keys)
            ),
            "total_columns": len(
                frame.columns
            ),
        }

        findings.append(finding)

        print(
            "\n"
            + "=" * 88
        )

        print(
            "[active_inventory_contract] "
            f"artifact: {artifact_key}"
        )

        print(
            "[active_inventory_contract] "
            f"match type: {match_type}"
        )

        print(
            "[active_inventory_contract] "
            f"key column: {key_column}"
        )

        print(
            "[active_inventory_contract] "
            f"rows: {len(matched)}"
        )

        print(
            "[active_inventory_contract] "
            f"columns: "
            f"{list(frame.columns)}"
        )

        if feature_keys:
            print(
                "[active_inventory_contract] "
                "feature keys:"
            )

            for feature_key in feature_keys:
                print(
                    "  ",
                    feature_key,
                )

        sample_columns = (
            _sample_columns(
                matched,
                matched_key_column=(
                    key_column
                ),
            )
        )

        sample = matched[
            sample_columns
        ].copy()

        sort_columns = [
            column
            for column in (
                "geo_id",
                "date",
                "observation_date",
                "metric_date",
                "evaluation_date",
                "feature_key",
            )
            if column in sample.columns
        ]

        if sort_columns:
            sample = sample.sort_values(
                sort_columns
            )

        focus_sample = sample

        if "geo_id" in sample.columns:
            focus_sample = sample[
                sample["geo_id"].isin(
                    [
                        (
                            "district_of_"
                            "columbia_dc__county"
                        ),
                        (
                            "alameda_county_"
                            "ca__county"
                        ),
                    ]
                )
            ]

        print(
            "\n[active_inventory_contract] "
            "sample:"
        )

        print(
            focus_sample.tail(
                20
            ).to_string(
                index=False
            )
        )

    if not findings:
        raise AssertionError(
            "No persisted artifact contains "
            "active_inventory rows"
        )

    summary = pd.DataFrame(
        findings
    )

    print(
        "\n"
        + "=" * 88
    )

    print(
        "[active_inventory_contract] "
        "candidate artifact summary:"
    )

    print(
        summary.to_string(
            index=False
        )
    )

    raw_candidates = summary[
        summary[
            "match_type"
        ].eq("metric_key")
        & summary[
            "value_columns"
        ].str.contains(
            (
                "raw_value|metric_value|"
                "observation_value|"
                "source_value|value"
            ),
            regex=True,
            na=False,
        )
    ]

    print(
        "\n[active_inventory_contract] "
        "likely raw-observation candidates:"
    )

    if raw_candidates.empty:
        print(
            "NONE FOUND"
        )
    else:
        print(
            raw_candidates.to_string(
                index=False
            )
        )

    if len(
        raw_candidates
    ) > 1:
        print(
            "\n[active_inventory_contract] "
            "WARNING: multiple possible raw "
            "observation artifacts were found. "
            "Inspect grain and lineage before "
            "selecting one."
        )

    print(
        "\n[active_inventory_contract] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
