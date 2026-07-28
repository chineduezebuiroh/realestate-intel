from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import pandas as pd

from .validation import (
    assert_no_duplicate_keys,
    assert_non_empty,
    assert_required_columns,
)
from .models import ReviewGeographySelection


DEFAULT_MANDATORY_REVIEW_GEOS = ("district_of_columbia_dc__county",)

ALLOWED_SELECTION_REASONS = frozenset(
    {
        "mandatory",
        "highest_candidate_delta",
        "highest_volatility",
        "lowest_volatility",
        "highest_transition_change",
        "largest_shock_divergence",
        "largest_seasonality_change",
        "coverage_exception",
        "manual_selection",
    }
)


@dataclass(frozen=True)
class GeographySelectionPolicy:
    mandatory_geo_ids: tuple[str, ...] = DEFAULT_MANDATORY_REVIEW_GEOS
    targeted_geo_types: tuple[str, ...] = ("county",)
    context_geo_types: tuple[str, ...] = ("cbsa_metro",)
    max_automatic_geographies: int = 6
    allow_explicit_non_target_types: bool = True
    include_context_geographies: bool = True
    deterministic_sort: bool = True
    persist_selection_rationale: bool = True

    def __post_init__(self) -> None:
        if self.max_automatic_geographies < 0:
            raise ValueError("max_automatic_geographies must be non-negative")


def _validate_candidates(candidates: pd.DataFrame) -> None:
    assert_non_empty(candidates, frame_name="geography_candidates")
    assert_required_columns(
        candidates,
        ("geo_id", "geo_type", "selection_reason"),
        frame_name="geography_candidates",
    )
    invalid_reasons = sorted(
        set(candidates["selection_reason"].dropna()).difference(
            ALLOWED_SELECTION_REASONS
        )
    )
    if invalid_reasons:
        raise ValueError(f"Unsupported selection reasons: {invalid_reasons}")


def _build_selection_rationale(
    selected: pd.DataFrame,
) -> pd.DataFrame:

    rationale = selected[
        [
            "geo_id",
            "geo_type",
            "selection_reason",
            "selected_automatically",
            "selected_manually",
        ]
    ].copy()

    rationale["selection_scope"] = rationale["selected_automatically"].map(
        lambda x: "targeted" if x else "manual"
    )

    rationale.loc[
        rationale["selection_reason"] == "mandatory",
        "selection_scope",
    ] = "mandatory"

    return rationale[
        [
            "geo_id",
            "geo_type",
            "selection_scope",
            "selection_reason",
            "selected_automatically",
            "selected_manually",
        ]
    ]


def select_review_geographies(
    candidates: pd.DataFrame,
    *,
    policy: GeographySelectionPolicy | None = None,
    manual_geo_ids: Sequence[str] = (),
) -> pd.DataFrame:
    active_policy = policy or GeographySelectionPolicy()
    _validate_candidates(candidates)
    working = candidates.copy()

    for column in ("selection_rank", "selection_metric", "selection_value"):
        if column not in working:
            working[column] = pd.NA

    mandatory = working[
        working["geo_id"].isin(active_policy.mandatory_geo_ids)
    ].copy()
    missing_mandatory = sorted(
        set(active_policy.mandatory_geo_ids).difference(mandatory["geo_id"])
    )
    if missing_mandatory:
        raise AssertionError(
            "Mandatory review geographies are missing from candidates: "
            f"{missing_mandatory}"
        )
    mandatory["selection_reason"] = "mandatory"
    mandatory["selected_automatically"] = False
    mandatory["selected_manually"] = False

    automatic = working[
        working["geo_type"].isin(active_policy.targeted_geo_types)
        & ~working["geo_id"].isin(active_policy.mandatory_geo_ids)
        & ~working["geo_id"].isin(manual_geo_ids)
    ].copy()
    automatic["_sort_rank"] = pd.to_numeric(
        automatic["selection_rank"], errors="coerce"
    ).fillna(float("inf"))
    automatic["_sort_value"] = pd.to_numeric(
        automatic["selection_value"], errors="coerce"
    ).fillna(float("-inf"))
    automatic = (
        automatic.sort_values(
            ["_sort_rank", "_sort_value", "geo_id"],
            ascending=[True, False, True],
            kind="mergesort",
        )
        .drop_duplicates(subset=["geo_id"], keep="first")
        .head(active_policy.max_automatic_geographies)
        .drop(columns=["_sort_rank", "_sort_value"])
    )
    automatic["selected_automatically"] = True
    automatic["selected_manually"] = False

    manual = working[
        working["geo_id"].isin(manual_geo_ids)
        & ~working["geo_id"].isin(active_policy.mandatory_geo_ids)
    ].copy()
    missing_manual = sorted(set(manual_geo_ids).difference(manual["geo_id"]))
    if missing_manual:
        raise AssertionError(
            "Manual review geographies are missing from candidates: "
            f"{missing_manual}"
        )
    manual = manual.drop_duplicates(subset=["geo_id"], keep="first")
    manual["selection_reason"] = "manual_selection"
    manual["selected_automatically"] = False
    manual["selected_manually"] = True

    selected = pd.concat(
        [mandatory, automatic, manual],
        ignore_index=True,
        sort=False,
    ).drop_duplicates(subset=["geo_id"], keep="first")
    selected["selection_order"] = range(1, len(selected) + 1)

    output_columns = [
        "geo_id",
        "geo_type",
        "selection_reason",
        "selection_order",
        "selection_rank",
        "selection_metric",
        "selection_value",
        "selected_automatically",
        "selected_manually",
    ]
    selected = selected[output_columns].sort_values(
        "selection_order", kind="mergesort"
    )
    assert_no_duplicate_keys(
        selected,
        ("geo_id",),
        frame_name="selected_review_geographies",
    )

    automatic_types = set(
        selected.loc[selected["selected_automatically"], "geo_type"]
    )
    invalid_automatic_types = sorted(
        automatic_types.difference(active_policy.targeted_geo_types)
    )
    if invalid_automatic_types:
        raise AssertionError(
            "Automatic targeted selection contains unsupported geography types: "
            f"{invalid_automatic_types}"
        )

    selected = selected.reset_index(drop=True)

    rationale = _build_selection_rationale(selected)

    return ReviewGeographySelection(
        selected_geographies=selected,
        targeted_geographies=selected[
            selected.selected_automatically
        ].copy(),
        aggregate_geographies=context_review_geographies(
            candidates,
            policy=active_policy,
        ),
        rationale=rationale,
        metadata={
            "mandatory_geo_ids": list(
                active_policy.mandatory_geo_ids
            ),
            "targeted_geo_types": list(
                active_policy.targeted_geo_types
            ),
            "context_geo_types": list(
                active_policy.context_geo_types
            ),
        },
    )


def context_review_geographies(
    frame: pd.DataFrame,
    *,
    policy: GeographySelectionPolicy | None = None,
    geo_type_column: str = "geo_type",
) -> pd.DataFrame:
    active_policy = policy or GeographySelectionPolicy()
    assert_required_columns(
        frame,
        (geo_type_column,),
        frame_name="aggregate_review_input",
    )
    return frame[
        frame[geo_type_column].isin(active_policy.context_geo_types)
    ].copy()
