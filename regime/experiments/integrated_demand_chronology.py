from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


# =============================================================================
# Contract
# =============================================================================

AXIS_KEY = "demand"

EXPECTED_DIMENSIONS = (
    "demand",
    "price",
    "affordability",
    "capital_markets",
)

PRICE_FAMILY_DIMENSIONS = (
    "price",
    "affordability",
)

INCUMBENT_DIMENSIONS = (
    "demand",
    "capital_markets",
)

PRODUCTION_PRICE_FAMILY_CANDIDATE_ID = (
    "price_family_ma12_structural_linked"
)

PRODUCTION_PRICE_FAMILY_LABEL = (
    "MA12_PRICE_AFFORDABILITY"
)

AXIS_REGISTRY_PATH = Path("config/axis_registry.csv")

PRICE_FAMILY_CHRONOLOGY_PATH = Path(
    "artifacts/regime/comparisons/"
    "price_family_structural_windows/"
    f"{PRODUCTION_PRICE_FAMILY_CANDIDATE_ID}/"
    "phase2_chronology/"
    "chronology_monthly.csv"
)

OUTPUT_DIR = Path(
    "artifacts/regime/review_exports/"
    "integrated_demand_chronology"
)

DISCOVERY_ROOTS = (
    Path("artifacts/regime/runs"),
    Path("artifacts/regime/archives"),
    Path("artifacts/regime/comparisons"),
)

EXCLUDED_DISCOVERY_PATH_PARTS = {
    "review_exports",
    "demand_axis_attribution",
    "integrated_demand_chronology",
    "phase2_chronology",
}

MIN_COMMON_MONTHS = 24

RECONSTRUCTION_WARN_TOLERANCE = 1e-8
RECONSTRUCTION_FAIL_TOLERANCE = 1e-5

PRODUCTION_RUN_ID = "macro_regime_v1_bps120_sources"

PRODUCTION_RUN_DIR = (
    Path("artifacts/regime/runs")
    / PRODUCTION_RUN_ID
)

INCUMBENT_DIMENSION_SCORES_PATH = (
    PRODUCTION_RUN_DIR
    / "dimension_scores.parquet"
)

INCUMBENT_AXIS_SCORES_PATH = (
    PRODUCTION_RUN_DIR
    / "axis_scores.parquet"
)

PRODUCTION_RUN_MANIFEST_PATH = (
    PRODUCTION_RUN_DIR
    / "manifest.json"
)


# =============================================================================
# Data structures
# =============================================================================

@dataclass(frozen=True)
class DimensionSourceCandidate:
    path: Path
    frame: pd.DataFrame
    dimensions: tuple[str, ...]
    geographies: int
    rows: int
    first_date: pd.Timestamp
    last_date: pd.Timestamp
    complete_geo_months: int
    common_geo_months_with_price_family: int
    path_preference: int

    @property
    def score(self) -> tuple[int, int, int, int, int]:
        """
        Deterministic ranking.

        Priority:
        1. contains all required dimensions;
        2. overlap with the selected Price/Affordability chronology;
        3. complete geography-month coverage;
        4. preferred artifact location;
        5. most recent end date.
        """
        all_dimensions = int(
            set(EXPECTED_DIMENSIONS).issubset(self.dimensions)
        )

        end_ordinal = int(self.last_date.toordinal())

        return (
            all_dimensions,
            self.common_geo_months_with_price_family,
            self.complete_geo_months,
            self.path_preference,
            end_ordinal,
        )


# =============================================================================
# Generic helpers
# =============================================================================

def _json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")

    if isinstance(value, Path):
        return str(value)

    if hasattr(value, "item"):
        return value.item()

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    return value


def _write_json(payload: dict[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            default=_json_default,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    output = frame.copy()

    for column in output.columns:
        if pd.api.types.is_datetime64_any_dtype(
            output[column]
        ):
            output[column] = output[column].dt.strftime(
                "%Y-%m-%d"
            )

    output.to_csv(path, index=False)


def _normalize_boolean(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .isin(
            {
                "true",
                "1",
                "yes",
                "y",
            }
        )
    )


def _first_existing_column(
    columns: Iterable[str],
    candidates: Iterable[str],
) -> str | None:
    available = set(columns)

    for candidate in candidates:
        if candidate in available:
            return candidate

    return None


def _month_end(series: pd.Series) -> pd.Series:
    dates = pd.to_datetime(
        series,
        errors="coerce",
    )

    return (
        dates.dt.to_period("M")
        .dt.to_timestamp("M")
    )


def _path_preference(path: Path) -> int:
    text = str(path)

    if "artifacts/regime/runs" in text:
        return 3

    if "artifacts/regime/archives" in text:
        return 2

    if "artifacts/regime/comparisons" in text:
        return 1

    return 0


# =============================================================================
# Registry
# =============================================================================

def load_axis_weights(
    path: Path = AXIS_REGISTRY_PATH,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing axis registry: {path}"
        )

    registry = pd.read_csv(path)

    required = {
        "axis",
        "dimension",
        "dimension_weight",
        "enabled",
    }

    missing = required - set(registry.columns)

    if missing:
        raise ValueError(
            f"{path} is missing required columns: "
            f"{sorted(missing)}"
        )

    enabled = _normalize_boolean(
        registry["enabled"]
    )

    weights = registry[
        registry["axis"].astype(str).eq(AXIS_KEY)
        & enabled
    ][
        [
            "axis",
            "dimension",
            "dimension_weight",
        ]
    ].copy()

    weights["dimension"] = (
        weights["dimension"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    weights["dimension_weight"] = pd.to_numeric(
        weights["dimension_weight"],
        errors="raise",
    )

    if weights["dimension"].duplicated().any():
        duplicates = (
            weights.loc[
                weights["dimension"].duplicated(
                    keep=False
                ),
                "dimension",
            ]
            .unique()
            .tolist()
        )

        raise ValueError(
            "Duplicate enabled Demand-axis dimensions: "
            f"{sorted(duplicates)}"
        )

    missing_dimensions = (
        set(EXPECTED_DIMENSIONS)
        - set(weights["dimension"])
    )

    if missing_dimensions:
        raise ValueError(
            "Demand-axis registry is missing dimensions: "
            f"{sorted(missing_dimensions)}"
        )

    total_weight = float(
        weights["dimension_weight"].sum()
    )

    if not np.isclose(total_weight, 1.0):
        raise ValueError(
            "Enabled Demand-axis weights must sum to 1.0; "
            f"found {total_weight:.12f}"
        )

    return (
        weights[
            weights["dimension"].isin(
                EXPECTED_DIMENSIONS
            )
        ]
        .sort_values("dimension")
        .reset_index(drop=True)
    )


# =============================================================================
# Price / Affordability challenger
# =============================================================================

def load_price_family_challenger(
    path: Path = PRICE_FAMILY_CHRONOLOGY_PATH,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            "Missing selected Price/Affordability chronology: "
            f"{path}"
        )

    frame = pd.read_csv(path)

    required = {
        "geo_id",
        "date",
        "series_type",
        "series_key",
        "challenger_metric_score",
    }

    missing = required - set(frame.columns)

    if missing:
        raise ValueError(
            f"{path} is missing required columns: "
            f"{sorted(missing)}"
        )

    frame["date"] = _month_end(frame["date"])

    frame["series_key"] = (
        frame["series_key"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    frame["challenger_metric_score"] = pd.to_numeric(
        frame["challenger_metric_score"],
        errors="coerce",
    )

    selected = frame[
        frame["series_type"].eq("dimension_score")
        & frame["series_key"].isin(
            PRICE_FAMILY_DIMENSIONS
        )
    ][
        [
            "geo_id",
            "date",
            "series_key",
            "challenger_metric_score",
        ]
    ].copy()

    selected = selected.rename(
        columns={
            "series_key": "dimension",
            "challenger_metric_score":
                "dimension_score",
        }
    )

    selected["source_role"] = (
        "ma12_price_family_challenger"
    )

    selected["source_candidate_id"] = (
        PRODUCTION_PRICE_FAMILY_CANDIDATE_ID
    )

    selected["source_path"] = str(path)

    selected = selected.dropna(
        subset=[
            "geo_id",
            "date",
            "dimension",
            "dimension_score",
        ]
    )

    duplicates = selected.duplicated(
        subset=[
            "geo_id",
            "date",
            "dimension",
        ],
        keep=False,
    )

    if duplicates.any():
        duplicate_rows = selected.loc[
            duplicates,
            [
                "geo_id",
                "date",
                "dimension",
            ],
        ]

        raise ValueError(
            "Selected Price/Affordability chronology "
            "contains duplicate geo/date/dimension rows:\n"
            f"{duplicate_rows.head(20).to_string(index=False)}"
        )

    found_dimensions = set(
        selected["dimension"].unique()
    )

    missing_dimensions = (
        set(PRICE_FAMILY_DIMENSIONS)
        - found_dimensions
    )

    if missing_dimensions:
        raise ValueError(
            "Selected Price/Affordability chronology is "
            "missing dimensions: "
            f"{sorted(missing_dimensions)}"
        )

    return selected.sort_values(
        [
            "geo_id",
            "date",
            "dimension",
        ],
        kind="mergesort",
    ).reset_index(drop=True)


# =============================================================================
# Incumbent dimension artifact normalization
# =============================================================================

def normalize_dimension_artifact(
    path: Path,
) -> pd.DataFrame | None:
    """
    Attempt to normalize a CSV into:

        geo_id
        date
        dimension
        dimension_score

    The function intentionally accepts several historical schema aliases.
    Files that do not resemble dimension-score history return None.
    """
    try:
        frame = pd.read_csv(
            path,
            nrows=None,
            low_memory=False,
        )
    except Exception:
        return None

    if frame.empty:
        return None

    geo_column = _first_existing_column(
        frame.columns,
        (
            "geo_id",
            "geography_id",
            "geo",
        ),
    )

    date_column = _first_existing_column(
        frame.columns,
        (
            "date",
            "period_end",
            "month_end",
            "observation_date",
        ),
    )

    dimension_column = _first_existing_column(
        frame.columns,
        (
            "dimension",
            "dimension_key",
            "series_key",
        ),
    )

    score_column = _first_existing_column(
        frame.columns,
        (
            "dimension_score",
            "challenger_dimension_score",
            "baseline_dimension_score",
            "challenger_metric_score",
            "score",
            "normalized_score",
            "value",
        ),
    )

    if (
        geo_column is None
        or date_column is None
        or dimension_column is None
        or score_column is None
    ):
        return None

    selected = frame[
        [
            geo_column,
            date_column,
            dimension_column,
            score_column,
        ]
    ].copy()

    selected.columns = [
        "geo_id",
        "date",
        "dimension",
        "dimension_score",
    ]

    selected["date"] = _month_end(
        selected["date"]
    )

    selected["dimension"] = (
        selected["dimension"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    selected["dimension_score"] = pd.to_numeric(
        selected["dimension_score"],
        errors="coerce",
    )

    # chronology_monthly.csv contains metrics, dimensions, and axes.
    # Only dimension rows should survive.
    if "series_type" in frame.columns:
        series_type = (
            frame["series_type"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        selected = selected.loc[
            series_type.eq("dimension_score")
            .to_numpy()
        ].copy()

    selected = selected[
        selected["dimension"].isin(
            EXPECTED_DIMENSIONS
        )
    ]

    selected = selected.dropna(
        subset=[
            "geo_id",
            "date",
            "dimension",
            "dimension_score",
        ]
    )

    if selected.empty:
        return None

    # Reject artifacts that only contain the Price family.
    # Phase D1 needs incumbent Demand and Capital Markets.
    if not set(INCUMBENT_DIMENSIONS).issubset(
        set(selected["dimension"])
    ):
        return None

    selected = selected.sort_values(
        [
            "geo_id",
            "date",
            "dimension",
        ],
        kind="mergesort",
    )

    # Some artifacts may contain repeated records from multiple runs.
    # A source with conflicting duplicate values is unsafe.
    duplicate_groups = (
        selected.groupby(
            [
                "geo_id",
                "date",
                "dimension",
            ],
            dropna=False,
        )["dimension_score"]
        .nunique(dropna=True)
    )

    conflicts = duplicate_groups[
        duplicate_groups.gt(1)
    ]

    if not conflicts.empty:
        return None

    selected = selected.drop_duplicates(
        subset=[
            "geo_id",
            "date",
            "dimension",
        ],
        keep="last",
    )

    selected["source_role"] = (
        "incumbent_dimension_history"
    )

    selected["source_candidate_id"] = (
        "incumbent"
    )

    selected["source_path"] = str(path)

    return selected.reset_index(drop=True)


def load_incumbent_dimension_history(
    path: Path = INCUMBENT_DIMENSION_SCORES_PATH,
) -> pd.DataFrame:
    """
    Load the canonical incumbent dimension-score history.

    Required normalized contract:

        geo_id
        date
        dimension
        dimension_score

    Additional source identity fields are retained for lineage.
    """
    override = os.environ.get(
        "INTEGRATED_DEMAND_DIMENSION_SOURCE"
    )

    if override:
        path = Path(override)

    if not path.exists():
        raise FileNotFoundError(
            "Missing incumbent dimension-score artifact: "
            f"{path}"
        )

    print(
        "[integrated_demand_chronology] "
        f"loading incumbent dimensions from {path}"
    )

    if path.suffix.lower() in {
        ".parquet",
        ".pq",
    }:
        frame = pd.read_parquet(path)

    elif path.suffix.lower() == ".csv":
        frame = pd.read_csv(
            path,
            low_memory=False,
        )

    else:
        raise ValueError(
            "Unsupported incumbent dimension artifact "
            f"format: {path.suffix}"
        )

    if frame.empty:
        raise ValueError(
            f"Incumbent dimension artifact is empty: {path}"
        )

    print(
        "Incumbent dimension artifact schema:"
    )

    for column in frame.columns:
        print(
            f"  {column}: "
            f"{frame[column].dtype}"
        )

    geo_column = _first_existing_column(
        frame.columns,
        (
            "geo_id",
            "geography_id",
            "geo",
        ),
    )

    date_column = _first_existing_column(
        frame.columns,
        (
            "date",
            "period_end",
            "month_end",
            "observation_date",
            "as_of_date",
        ),
    )

    dimension_column = _first_existing_column(
        frame.columns,
        (
            "dimension",
            "dimension_key",
            "dimension_id",
            "series_key",
        ),
    )

    score_column = _first_existing_column(
        frame.columns,
        (
            "dimension_score",
            "score",
            "normalized_score",
            "value",
        ),
    )

    resolved_columns = {
        "geo_id": geo_column,
        "date": date_column,
        "dimension": dimension_column,
        "dimension_score": score_column,
    }

    unresolved = [
        normalized_name
        for normalized_name, source_name
        in resolved_columns.items()
        if source_name is None
    ]

    if unresolved:
        raise ValueError(
            "Could not normalize incumbent dimension "
            "artifact. Missing logical fields: "
            f"{unresolved}. "
            f"Available columns: {list(frame.columns)}"
        )

    selected = frame[
        [
            geo_column,
            date_column,
            dimension_column,
            score_column,
        ]
    ].copy()

    selected.columns = [
        "geo_id",
        "date",
        "dimension",
        "dimension_score",
    ]

    selected["geo_id"] = (
        selected["geo_id"]
        .astype(str)
        .str.strip()
    )

    selected["date"] = _month_end(
        selected["date"]
    )

    selected["dimension"] = (
        selected["dimension"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    selected["dimension_score"] = pd.to_numeric(
        selected["dimension_score"],
        errors="coerce",
    )

    selected = selected[
        selected["dimension"].isin(
            EXPECTED_DIMENSIONS
        )
    ].copy()

    selected = selected.dropna(
        subset=[
            "geo_id",
            "date",
            "dimension",
            "dimension_score",
        ]
    )

    if selected.empty:
        raise ValueError(
            "No usable Demand-axis dimension rows were "
            f"found in {path}"
        )

    found_dimensions = set(
        selected["dimension"].unique()
    )

    missing_dimensions = (
        set(EXPECTED_DIMENSIONS)
        - found_dimensions
    )

    if missing_dimensions:
        raise ValueError(
            "Canonical incumbent dimension history is "
            "missing expected dimensions: "
            f"{sorted(missing_dimensions)}. "
            "Found dimensions: "
            f"{sorted(found_dimensions)}"
        )

    duplicate_counts = (
        selected.groupby(
            [
                "geo_id",
                "date",
                "dimension",
            ],
            dropna=False,
        )["dimension_score"]
        .nunique(dropna=True)
    )

    conflicts = duplicate_counts[
        duplicate_counts.gt(1)
    ]

    if not conflicts.empty:
        conflict_keys = (
            conflicts.reset_index()
            .head(30)
        )

        raise ValueError(
            "Conflicting duplicate incumbent dimension "
            "scores were found:\n"
            f"{conflict_keys.to_string(index=False)}"
        )

    selected = selected.drop_duplicates(
        subset=[
            "geo_id",
            "date",
            "dimension",
        ],
        keep="last",
    )

    selected["source_role"] = (
        "incumbent_dimension_history"
    )

    selected["source_candidate_id"] = (
        PRODUCTION_RUN_ID
    )

    selected["source_path"] = str(path)

    selected = selected.sort_values(
        [
            "geo_id",
            "date",
            "dimension",
        ],
        kind="mergesort",
    ).reset_index(drop=True)

    print(
        "Incumbent dimension history:"
    )
    print(
        f"  rows={len(selected):,}, "
        f"geographies="
        f"{selected['geo_id'].nunique():,}, "
        f"first_date="
        f"{selected['date'].min().date()}, "
        f"last_date="
        f"{selected['date'].max().date()}"
    )

    print(
        "  dimensions="
        + ", ".join(
            sorted(
                selected["dimension"].unique()
            )
        )
    )

    return selected
    

def discover_dimension_artifacts(
    price_family: pd.DataFrame,
) -> list[DimensionSourceCandidate]:
    override = os.environ.get(
        "INTEGRATED_DEMAND_DIMENSION_SOURCE"
    )

    if override:
        candidate_paths = [Path(override)]
    else:
        candidate_paths = []

        for root in DISCOVERY_ROOTS:
            if not root.exists():
                continue

            for path in root.rglob("*.csv"):
                if any(
                    part in EXCLUDED_DISCOVERY_PATH_PARTS
                    for part in path.parts
                ):
                    continue

                candidate_paths.append(path)

    price_keys = set(
        zip(
            price_family["geo_id"],
            price_family["date"],
        )
    )

    candidates: list[DimensionSourceCandidate] = []

    for path in sorted(set(candidate_paths)):
        normalized = normalize_dimension_artifact(path)

        if normalized is None or normalized.empty:
            continue

        dimensions = tuple(
            sorted(
                normalized["dimension"]
                .dropna()
                .unique()
                .tolist()
            )
        )

        complete_counts = (
            normalized.groupby(
                [
                    "geo_id",
                    "date",
                ],
                dropna=False,
            )["dimension"]
            .nunique()
        )

        complete_geo_months = int(
            complete_counts.ge(
                len(EXPECTED_DIMENSIONS)
            ).sum()
        )

        source_keys = set(
            zip(
                normalized["geo_id"],
                normalized["date"],
            )
        )

        common_geo_months = len(
            source_keys & price_keys
        )

        candidates.append(
            DimensionSourceCandidate(
                path=path,
                frame=normalized,
                dimensions=dimensions,
                geographies=int(
                    normalized["geo_id"].nunique()
                ),
                rows=int(len(normalized)),
                first_date=normalized["date"].min(),
                last_date=normalized["date"].max(),
                complete_geo_months=complete_geo_months,
                common_geo_months_with_price_family=(
                    common_geo_months
                ),
                path_preference=_path_preference(path),
            )
        )

    return sorted(
        candidates,
        key=lambda candidate: candidate.score,
        reverse=True,
    )


def candidate_inventory(
    candidates: list[DimensionSourceCandidate],
) -> pd.DataFrame:
    rows = []

    for rank, candidate in enumerate(
        candidates,
        start=1,
    ):
        rows.append(
            {
                "rank": rank,
                "path": str(candidate.path),
                "dimensions": "|".join(
                    candidate.dimensions
                ),
                "contains_all_expected_dimensions": (
                    set(EXPECTED_DIMENSIONS)
                    .issubset(candidate.dimensions)
                ),
                "geographies": candidate.geographies,
                "rows": candidate.rows,
                "first_date": candidate.first_date,
                "last_date": candidate.last_date,
                "complete_geo_months":
                    candidate.complete_geo_months,
                "common_geo_months_with_price_family":
                    candidate
                    .common_geo_months_with_price_family,
                "path_preference":
                    candidate.path_preference,
                "selection_score": str(
                    candidate.score
                ),
            }
        )

    return pd.DataFrame(rows)


def load_incumbent_axis_history(
    path: Path = INCUMBENT_AXIS_SCORES_PATH,
) -> pd.DataFrame:
    """
    Load the incumbent reported Demand-axis score for
    comparison with the newly integrated axis.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Missing incumbent axis artifact: {path}"
        )

    frame = pd.read_parquet(path)

    if frame.empty:
        raise ValueError(
            f"Incumbent axis artifact is empty: {path}"
        )

    geo_column = _first_existing_column(
        frame.columns,
        (
            "geo_id",
            "geography_id",
            "geo",
        ),
    )

    date_column = _first_existing_column(
        frame.columns,
        (
            "date",
            "period_end",
            "month_end",
            "observation_date",
            "as_of_date",
        ),
    )

    axis_column = _first_existing_column(
        frame.columns,
        (
            "axis",
            "axis_key",
            "axis_id",
            "series_key",
        ),
    )

    score_column = _first_existing_column(
        frame.columns,
        (
            "axis_score",
            "score",
            "normalized_score",
            "value",
        ),
    )

    required = {
        "geo_id": geo_column,
        "date": date_column,
        "axis": axis_column,
        "axis_score": score_column,
    }

    unresolved = [
        logical_name
        for logical_name, source_name
        in required.items()
        if source_name is None
    ]

    if unresolved:
        raise ValueError(
            "Could not normalize incumbent axis artifact. "
            f"Missing logical fields: {unresolved}. "
            f"Available columns: {list(frame.columns)}"
        )

    selected = frame[
        [
            geo_column,
            date_column,
            axis_column,
            score_column,
        ]
    ].copy()

    selected.columns = [
        "geo_id",
        "date",
        "axis",
        "incumbent_axis_score",
    ]

    selected["geo_id"] = (
        selected["geo_id"]
        .astype(str)
        .str.strip()
    )

    selected["date"] = _month_end(
        selected["date"]
    )

    selected["axis"] = (
        selected["axis"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    selected["incumbent_axis_score"] = (
        pd.to_numeric(
            selected["incumbent_axis_score"],
            errors="coerce",
        )
    )

    selected = selected[
        selected["axis"].eq(AXIS_KEY)
    ].dropna(
        subset=[
            "geo_id",
            "date",
            "incumbent_axis_score",
        ]
    )

    duplicate_counts = (
        selected.groupby(
            [
                "geo_id",
                "date",
            ]
        )["incumbent_axis_score"]
        .nunique(dropna=True)
    )

    conflicts = duplicate_counts[
        duplicate_counts.gt(1)
    ]

    if not conflicts.empty:
        raise ValueError(
            "Conflicting duplicate incumbent Demand-axis "
            "scores were found."
        )

    selected = selected.drop_duplicates(
        subset=[
            "geo_id",
            "date",
        ],
        keep="last",
    )

    return selected[
        [
            "geo_id",
            "date",
            "incumbent_axis_score",
        ]
    ].sort_values(
        [
            "geo_id",
            "date",
        ]
    ).reset_index(drop=True)
    

def select_incumbent_source(
    candidates: list[DimensionSourceCandidate],
) -> DimensionSourceCandidate:
    if not candidates:
        raise FileNotFoundError(
            "No usable incumbent dimension-score artifact "
            "was found beneath:\n"
            + "\n".join(
                f"  {root}"
                for root in DISCOVERY_ROOTS
            )
            + "\n\nSet INTEGRATED_DEMAND_DIMENSION_SOURCE "
            "to an explicit dimension-score CSV path."
        )

    eligible = [
        candidate
        for candidate in candidates
        if set(EXPECTED_DIMENSIONS).issubset(
            candidate.dimensions
        )
        and (
            candidate
            .common_geo_months_with_price_family
            >= MIN_COMMON_MONTHS
        )
    ]

    if not eligible:
        inventory = candidate_inventory(candidates)

        raise ValueError(
            "Dimension artifacts were found, but none "
            "contained all four Demand-axis dimensions "
            f"with at least {MIN_COMMON_MONTHS} common "
            "geo-months with the selected MA12 chronology.\n\n"
            f"{inventory.head(20).to_string(index=False)}\n\n"
            "Set INTEGRATED_DEMAND_DIMENSION_SOURCE "
            "to an explicit source after reviewing the "
            "candidate inventory."
        )

    return eligible[0]


# =============================================================================
# Integrated chronology
# =============================================================================

def build_integrated_dimension_history(
    incumbent: pd.DataFrame,
    price_family: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the integrated dimension history on the exact
    geography-month universe supported by the selected
    Price/Affordability challenger.

    Demand and Capital Markets come from the incumbent run.
    Price and Affordability come from the MA12 challenger.

    No outer expansion to unrelated incumbent geographies
    is permitted.
    """
    challenger_keys = (
        price_family[
            [
                "geo_id",
                "date",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .reset_index(drop=True)
    )

    incumbent_selected = incumbent[
        incumbent["dimension"].isin(
            INCUMBENT_DIMENSIONS
        )
    ].copy()

    incumbent_selected = incumbent_selected.merge(
        challenger_keys,
        on=[
            "geo_id",
            "date",
        ],
        how="inner",
        validate="many_to_one",
    )

    integrated = pd.concat(
        [
            incumbent_selected,
            price_family,
        ],
        ignore_index=True,
        sort=False,
    )

    integrated["date"] = _month_end(
        integrated["date"]
    )

    duplicates = integrated.duplicated(
        subset=[
            "geo_id",
            "date",
            "dimension",
        ],
        keep=False,
    )

    if duplicates.any():
        duplicate_rows = integrated.loc[
            duplicates,
            [
                "geo_id",
                "date",
                "dimension",
                "source_role",
                "source_path",
            ],
        ].sort_values(
            [
                "geo_id",
                "date",
                "dimension",
            ]
        )

        raise ValueError(
            "Integrated dimension history contains "
            "duplicate geo/date/dimension rows:\n"
            f"{duplicate_rows.head(30).to_string(index=False)}"
        )

    challenger_key_count = len(
        challenger_keys
    )

    integrated_key_count = (
        integrated[
            [
                "geo_id",
                "date",
            ]
        ]
        .drop_duplicates()
        .shape[0]
    )

    if integrated_key_count != challenger_key_count:
        raise ValueError(
            "Integrated chronology geography-month universe "
            "does not match the selected challenger universe: "
            f"challenger={challenger_key_count:,}, "
            f"integrated={integrated_key_count:,}"
        )

    return integrated.sort_values(
        [
            "geo_id",
            "date",
            "dimension",
        ],
        kind="mergesort",
    ).reset_index(drop=True)


def build_incumbent_price_family_wide(
    incumbent_dimensions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Return incumbent Price and Affordability scores in wide form
    for direct comparison with the MA12 replacements.
    """
    selected = incumbent_dimensions[
        incumbent_dimensions["dimension"].isin(
            PRICE_FAMILY_DIMENSIONS
        )
    ][
        [
            "geo_id",
            "date",
            "dimension",
            "dimension_score",
        ]
    ].copy()

    if selected.empty:
        raise ValueError(
            "Incumbent dimension history contains no "
            "Price/Affordability rows."
        )

    duplicate_counts = (
        selected.groupby(
            [
                "geo_id",
                "date",
                "dimension",
            ],
            dropna=False,
        )["dimension_score"]
        .nunique(dropna=True)
    )

    conflicts = duplicate_counts[
        duplicate_counts.gt(1)
    ]

    if not conflicts.empty:
        raise ValueError(
            "Conflicting incumbent Price/Affordability "
            "scores were found."
        )

    selected = selected.drop_duplicates(
        subset=[
            "geo_id",
            "date",
            "dimension",
        ],
        keep="last",
    )

    wide = (
        selected.pivot(
            index=[
                "geo_id",
                "date",
            ],
            columns="dimension",
            values="dimension_score",
        )
        .reset_index()
    )

    wide.columns.name = None

    return wide.rename(
        columns={
            "price": "incumbent_price",
            "affordability":
                "incumbent_affordability",
        }
    )
    

def build_wide_chronology(
    integrated_long: pd.DataFrame,
    weights: pd.DataFrame,
    incumbent_dimensions: pd.DataFrame,
) -> pd.DataFrame:
    score_wide = (
        integrated_long.pivot(
            index=[
                "geo_id",
                "date",
            ],
            columns="dimension",
            values="dimension_score",
        )
        .reset_index()
    )

    score_wide.columns.name = None

    source_role_wide = (
        integrated_long.pivot(
            index=[
                "geo_id",
                "date",
            ],
            columns="dimension",
            values="source_role",
        )
        .reset_index()
    )

    source_role_wide.columns.name = None

    source_role_wide = source_role_wide.rename(
        columns={
            dimension:
                f"{dimension}_source_role"
            for dimension in EXPECTED_DIMENSIONS
            if dimension in source_role_wide.columns
        }
    )

    source_path_wide = (
        integrated_long.pivot(
            index=[
                "geo_id",
                "date",
            ],
            columns="dimension",
            values="source_path",
        )
        .reset_index()
    )

    source_path_wide.columns.name = None

    source_path_wide = source_path_wide.rename(
        columns={
            dimension:
                f"{dimension}_source_path"
            for dimension in EXPECTED_DIMENSIONS
            if dimension in source_path_wide.columns
        }
    )

    wide = score_wide.merge(
        source_role_wide,
        on=[
            "geo_id",
            "date",
        ],
        how="outer",
        validate="one_to_one",
    )

    wide = wide.merge(
        source_path_wide,
        on=[
            "geo_id",
            "date",
        ],
        how="outer",
        validate="one_to_one",
    )

    for dimension in EXPECTED_DIMENSIONS:
        if dimension not in wide.columns:
            wide[dimension] = np.nan

    weight_map = dict(
        zip(
            weights["dimension"],
            weights["dimension_weight"],
        )
    )

    contribution_columns = []

    for dimension in EXPECTED_DIMENSIONS:
        weight = float(weight_map[dimension])

        wide[f"{dimension}_weight"] = weight

        contribution_column = (
            f"{dimension}_weighted_contribution"
        )

        contribution_columns.append(
            contribution_column
        )

        wide[contribution_column] = (
            wide[dimension] * weight
        )

    wide["available_dimension_count"] = (
        wide[list(EXPECTED_DIMENSIONS)]
        .notna()
        .sum(axis=1)
    )

    wide["complete_dimension_coverage"] = (
        wide["available_dimension_count"]
        .eq(len(EXPECTED_DIMENSIONS))
    )

    wide["integrated_demand_axis"] = (
        wide[contribution_columns]
        .sum(
            axis=1,
            min_count=len(contribution_columns),
        )
    )

    wide["price_family_candidate_id"] = (
        PRODUCTION_PRICE_FAMILY_CANDIDATE_ID
    )

    wide["price_family_candidate_label"] = (
        PRODUCTION_PRICE_FAMILY_LABEL
    )

    wide["integration_policy"] = (
        "incumbent_demand_capital_markets__"
        "ma12_price_affordability"
    )

    incumbent_axis = load_incumbent_axis_history()

    wide = wide.merge(
        incumbent_axis,
        on=[
            "geo_id",
            "date",
        ],
        how="left",
        validate="one_to_one",
    )

    wide[
        "integrated_minus_incumbent_axis"
    ] = (
        wide["integrated_demand_axis"]
        - wide["incumbent_axis_score"]
    )

    wide[
        "absolute_integrated_minus_incumbent_axis"
    ] = (
        wide[
            "integrated_minus_incumbent_axis"
        ].abs()
    )
    
    incumbent_price_family = (
        build_incumbent_price_family_wide(
            incumbent_dimensions
        )
    )

    wide = wide.merge(
        incumbent_price_family,
        on=[
            "geo_id",
            "date",
        ],
        how="left",
        validate="one_to_one",
    )

    wide["price_score_delta"] = (
        wide["price"]
        - wide["incumbent_price"]
    )

    wide["affordability_score_delta"] = (
        wide["affordability"]
        - wide["incumbent_affordability"]
    )

    wide["price_axis_delta"] = (
        wide["price_score_delta"]
        * wide["price_weight"]
    )

    wide["affordability_axis_delta"] = (
        wide["affordability_score_delta"]
        * wide["affordability_weight"]
    )

    wide["price_family_axis_delta"] = (
        wide["price_axis_delta"]
        + wide["affordability_axis_delta"]
    )

    wide["price_family_delta_reconstruction_residual"] = (
        wide["integrated_minus_incumbent_axis"]
        - wide["price_family_axis_delta"]
    )
    
    return wide.sort_values(
        [
            "geo_id",
            "date",
        ],
        kind="mergesort",
    ).reset_index(drop=True)


def build_coverage_summary(
    wide: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for geo_id, group in wide.groupby(
        "geo_id",
        sort=True,
    ):
        complete = group[
            group["complete_dimension_coverage"]
        ]

        row: dict[str, Any] = {
            "geo_id": geo_id,
            "rows": int(len(group)),
            "first_any_date": group["date"].min(),
            "last_any_date": group["date"].max(),
            "complete_rows": int(len(complete)),
            "complete_coverage_rate": float(
                group[
                    "complete_dimension_coverage"
                ].mean()
            ),
            "first_complete_date": (
                complete["date"].min()
                if not complete.empty
                else pd.NaT
            ),
            "last_complete_date": (
                complete["date"].max()
                if not complete.empty
                else pd.NaT
            ),
        }

        for dimension in EXPECTED_DIMENSIONS:
            available = group[
                group[dimension].notna()
            ]

            row[
                f"{dimension}_available_rows"
            ] = int(len(available))

            row[
                f"{dimension}_first_date"
            ] = (
                available["date"].min()
                if not available.empty
                else pd.NaT
            )

            row[
                f"{dimension}_last_date"
            ] = (
                available["date"].max()
                if not available.empty
                else pd.NaT
            )

        rows.append(row)

    return pd.DataFrame(rows)


def build_missing_dimension_rows(
    wide: pd.DataFrame,
) -> pd.DataFrame:
    incomplete = wide[
        ~wide["complete_dimension_coverage"]
    ].copy()

    if incomplete.empty:
        return pd.DataFrame(
            columns=[
                "geo_id",
                "date",
                "missing_dimensions",
                "available_dimension_count",
            ]
        )

    incomplete["missing_dimensions"] = (
        incomplete.apply(
            lambda row: "|".join(
                dimension
                for dimension in EXPECTED_DIMENSIONS
                if pd.isna(row[dimension])
            ),
            axis=1,
        )
    )

    return incomplete[
        [
            "geo_id",
            "date",
            "missing_dimensions",
            "available_dimension_count",
        ]
    ].sort_values(
        [
            "geo_id",
            "date",
        ]
    )


def build_latest_state(
    wide: pd.DataFrame,
) -> pd.DataFrame:
    complete = wide[
        wide["complete_dimension_coverage"]
    ].copy()

    if complete.empty:
        return complete

    return (
        complete.sort_values("date")
        .groupby(
            "geo_id",
            group_keys=False,
        )
        .tail(1)
        .sort_values("geo_id")
        .reset_index(drop=True)
    )


def build_axis_long(
    wide: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []

    for row in wide.itertuples(index=False):
        for dimension in EXPECTED_DIMENSIONS:
            records.append(
                {
                    "geo_id": row.geo_id,
                    "date": row.date,
                    "axis": AXIS_KEY,
                    "dimension": dimension,
                    "dimension_score": getattr(
                        row,
                        dimension,
                    ),
                    "configured_weight": getattr(
                        row,
                        f"{dimension}_weight",
                    ),
                    "weighted_contribution": getattr(
                        row,
                        f"{dimension}_weighted_contribution",
                    ),
                    "integrated_demand_axis":
                        row.integrated_demand_axis,
                    "complete_dimension_coverage":
                        row.complete_dimension_coverage,
                    "source_role": getattr(
                        row,
                        f"{dimension}_source_role",
                        None,
                    ),
                    "source_path": getattr(
                        row,
                        f"{dimension}_source_path",
                        None,
                    ),
                    "price_family_candidate_id":
                        row.price_family_candidate_id,
                    "integration_policy":
                        row.integration_policy,
                }
            )

    return pd.DataFrame(records)


# =============================================================================
# Public runner
# =============================================================================

def build_integrated_demand_chronology(
    *,
    output_dir: Path = OUTPUT_DIR,
) -> dict[str, Any]:
    print(
        "[integrated_demand_chronology] "
        "stage=load_axis_weights"
    )

    weights = load_axis_weights()

    print("Demand-axis weights:")

    for row in weights.itertuples(index=False):
        print(
            f"  {row.dimension}: "
            f"{float(row.dimension_weight):.2%}"
        )

    print(
        "\n[integrated_demand_chronology] "
        "stage=load_price_family_challenger"
    )

    price_family = load_price_family_challenger()

    print(
        "Selected Price/Affordability chronology:"
    )
    print(f"  path={PRICE_FAMILY_CHRONOLOGY_PATH}")
    print(
        f"  rows={len(price_family):,}, "
        f"geographies="
        f"{price_family['geo_id'].nunique():,}, "
        f"first_date="
        f"{price_family['date'].min().date()}, "
        f"last_date="
        f"{price_family['date'].max().date()}"
    )    
    
    print(
        "\n[integrated_demand_chronology] "
        "stage=load_incumbent_dimension_history"
    )

    incumbent_dimensions = (
        load_incumbent_dimension_history()
    )

    selected_source_path = Path(
        incumbent_dimensions[
            "source_path"
        ].iloc[0]
    )

    print("\nSelected incumbent source:")
    print(f"  {selected_source_path}")
    print(
        "  production_run_id="
        f"{PRODUCTION_RUN_ID}"
    )
    print(
        "  dimensions="
        + ", ".join(
            sorted(
                incumbent_dimensions[
                    "dimension"
                ].unique()
            )
        )
    )
    
    print(
        f"  geographies={incumbent_dimensions['geo_id'].nunique():,}, "
        f"rows={len(incumbent_dimensions):,}, "
        f"first_date={incumbent_dimensions['date'].min().date()}, "
        f"last_date={incumbent_dimensions['date'].max().date()}"
    )

    incumbent_keys = set(
        zip(
            incumbent_dimensions["geo_id"],
            incumbent_dimensions["date"],
        )
    )

    price_family_keys = set(
        zip(
            price_family["geo_id"],
            price_family["date"],
        )
    )

    print(
        "  common_geo_months_with_price_family="
        f"{len(incumbent_keys & price_family_keys):,}"
    )

    print(
        "\n[integrated_demand_chronology] "
        "stage=integrate_dimensions"
    )

    integrated_long = (
        build_integrated_dimension_history(
            incumbent_dimensions,
            price_family,
        )
    )
    
    challenger_geographies = sorted(
        price_family["geo_id"]
        .dropna()
        .unique()
        .tolist()
    )

    integrated_geographies = sorted(
        integrated_long["geo_id"]
        .dropna()
        .unique()
        .tolist()
    )

    if integrated_geographies != challenger_geographies:
        raise ValueError(
            "Integrated geography universe differs from "
            "the MA12 challenger geography universe.\n"
            f"challenger={challenger_geographies}\n"
            f"integrated={integrated_geographies}"
        )

    print(
        "Integrated evaluation universe:"
    )
    print(
        f"  geographies={len(integrated_geographies):,}"
    )
    print(
        "  geography_ids="
        + ", ".join(integrated_geographies)
    )

    wide = build_wide_chronology(
        integrated_long,
        weights,
        incumbent_dimensions,
    )

    coverage = build_coverage_summary(
        wide
    )

    missing_rows = build_missing_dimension_rows(
        wide
    )

    latest_state = build_latest_state(
        wide
    )
    
    axis_impact_rows = []

    for geo_id, group in wide.groupby(
        "geo_id",
        sort=True,
    ):
        overlap = group.dropna(
            subset=[
                "integrated_demand_axis",
                "incumbent_axis_score",
            ]
        )
        
        if overlap.empty:
            continue

        axis_impact_rows.append(
            {
                "geo_id": geo_id,
                "overlap_rows": int(
                    len(overlap)
                ),
                "first_overlap_date":
                    overlap["date"].min(),
                "last_overlap_date":
                    overlap["date"].max(),
                "mean_incumbent_axis_score": float(
                    overlap[
                        "incumbent_axis_score"
                    ].mean()
                ),
                "mean_integrated_axis_score": float(
                    overlap[
                        "integrated_demand_axis"
                    ].mean()
                ),
                "mean_integrated_minus_incumbent": float(
                    overlap[
                        "integrated_minus_incumbent_axis"
                    ].mean()
                ),
                "mean_absolute_integrated_minus_incumbent":
                    float(
                        overlap[
                            "absolute_integrated_minus_incumbent_axis"
                        ].mean()
                    ),
                "p90_absolute_integrated_minus_incumbent":
                    float(
                        overlap[
                            "absolute_integrated_minus_incumbent_axis"
                        ].quantile(0.90)
                    ),
                "latest_incumbent_axis_score": float(
                    overlap.sort_values("date")[
                        "incumbent_axis_score"
                    ].iloc[-1]
                ),
                "latest_integrated_axis_score": float(
                    overlap.sort_values("date")[
                        "integrated_demand_axis"
                    ].iloc[-1]
                ),
                "mean_price_score_delta": float(
                    overlap[
                        "price_score_delta"
                    ].mean()
                ),
                "mean_affordability_score_delta": float(
                    overlap[
                        "affordability_score_delta"
                    ].mean()
                ),
                "mean_price_axis_delta": float(
                    overlap[
                        "price_axis_delta"
                    ].mean()
                ),
                "mean_affordability_axis_delta": float(
                    overlap[
                        "affordability_axis_delta"
                    ].mean()
                ),
                "mean_price_family_axis_delta": float(
                    overlap[
                        "price_family_axis_delta"
                    ].mean()
                ),
                "max_delta_reconstruction_residual": float(
                    overlap[
                        "price_family_delta_reconstruction_residual"
                    ].abs().max()
                ),
            }
        )

    axis_impact_summary = pd.DataFrame(
        axis_impact_rows
    )

    axis_long = build_axis_long(
        wide
    )

    complete = wide[
        wide["complete_dimension_coverage"]
    ].copy()

    if complete.empty:
        raise ValueError(
            "Integrated chronology contains no complete "
            "four-dimension geography-months."
        )

    common_geo_count = int(
        complete["geo_id"].nunique()
    )

    if common_geo_count == 0:
        raise ValueError(
            "Integrated chronology contains no common "
            "geographies."
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    _write_csv(
        integrated_long,
        output_dir
        / "integrated_dimension_history_long.csv",
    )

    _write_csv(
        wide,
        output_dir
        / "monthly_integrated_demand_axis.csv",
    )

    _write_csv(
        axis_long,
        output_dir
        / "monthly_integrated_demand_axis_long.csv",
    )

    _write_csv(
        coverage,
        output_dir
        / "coverage_summary.csv",
    )

    _write_csv(
        missing_rows,
        output_dir
        / "missing_dimension_rows.csv",
    )

    _write_csv(
        latest_state,
        output_dir
        / "latest_integrated_state.csv",
    )
    
    _write_csv(
        axis_impact_summary,
        output_dir
        / "axis_impact_summary.csv",
    )

    manifest = {
        "phase": "D1",
        "artifact_contract":
            "integrated_monthly_demand_chronology",
        "axis": AXIS_KEY,
        "weights": {
            row.dimension: float(
                row.dimension_weight
            )
            for row in weights.itertuples(
                index=False
            )
        },
        "price_family_candidate_id":
            PRODUCTION_PRICE_FAMILY_CANDIDATE_ID,
        "price_family_candidate_label":
            PRODUCTION_PRICE_FAMILY_LABEL,
        "price_family_source":
            PRICE_FAMILY_CHRONOLOGY_PATH,            
        "production_run_id":
            PRODUCTION_RUN_ID,
        "incumbent_dimension_source":
            selected_source_path,
        "incumbent_axis_source":
            INCUMBENT_AXIS_SCORES_PATH,
        "incumbent_source_dimensions":
            sorted(
                incumbent_dimensions[
                    "dimension"
                ].unique()
                .tolist()
            ),
        "integration_policy": (
            "incumbent demand and capital_markets; "
            "MA12 challenger price and affordability; "
            "fixed configured axis weights; "
            "no availability renormalization"
        ),
        "rows_total": int(len(wide)),
        "rows_complete": int(len(complete)),
        "complete_coverage_rate": float(
            wide[
                "complete_dimension_coverage"
            ].mean()
        ),
        "geographies_total": int(
            wide["geo_id"].nunique()
        ),
        "geographies_complete": common_geo_count,
        "first_any_date":
            wide["date"].min(),
        "last_any_date":
            wide["date"].max(),
        "first_complete_date":
            complete["date"].min(),
        "last_complete_date":
            complete["date"].max(),
        "output_directory": output_dir,
        "output_files": [
            "integrated_dimension_history_long.csv",
            "monthly_integrated_demand_axis.csv",
            "monthly_integrated_demand_axis_long.csv",
            "coverage_summary.csv",
            "missing_dimension_rows.csv",
            "latest_integrated_state.csv",
            "integration_manifest.json",
            "axis_impact_summary.csv",
        ],
    }

    _write_json(
        manifest,
        output_dir
        / "integration_manifest.json",
    )

    print(
        "\n[integrated_demand_chronology] "
        "stage=validation"
    )

    print("\nCoverage summary:")

    coverage_print = coverage[
        coverage["complete_rows"].gt(0)
    ].copy()

    print(
        coverage_print.to_string(
            index=False
        )
    )
    
    zero_complete_geographies = int(
        coverage["complete_rows"].eq(0).sum()
    )

    if zero_complete_geographies:
        print(
            "\nWARNING: "
            f"{zero_complete_geographies:,} geographies "
            "have zero complete integrated rows."
        )

    print("\nLatest integrated state:")
    latest_columns = [
        "geo_id",
        "date",
        *EXPECTED_DIMENSIONS,
        *[
            f"{dimension}_weighted_contribution"
            for dimension in EXPECTED_DIMENSIONS
        ],
        "integrated_demand_axis",
    ]

    print(
        latest_state[
            [
                column
                for column in latest_columns
                if column in latest_state.columns
            ]
        ].to_string(index=False)
    )

    print("\nIntegrated chronology summary:")
    print(
        f"  total rows: {len(wide):,}"
    )
    print(
        f"  complete rows: {len(complete):,}"
    )
    print(
        "  complete coverage rate: "
        f"{wide['complete_dimension_coverage'].mean():.2%}"
    )
    print(
        f"  complete geographies: "
        f"{common_geo_count:,}"
    )
    print(
        "  complete chronology: "
        f"{complete['date'].min().date()} "
        "to "
        f"{complete['date'].max().date()}"
    )
    print(
        "\nMA12 integrated-axis impact:"
    )

    print(
        axis_impact_summary.to_string(
            index=False
        )
    )

    print("\nArtifacts written to:")
    print(output_dir)

    print("\nFiles:")

    for path in sorted(
        output_dir.iterdir()
    ):
        print(f"  {path}")

    return {
        "weights": weights,
        "price_family": price_family,        
        "incumbent_dimension_history": incumbent_dimensions,
        "incumbent_dimension_source": selected_source_path,
        "integrated_dimension_history": integrated_long,
        "monthly_integrated_demand_axis": wide,
        "monthly_integrated_demand_axis_long": axis_long,
        "coverage_summary": coverage,
        "missing_dimension_rows": missing_rows,
        "latest_integrated_state": latest_state,
        "manifest": manifest,
        "axis_impact_summary": axis_impact_summary,
    }
