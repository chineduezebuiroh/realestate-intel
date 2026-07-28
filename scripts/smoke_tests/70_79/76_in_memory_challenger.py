from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from regime.artifacts import (
    RegimeArtifactStore,
)
from regime.experiments.in_memory_challenger import (
    build_in_memory_smoothing_challenger,
)


BASELINE_RUN_ID = (
    "macro_regime_v1_bps120_sources"
)

EXPERIMENT_ID = (
    "inventory_ma3_deviation"
)


def main() -> int:
    store = RegimeArtifactStore()

    source_metrics = store.read_dataframe(
        BASELINE_RUN_ID,
        "source_metrics",
    )

    baseline_features = (
        store.read_dataframe(
            BASELINE_RUN_ID,
            "features",
        )
    )

    challenger = (
        build_in_memory_smoothing_challenger(
            baseline_features=(
                baseline_features
            ),
            source_metrics=(
                source_metrics
            ),
            experiment_id=(
                EXPERIMENT_ID
            ),
        )
    )

    mapping = challenger.as_mapping()

    required = {
        "features",
        "smoothing_lineage",
        "normalized_features",
        "metric_scores",
        "aligned_metric_scores",
        "dimension_scores",
        "axis_scores",
        "coordinates",
        "geometry",
        "regime_assignments",
    }

    if set(mapping) != required:
        raise AssertionError(
            "In-memory challenger artifact "
            "contract mismatch"
        )

    for name in required:
        if mapping[name].empty:
            raise AssertionError(
                "Expected non-empty in-memory "
                f"artifact: {name}"
            )

    print("=" * 100)
    print(
        "SMOKE TEST 76 — "
        "IN-MEMORY CHALLENGER: PASS"
    )
    print("=" * 100)

    print(
        "features=",
        len(challenger.features),
    )

    print(
        "normalized_features=",
        len(
            challenger.normalized_features
        ),
    )

    print(
        "metric_scores=",
        len(challenger.metric_scores),
    )

    print(
        "dimension_scores=",
        len(challenger.dimension_scores),
    )

    print(
        "axis_scores=",
        len(challenger.axis_scores),
    )

    print(
        "regime_assignments=",
        len(
            challenger.regime_assignments
        ),
    )

    print(
        "smoothing_lineage=",
        len(
            challenger.smoothing_lineage
        ),
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())