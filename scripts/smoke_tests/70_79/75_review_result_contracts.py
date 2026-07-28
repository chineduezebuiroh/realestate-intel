from __future__ import annotations

from pathlib import Path

import pandas as pd

from regime.review import (
    GeneratedPlot,
    ReviewResult,
)


def main() -> int:

    review = ReviewResult()

    review.add_table(
        "summary",
        pd.DataFrame(
            {
                "x": [1, 2],
            }
        ),
    )

    review.add_plot(
        GeneratedPlot(
            name="example_plot",
            path=Path("plots/example.png"),
        )
    )

    assert len(review.tables) == 1
    assert len(review.plots) == 1

    try:
        review.add_table(
            "summary",
            pd.DataFrame(),
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "Duplicate table accepted"
        )

    print("=" * 100)
    print(
        "SMOKE TEST 75 — REVIEW RESULT CONTRACTS: PASS"
    )
    print("=" * 100)
    print(
        f"tables={len(review.tables)}"
    )
    print(
        f"plots={len(review.plots)}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())