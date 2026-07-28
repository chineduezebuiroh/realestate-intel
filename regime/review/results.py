from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


@dataclass(slots=True)
class GeneratedPlot:
    """
    Plot already rendered by a diagnostic.

    The review framework never creates plots.
    It only records them in manifests.
    """

    name: str
    path: Path
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class ReviewResult:
    """
    Strongly typed container returned by review diagnostics.
    """

    tables: dict[str, pd.DataFrame] = field(default_factory=dict)

    plots: list[GeneratedPlot] = field(default_factory=list)

    metadata: dict[str, object] = field(default_factory=dict)

    def add_table(
        self,
        name: str,
        frame: pd.DataFrame,
    ) -> None:

        if name in self.tables:
            raise ValueError(
                f"Duplicate review table: {name}"
            )

        self.tables[name] = frame

    def add_plot(
        self,
        plot: GeneratedPlot,
    ) -> None:

        self.plots.append(plot)