from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from pathlib import Path


@dataclass(slots=True)
class ReviewTable:
    name: str
    dataframe: pd.DataFrame
    subdirectory: str = "tables"

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("ReviewTable.name must be non-empty")

        if not self.subdirectory.strip():
            raise ValueError(
                "ReviewTable.subdirectory must be non-empty"
            )


@dataclass(slots=True)
class ReviewPlot:
    name: str
    relative_path: Path
    section: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("ReviewPlot.name must be non-empty")

        self.relative_path = Path(self.relative_path)

        if self.relative_path.is_absolute():
            raise ValueError(
                "ReviewPlot.relative_path must be relative"
            )


@dataclass(slots=True)
class ReviewBundle:
    campaign_id: str
    tables: list[ReviewTable] = field(default_factory=list)
    plots: list[ReviewPlot] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.campaign_id.strip():
            raise ValueError(
                "ReviewBundle.campaign_id must be non-empty"
            )

    def add_table(
        self,
        name: str,
        dataframe: pd.DataFrame,
        *,
        subdirectory: str = "tables",
    ) -> ReviewTable:
        if any(
            table.name == name
            and table.subdirectory == subdirectory
            for table in self.tables
        ):
            raise ValueError(
                "Duplicate review table target: "
                f"{subdirectory}/{name}.csv"
            )

        table = ReviewTable(
            name=name,
            dataframe=dataframe,
            subdirectory=subdirectory,
        )
        self.tables.append(table)
        return table

    def add_plot(
        self,
        name: str,
        relative_path: str | Path,
        *,
        section: str | None = None,
    ) -> ReviewPlot:
        plot = ReviewPlot(
            name=name,
            relative_path=Path(relative_path),
            section=section,
        )

        if any(
            existing.relative_path == plot.relative_path
            for existing in self.plots
        ):
            raise ValueError(
                "Duplicate review plot path: "
                f"{plot.relative_path.as_posix()}"
            )

        self.plots.append(plot)
        return plot
        
    @property
    def table_count(self) -> int:
        return len(self.tables)


    @property
    def plot_count(self) -> int:
        return len(self.plots)
