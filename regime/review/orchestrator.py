from __future__ import annotations

from pathlib import Path

from .artifact_writer import ReviewArtifactWriter
from .decision import DecisionSummary
from .manifest import ReviewManifest
from .models import ReviewBundle


def write_review_bundle(
    *,
    bundle: ReviewBundle,
    writer: ReviewArtifactWriter,
    manifest: ReviewManifest,
    decision: DecisionSummary | None = None,
) -> Path:
    """
    Persist a complete review bundle.

    Responsibilities
    ----------------
    • write every ReviewTable
    • discover written artifacts
    • update manifest.outputs
    • write manifest
    • optionally write decision summary

    Plot files are assumed to have already been produced by the
    diagnostic. They are referenced by the manifest but are not
    generated here.
    """

    writer.prepare()

    for table in bundle.tables:
        writer.write_table(
            table.name,
            table.dataframe,
            subdir=table.subdirectory,
        )

    outputs = writer.build_output_manifest()

    plot_outputs = [
        {
            "path": plot.relative_path.as_posix(),
            "artifact_type": "plot",
        }
        for plot in bundle.plots
    ]

    manifest.outputs = outputs + plot_outputs

    manifest.write(writer)

    if decision is not None:
        decision.write(writer)

    return writer.output_dir