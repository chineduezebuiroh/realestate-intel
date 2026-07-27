"""Compatibility exports for the production-safe substitution implementation."""

from regime.source_substitution import (  # noqa: F401
    SUBSTITUTION_LINEAGE_COLUMNS,
    SourceSubstitutionResult,
    apply_metric_source_substitution,
)

__all__ = [
    "SUBSTITUTION_LINEAGE_COLUMNS",
    "SourceSubstitutionResult",
    "apply_metric_source_substitution",
]
