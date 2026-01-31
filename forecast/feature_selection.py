# forecast/feature_selection.py
# Thin compatibility shim. Canonical code lives in forecast/selection/.

from forecast.selection.scoring import ScoredCandidate, score_candidates
from forecast.selection.governance import (
    default_bucket,
    select_scored_candidates,
    scored_to_feature_specs,
)

__all__ = [
    "ScoredCandidate",
    "score_candidates",
    "default_bucket",
    "select_scored_candidates",
    "scored_to_feature_specs",
]
