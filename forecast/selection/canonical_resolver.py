from __future__ import annotations
# forecast/selection/canonical_resolver.py

from pathlib import Path
from typing import Optional
from forecast.selection.canonical_loader import load_canonical_base_feature_ids

def resolve_canonical_exog_csv(
    *,
    artifact_root: str,
    stability_version: str,
    metric_id: str,
    n: int,
) -> Path:
    root = Path(artifact_root)
    return root / "canonical_exogs" / stability_version / f"canonical_exog_set__metric={metric_id}__n={int(n)}.csv"

def maybe_load_canonical_base_ids(
    *,
    artifact_root: str,
    stability_version: str,
    metric_id: str,
    n: int,
    override_csv: Optional[str] = None,
) -> tuple[Optional[Path], Optional[list[str]]]:
    if override_csv:
        p = Path(override_csv)
        return p, load_canonical_base_feature_ids(p, n=n)

    p = resolve_canonical_exog_csv(
        artifact_root=artifact_root,
        stability_version=stability_version,
        metric_id=metric_id,
        n=n,
    )
    if p.exists():
        return p, load_canonical_base_feature_ids(p, n=n)

    return None, None
