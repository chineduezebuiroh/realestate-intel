from __future__ import annotations
# regime/canonical_metrics.py

import pandas as pd
import numpy as np

from regime._00_config_loader import RegimeConfig, load_regime_config


PHYSICAL_2Y_10Y_SOURCE_KEY = "fred_2y10y_spread"
CANONICAL_10Y_2Y_KEY = "spread_10y_2y"
SPREAD_PARITY_TOLERANCE = 1e-12


def canonicalize_source_polarity(resolved: pd.DataFrame) -> pd.DataFrame:
    """Canonicalize provider orientation before any feature is constructed.

    ``fred_2y10y_spread`` retains its provider identity and physical 2Y-minus-10Y
    values.  Its governed canonical identity is 10Y-minus-2Y, so this boundary
    performs the one and only sign inversion.
    """
    out = resolved.copy()
    mask = (
        out["source_metric_key"].eq(PHYSICAL_2Y_10Y_SOURCE_KEY)
        & out["canonical_metric_key"].eq(CANONICAL_10Y_2Y_KEY)
    )
    out.loc[mask, "value"] = -out.loc[mask, "value"].astype(float)
    return out


def validate_spread_10y_2y_parity(
    canonical: pd.DataFrame,
    treasury_10y: pd.DataFrame,
    treasury_2y: pd.DataFrame,
    physical_2y_10y: pd.DataFrame | None = None,
    *,
    tolerance: float = SPREAD_PARITY_TOLERANCE,
) -> dict[str, int]:
    """Fail closed unless overlapping observations prove governed polarity."""
    keys = ["geo_id", "date"]
    frames = []
    for frame, name in ((canonical, "canonical"), (treasury_10y, "ten"), (treasury_2y, "two")):
        missing = set(keys + ["value"]) - set(frame.columns)
        if missing:
            raise ValueError(f"{name} spread parity input missing columns: {sorted(missing)}")
        q = frame[keys + ["value"]].rename(columns={"value": name})
        if q.duplicated(keys).any():
            raise ValueError(f"{name} spread parity input has duplicate observations")
        frames.append(q)
    joined = frames[0].merge(frames[1], on=keys, how="inner", validate="one_to_one").merge(
        frames[2], on=keys, how="inner", validate="one_to_one"
    )
    if joined.empty:
        raise ValueError("no overlapping canonical/10Y/2Y observations for spread parity")
    governed = joined.ten.astype(float) - joined.two.astype(float)
    if not np.allclose(joined.canonical, governed, rtol=0, atol=tolerance):
        raise ValueError("spread_10y_2y violates treasury_10y - treasury_2y parity")
    nonzero = governed.abs().gt(tolerance)
    if not nonzero.any():
        raise ValueError("spread parity is indeterminate because every governed observation is zero")
    if np.allclose(joined.loc[nonzero, "canonical"], -governed[nonzero], rtol=0, atol=tolerance):
        raise ValueError("spread_10y_2y matches forbidden treasury_2y - treasury_10y orientation")
    physical_rows = 0
    if physical_2y_10y is not None:
        missing = set(keys + ["value"]) - set(physical_2y_10y.columns)
        if missing:
            raise ValueError(f"physical spread parity input missing columns: {sorted(missing)}")
        physical = physical_2y_10y[keys + ["value"]].rename(columns={"value": "physical"})
        if physical.duplicated(keys).any():
            raise ValueError("physical spread parity input has duplicate observations")
        proof = joined.merge(physical, on=keys, how="inner", validate="one_to_one")
        if proof.empty:
            raise ValueError("no overlapping physical provider observations for spread parity")
        if not np.allclose(proof.canonical, -proof.physical.astype(float), rtol=0, atol=tolerance):
            raise ValueError("spread_10y_2y violates inversion parity with fred_spread_2y_10y")
        physical_rows = len(proof)
    return {"canonical_formula_rows": len(joined), "physical_inversion_rows": physical_rows}


def _truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def resolve_canonical_metrics(
    raw: pd.DataFrame,
    config: RegimeConfig | None = None,
) -> pd.DataFrame:
    """
    Convert physical/source metric series into canonical metric series.

    Input columns:
      geo_id, date, metric_key, value

    Output columns:
      geo_id, date, canonical_metric_key, value, source_metric_key

    Resolution rules come from metric_dimension_registry.csv:
      - canonical_metric_key
      - source_priority
      - merge_strategy
      - enabled
      - diagnostic_only

    For primary_else_fallback, lower source_priority wins.
    """
    if config is None:
        config = load_regime_config(validate=True)

    dim = config.metric_dimensions.copy()

    dim = dim[_truthy(dim["enabled"]) & ~_truthy(dim["diagnostic_only"])].copy()

    keep_cols = [
        "metric_key",
        "canonical_metric_key",
        "source_priority",
        "merge_strategy",
    ]

    dim = dim[keep_cols].drop_duplicates()
    dim["source_priority"] = pd.to_numeric(
        dim["source_priority"],
        errors="coerce",
    ).fillna(9999)

    merged = raw.merge(dim, on="metric_key", how="inner")

    if merged.empty:
        return pd.DataFrame(
            columns=[
                "geo_id",
                "date",
                "canonical_metric_key",
                "value",
                "source_metric_key",
            ]
        )

    # Strategy v1:
    # - primary_else_fallback: pick lowest source_priority per geo/date/canonical metric
    # - direct: same behavior, but normally only one metric exists
    # - diagnostic_only rows are excluded above
    merged = merged.sort_values(
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "source_priority",
            "metric_key",
        ]
    )

    merged = merged.rename(columns={"metric_key": "source_metric_key"})

    resolved = (
        merged
        .drop_duplicates(
            subset=["geo_id", "date", "canonical_metric_key"],
            keep="first",
        )
        [["geo_id", "date", "canonical_metric_key", "value", "source_metric_key"]]
        .copy()
    )

    return canonicalize_source_polarity(resolved)
