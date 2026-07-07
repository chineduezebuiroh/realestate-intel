from __future__ import annotations
# regime/derived_metrics.py

import pandas as pd


def build_derived_metrics(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Placeholder for derived metrics.

    Future derived metrics:
    - price_to_income
    - payment_burden
    - permit_intensity

    Expected input/output columns:
    geo_id, date, metric_key, value
    """
    return pd.DataFrame(columns=["geo_id", "date", "metric_key", "value"])
