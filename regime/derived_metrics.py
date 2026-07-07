from __future__ import annotations
# regime/derived_metrics.py

import pandas as pd


def _wide(raw: pd.DataFrame) -> pd.DataFrame:
    return (
        raw.pivot_table(
            index=["geo_id", "date"],
            columns="canonical_metric_key",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )


def _long(df: pd.DataFrame, metric_key: str, value_col: str) -> pd.DataFrame:
    out = df[["geo_id", "date", value_col]].copy()
    out = out.rename(columns={value_col: "value"})
    out["canonical_metric_key"] = metric_key
    out = out.dropna(subset=["value"])
    return out[["geo_id", "date", "canonical_metric_key", "value"]]


def build_derived_metrics(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Build derived canonical metric series.

    Input/output columns:
      geo_id, date, canonical_metric_key, value
    """
    if raw.empty:
        return pd.DataFrame(columns=["geo_id", "date", "canonical_metric_key", "value"])

    w = _wide(raw)
    outputs = []

    if {"median_sale_price", "median_household_income"}.issubset(w.columns):
        tmp = w.copy()
        tmp["price_to_income"] = tmp["median_sale_price"] / tmp["median_household_income"]
        outputs.append(_long(tmp, "price_to_income", "price_to_income"))

    if {"median_sale_price", "median_household_income", "mortgage_30y"}.issubset(w.columns):
        tmp = w.copy()
        annual_rate = tmp["mortgage_30y"] / 100.0
        monthly_rate = annual_rate / 12.0
        n = 360

        principal = tmp["median_sale_price"] * 0.80
        monthly_income = tmp["median_household_income"] / 12.0

        payment = principal * (
            monthly_rate * (1 + monthly_rate) ** n
        ) / ((1 + monthly_rate) ** n - 1)

        tmp["payment_burden"] = payment / monthly_income
        outputs.append(_long(tmp, "payment_burden", "payment_burden"))

    if {"bps_total_units", "population"}.issubset(w.columns):
        tmp = w.copy()
        tmp["permit_intensity"] = tmp["bps_total_units"] / tmp["population"]
        outputs.append(_long(tmp, "permit_intensity", "permit_intensity"))

    if not outputs:
        return pd.DataFrame(columns=["geo_id", "date", "canonical_metric_key", "value"])

    return pd.concat(outputs, ignore_index=True)
