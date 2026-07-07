from __future__ import annotations
# regime/derived_metrics.py

import pandas as pd


MONTHLY_KEYS = {
    "median_sale_price",
    "mortgage_30y",
    "permit_activity",
}

ANNUAL_FFILL_KEYS = {
    "median_household_income",
    "population",
}


def _monthly_panel(raw: pd.DataFrame) -> pd.DataFrame:
    raw = raw.copy()
    raw["date"] = pd.to_datetime(raw["date"])

    wide = (
        raw.pivot_table(
            index=["geo_id", "date"],
            columns="canonical_metric_key",
            values="value",
            aggfunc="first",
        )
        .reset_index()
        .sort_values(["geo_id", "date"])
    )

    # Build one monthly calendar per geo from observed monthly metric dates.
    monthly_dates = (
        raw[raw["canonical_metric_key"].isin(MONTHLY_KEYS)]
        [["geo_id", "date"]]
        .drop_duplicates()
        .copy()
    )

    panel = monthly_dates.merge(wide, on=["geo_id", "date"], how="left")
    panel = panel.sort_values(["geo_id", "date"])

    # Bring annual values forward onto monthly observations by geo.
    for col in ANNUAL_FFILL_KEYS:
        if col in panel.columns:
            panel[col] = panel.groupby("geo_id")[col].ffill()
    
    # Broadcast national mortgage rate to local geo/date rows.
    if "mortgage_30y" in wide.columns:
        mortgage = (
            wide[["date", "mortgage_30y"]]
            .dropna(subset=["mortgage_30y"])
            .drop_duplicates(subset=["date"])
        )
        panel = panel.drop(columns=["mortgage_30y"], errors="ignore")
        panel = panel.merge(mortgage, on="date", how="left")
    
    return panel


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

    w = _monthly_panel(raw)
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

        payment = principal * (
            monthly_rate * (1 + monthly_rate) ** n
        ) / ((1 + monthly_rate) ** n - 1)

        monthly_income = tmp["median_household_income"] / 12.0
        tmp["payment_burden"] = payment / monthly_income
        outputs.append(_long(tmp, "payment_burden", "payment_burden"))

    if {"permit_activity", "population"}.issubset(w.columns):
        tmp = w.copy()
        tmp["permit_intensity"] = (tmp["permit_activity"] / tmp["population"]) * 1000.0
        outputs.append(_long(tmp, "permit_intensity", "permit_intensity"))

    if not outputs:
        return pd.DataFrame(columns=["geo_id", "date", "canonical_metric_key", "value"])

    return pd.concat(outputs, ignore_index=True)
