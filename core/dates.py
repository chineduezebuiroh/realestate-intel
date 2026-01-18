# core/dates.py
import pandas as pd

def to_month_end_index(idx) -> pd.DatetimeIndex:
    """
    Normalize a date-like index/series to naive month-end timestamps.
    """
    dt = pd.to_datetime(idx).tz_localize(None)
    return dt.to_period("M").to_timestamp("M")

def to_month_end_date(s) -> pd.Series:
    """
    Normalize a date-like series to python date objects at month-end.
    """
    dt = pd.to_datetime(s, errors="coerce")
    dt = dt.dt.to_period("M").dt.to_timestamp("M")
    return dt.dt.date
