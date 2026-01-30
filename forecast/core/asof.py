# forecast/asof.py
from __future__ import annotations

import datetime as dt
from typing import Optional, Union

import pandas as pd


AsofInput = Optional[Union[str, dt.date, dt.datetime]]


def normalize_month_end(data_asof: AsofInput) -> Optional[dt.date]:
    """
    Normalize an as-of input to a month-end DATE (YYYY-MM-DD).
    - None -> None
    - '2025-12-15' -> 2025-12-31
    - date/datetime -> month-end date
    """
    if data_asof is None:
        return None

    if isinstance(data_asof, dt.datetime):
        d = data_asof.date()
    elif isinstance(data_asof, dt.date):
        d = data_asof
    else:
        # string
        d = pd.to_datetime(str(data_asof), errors="raise").date()

    # month-end normalize
    return pd.Timestamp(d).to_period("M").to_timestamp("M").date()
