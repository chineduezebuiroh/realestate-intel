from __future__ import annotations

import pandas as pd
from typing import Optional

def parse_data_asof(s: Optional[str]):
    if not s:
        return None
    return pd.to_datetime(s).date()

def base_id_from_feature_id(feature_id: str) -> str:
    return str(feature_id).rsplit("_lag", 1)[0]
