from __future__ import annotations

import pandas as pd
from typing import Optional

def parse_data_asof(s: Optional[str]):
    if not s:
        return None
    return pd.to_datetime(s).date()

def base_id_from_feature_id(feature_id: str) -> str:
    return str(feature_id).rsplit("_lag", 1)[0]

def _source_from_feature_id(feature_id: str) -> str:
    """
    feature_id format (current convention):
      "<metric>__<geo>__<pt>__<source>_lag<k>"

    We want the trailing token that contains the source (e.g. "redfin_lag1", "laus_lag6").
    """
    try:
        return str(feature_id).split("__")[-1]
    except Exception:
        return ""


def _is_redfin_source(source_token: str) -> bool:
    return str(source_token).lower().startswith("redfin")
