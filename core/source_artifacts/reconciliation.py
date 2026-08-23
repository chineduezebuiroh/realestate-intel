from __future__ import annotations
import pandas as pd
from .models import CANONICAL_KEY

def preserve_prior(prior: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    """New overlap wins; prior-only survives; new-only is appended."""
    new=new.copy(); new["date"]=pd.to_datetime(new["date"]).dt.date
    if prior is not None:
        prior=prior.copy(); prior["date"]=pd.to_datetime(prior["date"]).dt.date
    if prior is None or prior.empty: result = new.copy()
    else:
        marked = new[CANONICAL_KEY].drop_duplicates()
        old_only = prior.merge(marked, on=CANONICAL_KEY, how="left", indicator=True)
        old_only = old_only.loc[old_only.pop("_merge").eq("left_only")]
        result = pd.concat([old_only, new], ignore_index=True)
    return result.sort_values(CANONICAL_KEY, kind="mergesort").reset_index(drop=True)
