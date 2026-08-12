#!/usr/bin/env python3
"""Fast contract smoke test for the diagnostic-only LAUS MA factorial."""
import numpy as np
import pandas as pd

from regime.calendar_ma import minimum_valid_observations
from regime.experiments.laus_ma_window_calibration import (
    BALANCES, GOVERNANCE, LAUS_WEIGHTS, MA_WINDOWS, construct_laus_features,
    scenario_grid,
)


def main() -> None:
    grid=scenario_grid()
    assert len(grid)==16 and set(grid.ma_months)==set(MA_WINDOWS)
    assert set(grid.labor_force_membership)=={"LF-IN"}
    assert set(grid.laus_weight_policy)==set(LAUS_WEIGHTS)
    assert set(grid.balance_policy)==set(BALANCES)
    assert all((grid[k]==v).all() for k,v in GOVERNANCE.items())
    assert minimum_valid_observations(3)==2 and minimum_valid_observations(6)==4
    assert minimum_valid_observations(9)==6 and minimum_valid_observations(12)==8

    # Missing February proves calendarization (not sparse-row rolling), no fill,
    # exact lag3/lag12, and the shared 2/3 coverage rule for every candidate.
    dates=pd.date_range("2018-01-31",periods=30,freq="ME")
    rows=[]
    for metric in ("labor_force","employment","laus_unemployment_rate"):
        for i,date in enumerate(dates):
            if i==1: continue
            rows.append({"geo_id":"district_of_columbia_dc__county","date":date,
                "canonical_metric_key":metric,"raw_value":np.nan if i==8 else 100+i})
    source=pd.DataFrame(rows)
    for window in MA_WINDOWS:
        f=construct_laus_features(source,window)
        assert set(f.feature_type)=={"level","short","long"}
        level=f.loc[f.feature_key.eq("laus_labor_force_level")].sort_values("date")
        assert pd.isna(level.iloc[window-2].raw_feature_value)
        # A missing calendar month remains represented and unavailable; it was
        # neither dropped nor converted to zero/forward-filled.
        feb=level.loc[level.date.dt.to_period("M").eq(pd.Period("2018-02","M"))]
        assert len(feb)==1 and feb.raw_feature_value.isna().all()
        short=f.loc[f.feature_key.eq("laus_labor_force_short")].sort_values("date")
        long=f.loc[f.feature_key.eq("laus_labor_force_long")].sort_values("date")
        assert short.raw_feature_value.first_valid_index() is not None
        assert long.raw_feature_value.first_valid_index() is not None
        # Verify every available momentum value against an explicit calendar
        # month lookup.  Positional/sparse-row lagging cannot satisfy this
        # identity across the deliberately omitted February row.
        levels=level.set_index(level.date.dt.to_period("M")).raw_feature_value
        for lag,frame in ((3,short),(12,long)):
            for row in frame.loc[frame.raw_feature_value.notna()].itertuples():
                month=pd.Timestamp(row.date).to_period("M")
                expected=levels.loc[month]/levels.loc[month-lag]-1
                assert np.isclose(row.raw_feature_value,expected)
    assert "district_of_columbia_dc__county" in source.geo_id.unique()
    print("PASS: governed 16-scenario LAUS MA calibration contract")


if __name__ == "__main__": main()
