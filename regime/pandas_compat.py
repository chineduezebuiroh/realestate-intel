"""Version-stable pandas offsets used for monthly timestamp generation."""

import pandas as pd

MONTH_END = pd.offsets.MonthEnd()
MONTH_BEGIN = pd.offsets.MonthBegin()
