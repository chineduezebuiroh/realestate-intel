from __future__ import annotations
# core/config.py

from datetime import date
from pathlib import Path


FULL_DB_PATH = Path("data/market.duckdb")
SERVING_DB_PATH = Path("data/market_serving.duckdb")
SERVING_START_DATE = date(2015, 1, 1)
