from __future__ import annotations
# sources/census_nrc_fred/ingest.py

import argparse
from pathlib import Path
from typing import Optional, List

import pandas as pd
import requests

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

OUT_RAW_LONG = Path("data/census/nrc_fred_raw_long.csv")
SOURCE_ID = "census_nrc_fred"

# FRED "fredgraph.csv" endpoint doesn't require an API key.
FREDGRAPH_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

# We’re intentionally doing “Option 1.5”: US + Census regions only.
# Series are monthly SAAR totals (as published via FRED).
SERIES = [
    # Housing starts (total units, SAAR)
    ("us_nation", "census_housing_starts_total_saar", "HOUST"),
    ("us_region_northeast", "census_housing_starts_total_saar", "HOUSTNE"),
    ("us_region_midwest", "census_housing_starts_total_saar", "HOUSTMW"),
    ("us_region_south", "census_housing_starts_total_saar", "HOUSTS"),
    ("us_region_west", "census_housing_starts_total_saar", "HOUSTW"),

    # Housing completions (total units, SAAR)
    # NOTE: FRED has region completions too, but IDs are not as universally remembered as starts.
    # We start with national completions now, then we can add region completions once you confirm the IDs.
    ("us_nation", "census_housing_completions_total_saar", "COMPUTSA"),
    # --- completions (SAAR) ---
    {"series_id": "COMPUNETSA", "geo_id": "us_region_northeast", "metric_id": "census_housing_completions_total_saar"},
    {"series_id": "COMPUMWTSA", "geo_id": "us_region_midwest",   "metric_id": "census_housing_completions_total_saar"},
    {"series_id": "COMPUSTSA",  "geo_id": "us_region_south",     "metric_id": "census_housing_completions_total_saar"},
    {"series_id": "COMPUWTSA",  "geo_id": "us_region_west",      "metric_id": "census_housing_completions_total_saar"},
]


def fetch_fred_series(series_id: str) -> pd.DataFrame:
    url = FREDGRAPH_CSV.format(series_id=series_id)
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    # fredgraph.csv has columns: DATE, <SERIES_ID>
    df = pd.read_csv(pd.io.common.StringIO(r.text))

    # ---- normalize FRED schema to {date, value} ----
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]  # normalize whitespace early

    # date col (fredgraph typically uses DATE)
    if "DATE" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"DATE": "date"})
    if "observation_date" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"observation_date": "date"})

    # value col (fredgraph typically uses the series_id)
    if "value" not in df.columns:
        if series_id in df.columns:
            df = df.rename(columns={series_id: "value"})
        else:
            non_date = [c for c in df.columns if c != "date"]
            if len(non_date) == 1:
                df = df.rename(columns={non_date[0]: "value"})
            else:
                raise SystemExit(
                    f"[nrc_fred] cannot identify value column for {series_id}: {df.columns.tolist()}"
                )

    # parse types (FRED can use "." for missing)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")

    df = df[df["date"].notna()].copy()
    df = df[df["value"].notna()].copy()

    # hard invariant: output must be exactly {date, value} (+ maybe extras if you add them later)
    expected = ["date", "value"]
    got = list(df.columns)
    if got[:2] != expected:
        raise SystemExit(f"[nrc_fred] unexpected schema for {series_id}: {got}")

    return df


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest Census New Residential Construction series via FRED")
    parser.add_argument("--out", default=str(OUT_RAW_LONG))
    args = parser.parse_args(argv)

    rows = []
    for geo_id, metric_id, series_id in SERIES:
        print(f"[nrc_fred] fetching {series_id} → geo_id={geo_id} metric_id={metric_id}")
        df = fetch_fred_series(series_id)
        df["geo_id"] = geo_id
        df["metric_id"] = metric_id
        df["source_id"] = SOURCE_ID
        rows.append(df[["geo_id", "metric_id", "date", "value", "source_id"]])

    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["geo_id", "metric_id", "date"]).reset_index(drop=True)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"[nrc_fred] wrote {len(out):,} rows → {out_path}")


if __name__ == "__main__":
    main()

