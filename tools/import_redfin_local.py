# tools/import_redfin_local.py
import argparse, os, shutil, pathlib, sys
import pandas as pd

RAW_DIR = "data/raw/redfin"
LATEST = os.path.join(RAW_DIR, "monthly_market_totals.csv")  # <- monthly

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--file", required=True, help="Path to a manually downloaded Redfin MONTHLY CSV")
    args = p.parse_args()

    src = os.path.abspath(args.file)
    if not os.path.exists(src):
        sys.exit(f"[import] file not found: {src}")

    pathlib.Path(RAW_DIR).mkdir(parents=True, exist_ok=True)

    # Sanity check
    try:
        head = pd.read_csv(src, nrows=5)
    except Exception as e:
        sys.exit(f"[import] cannot read CSV: {e}")

    # Expect a monthly CSV (usually still has `period_end`), this is just advisory:
    expected_any = {"period_end", "period_end_date", "month", "region", "region_type"}
    lower = set(map(str.lower, head.columns))
    if not (expected_any & lower):
        print(f"[import] warning: columns look unusual for Redfin monthly: {list(head.columns)}")

    # Archive copy with timestamp
    stamp = pd.Timestamp.utcnow().strftime("%Y%m%d")
    dated_name = f"monthly_market_totals_{stamp}.csv"
    dst_dated = os.path.join(RAW_DIR, dated_name)
    shutil.copy2(src, dst_dated)

    # Stable “latest” pointer (transform reads this)
    shutil.copy2(dst_dated, LATEST)

    print(f"[import] archived -> {dst_dated}")
    print(f"[import] latest   -> {LATEST}")

if __name__ == "__main__":
    main()
