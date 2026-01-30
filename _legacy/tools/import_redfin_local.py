# tools/import_redfin_local.py
import argparse, os, shutil, pathlib, sys
import pandas as pd

def detect_sep(path: str) -> str:
    # Quick heuristic: tabs for .tsv/.tsv000, else comma
    p = path.lower()
    if p.endswith(".tsv") or p.endswith(".tsv000"):
        return "\t"
    return ","

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--file", required=True, help="Path to a manually downloaded Redfin MONTHLY file (.tsv/.tsv000/.csv)")
    p.add_argument("--level", required=True, choices=["city","county","state"], help="Geography granularity")
    args = p.parse_args()

    src = os.path.abspath(args.file)
    if not os.path.exists(src):
        sys.exit(f"[import] file not found: {src}")

    raw_dir = os.path.join("data","raw","redfin", args.level)
    latest = os.path.join(raw_dir, "monthly_latest.tsv")  # normalize to .tsv on disk
    pathlib.Path(raw_dir).mkdir(parents=True, exist_ok=True)

    # Sanity read a few rows (auto sep)
    sep = detect_sep(src)
    try:
        head = pd.read_csv(src, sep=sep, nrows=5)
    except Exception as e:
        sys.exit(f"[import] cannot read input file with sep='{sep}': {e}")

    # Archive and copy to latest
    stamp = pd.Timestamp.utcnow().strftime("%Y%m%d")
    dated_name = f"monthly_{args.level}_{stamp}.tsv"
    dst_dated = os.path.join(raw_dir, dated_name)
    # If source isn’t tab, convert to canonical TSV for consistency
    if sep == "\t":
        shutil.copy2(src, dst_dated)
    else:
        head0 = pd.read_csv(src, sep=sep)  # full read for conversion
        head0.to_csv(dst_dated, sep="\t", index=False)

    shutil.copy2(dst_dated, latest)

    print(f"[import:{args.level}] archived -> {dst_dated}")
    print(f"[import:{args.level}] latest   -> {latest}")

if __name__ == "__main__":
    main()
