# ingest/redfin_market_trends.py
import os, time, pathlib, requests, shutil
from datetime import datetime, timedelta

PRIMARY_URL = "https://redfin-public-data.s3.amazonaws.com/redfin_market_trends/latest/weekly_market_totals.csv"
OUT_DIR = "./data/raw/redfin"
OUT_FILE = f"{OUT_DIR}/weekly_market_totals.csv"
TMP_FILE = f"{OUT_DIR}/_weekly_market_totals.tmp"

# Optional: comma-separated list of mirror URLs in env
# e.g. REDFIN_MIRRORS="https://example.com/weekly_market_totals.csv,https://mirror2/weekly_market_totals.csv"
MIRRORS = [u.strip() for u in os.getenv("REDFIN_MIRRORS","").split(",") if u.strip()]

UA = os.getenv("REDFIN_USER_AGENT",
               "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

MAX_AGE_DAYS = int(os.getenv("REDFIN_CACHE_DAYS", "7"))
TIMEOUT = 45

def _fresh_enough(path: str) -> bool:
    if not os.path.exists(path): return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return (datetime.utcnow() - mtime) <= timedelta(days=MAX_AGE_DAYS)

def _get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "text/csv,*/*"})
    return s

def _download(url: str) -> None:
    sess = _get_session()
    backoff = 2
    for attempt in range(6):  # ~2+4+8+16+32 seconds max total delay
        try:
            with sess.get(url, stream=True, timeout=TIMEOUT) as r:
                if r.status_code == 200 and "text" in r.headers.get("Content-Type",""):
                    pathlib.Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
                    with open(TMP_FILE, "wb") as f:
                        shutil.copyfileobj(r.raw, f)
                    os.replace(TMP_FILE, OUT_FILE)
                    print(f"[redfin] downloaded -> {OUT_FILE}")
                    return
                else:
                    raise RuntimeError(f"HTTP {r.status_code} content-type={r.headers.get('Content-Type')}")
        except Exception as e:
            print(f"[redfin] attempt {attempt+1} failed: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    raise RuntimeError(f"[redfin] failed to download after retries: {url}")

def main():
    # Use cache if recent
    if _fresh_enough(OUT_FILE):
        print(f"[redfin] using cached file: {OUT_FILE}")
        return

    # Try primary, then any mirrors defined in env
    try:
        _download(PRIMARY_URL)
        return
    except Exception as e:
        print(f"[redfin] primary failed: {e}")

    for m in MIRRORS:
        try:
            print(f"[redfin] trying mirror: {m}")
            _download(m)
            return
        except Exception as e:
            print(f"[redfin] mirror failed: {e}")

    raise SystemExit("[redfin] all sources failed")

if __name__ == "__main__":
    main()
