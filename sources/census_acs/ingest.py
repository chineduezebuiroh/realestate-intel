from __future__ import annotations
# sources/census_acs/ingest.py

import os
import csv
import time
from pathlib import Path
from typing import Optional, Dict, Any, List

from dotenv import load_dotenv
import requests
from requests.exceptions import ReadTimeout, ConnectionError, RequestException

import pandas as pd

load_dotenv()

PLAN_PATH = Path("data/census/census_acs5_query_plan.generated.csv")
OUT_RAW   = Path("data/census/census_acs5_raw.csv")

CENSUS_KEY = (os.getenv("CENSUS_API_KEY") or "").strip()
if not CENSUS_KEY:
    raise SystemExit("[census][fatal] CENSUS_API_KEY not set")


def build_census_geo_params(level: str, code: str) -> Optional[dict]:
    level = (level or "").strip().lower()
    code = (code or "").strip()
    if not code:
        return None

    if level in ("nation", "national", "us"):
        return {"for": "us:1"}

    if level in ("state", "state_equiv"):
        return {"for": f"state:{code}"}

    if level in ("county", "county_equiv", "independent_city"):
        if len(code) != 5:
            return None
        return {"for": f"county:{code[2:]}", "in": f"state:{code[:2]}"}

    if level in ("city", "place"):
        if len(code) != 7:
            return None
        return {"for": f"place:{code[2:]}", "in": f"state:{code[:2]}"}

    if level in ("msa", "metro_area", "metro"):
        return {"for": f"metropolitan statistical area/micropolitan statistical area:{code}"}

    # intentionally unsupported (keep behavior explicit)
    if level in ("csa", "combined_area", "metro_division", "msd"):
        return None

    return None


def census_request(
    year: int,
    dataset: str,
    var_codes: List[str],
    for_param: str,
    in_param: Optional[str] = None,
    *,
    timeout: int = 60,
    max_attempts: int = 5,
):
    base = f"https://api.census.gov/data/{year}/{dataset}"
    params: Dict[str, str] = {"get": "NAME," + ",".join(var_codes), "for": for_param}
    if in_param:
        params["in"] = in_param
    if CENSUS_KEY:
        params["key"] = CENSUS_KEY

    last_err = None

    for attempt in range(1, max_attempts + 1):
        try:
            print(
                f"[census:req] attempt={attempt}/{max_attempts} "
                f"year={year} dataset={dataset} for={for_param} in={in_param}"
            )
            r = requests.get(base, params=params, timeout=timeout)

            if r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"retryable status={r.status_code}")
                wait_s = min(2 ** (attempt - 1), 20)
                print(
                    f"[census:req][warn] retryable HTTP status {r.status_code} "
                    f"on attempt {attempt}/{max_attempts}; sleeping {wait_s}s"
                )
                if attempt < max_attempts:
                    time.sleep(wait_s)
                    continue
                return None

            if r.status_code == 404:
                # Treat as "geo not available for this year/dataset" or bad geo mapping; skip.
                return None

            r.raise_for_status()
            
            try:
                data = r.json()
            except ValueError:
                print(
                    f"[census:req][warn] non-JSON response; "
                    f"year={year} dataset={dataset} for={for_param} in={in_param}; "
                    f"status={r.status_code}; body={r.text[:200]!r}"
                )
                return None

            if not data or len(data) < 2:
                return None

            headers, row = data[0], data[1]
            return dict(zip(headers, row))

        except (ReadTimeout, ConnectionError) as e:
            last_err = e
            wait_s = min(2 ** (attempt - 1), 20)
            print(
                f"[census:req][warn] transient failure on attempt {attempt}/{max_attempts}: "
                f"{type(e).__name__}: {e}; sleeping {wait_s}s"
            )
            if attempt < max_attempts:
                time.sleep(wait_s)
                continue
            return None

        except RequestException as e:
            last_err = e
            status = getattr(getattr(e, "response", None), "status_code", None)

            if status in {429, 500, 502, 503, 504} and attempt < max_attempts:
                wait_s = min(2 ** (attempt - 1), 20)
                print(
                    f"[census:req][warn] retryable request exception on attempt {attempt}/{max_attempts}: "
                    f"status={status} err={e}; sleeping {wait_s}s"
                )
                time.sleep(wait_s)
                continue

            if status == 404:
                return None

            raise

    print(f"[census:req][warn] exhausted retries; last_err={last_err}")
    return None


def main():
    if not PLAN_PATH.exists():
        raise SystemExit("[census] missing plan; run -m sources.census.expand_spec first")

    plan = pd.read_csv(PLAN_PATH, dtype=str)
    if plan.empty:
        print("[census] plan is empty; nothing to do")
        return

    vintage = int(plan["vintage"].iloc[0])
    years_back = 10
    years = list(range(vintage - years_back + 1, vintage + 1))

    rows = []
    skipped = 0
    calls = 0
    skipped_geo_ids = []

    for rec in plan.to_dict(orient="records"):
        geo_id = rec["geo_id"]
        level = (rec["geo_level"] or "").strip().lower()
        code  = (rec["census_code"] or "").strip()
        dataset = rec["dataset"]
        var_codes = [v for v in (rec["variables_csv"] or "").split(",") if v.strip()]

        geo_params = build_census_geo_params(level, code)

        if geo_params is not None and geo_params.get("for", "").startswith("place:"):
            print(f"[census][debug] geo_id={geo_id} level={level} census_code={code} for={geo_params['for']} in={geo_params.get('in')}")

        if geo_params is None:
            skipped += 1
            skipped_geo_ids.append(geo_id)
            continue

        for y in years:
            resp = census_request(
                year=y,
                dataset=dataset,
                var_codes=var_codes,
                for_param=geo_params["for"],
                in_param=geo_params.get("in"),
            )
            calls += 1
            if not resp:
                continue

            for var_code in var_codes:
                raw_val = resp.get(var_code)
                try:
                    val = float(raw_val) if raw_val not in (None, "", "null") else None
                except Exception:
                    val = None

                rows.append({
                    "geo_id": geo_id,
                    "geo_level": level,
                    "census_code": code,
                    "dataset": dataset,
                    "vintage": vintage,
                    "year": y,
                    "date": f"{y}-12-31",
                    "variable_code": var_code,
                    "value": val,
                    "census_name": resp.get("NAME"),
                })

            time.sleep(0.05)

    if skipped_geo_ids:
        print(f"[census] skipped unsupported geo_ids ({len(skipped_geo_ids)}): {sorted(set(skipped_geo_ids))}")

    df = pd.DataFrame(rows)
    OUT_RAW.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_RAW, index=False)

    print(f"[census] calls={calls} rows={len(df)} skipped_geos={skipped} → {OUT_RAW}")
    print(df.head(10))


if __name__ == "__main__":
    main()
