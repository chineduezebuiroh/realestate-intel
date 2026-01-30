#!/usr/bin/env python
from dotenv import load_dotenv
load_dotenv()

import os, requests

BEA_API_URL = "https://apps.bea.gov/api/data"
API_KEY = (os.getenv("BEA_API_KEY") or os.getenv("BEA_API_USER_ID") or "").strip()
if not API_KEY:
    raise SystemExit("Set BEA_API_KEY or BEA_API_USER_ID in your environment first.")

params = {
    "UserID": API_KEY,
    "method": "GetParameterValues",
    "DataSetName": "Regional",
    "ParameterName": "LineCode",
    "TableName": "SQGDP9",
    "ResultFormat": "JSON",
}

print("[bea:debug] Requesting LineCode list for Regional/SQGDP9 ...")
r = requests.get(BEA_API_URL, params=params, timeout=60)
r.raise_for_status()
j = r.json()

api = j.get("BEAAPI", {})
if "Error" in api:
    raise SystemExit(f"BEA Error: {api['Error']}")

values = api.get("Results", {}).get("ParamValue", [])
print(f"[bea:debug] Found {len(values)} linecodes\n")
for v in values[:200]:  # print first 200; enough to find "All industry total"
    print(f"{v.get('Key',''):>6}  -  {(v.get('Desc') or '').strip()}")
