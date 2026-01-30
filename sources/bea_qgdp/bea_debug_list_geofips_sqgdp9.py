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
    "ParameterName": "GeoFips",
    "TableName": "SQGDP9",
    "ResultFormat": "JSON",
}

print("[bea:debug] Requesting GeoFips list for Regional/SQGDP9 ...")
r = requests.get(BEA_API_URL, params=params, timeout=60)
r.raise_for_status()
j = r.json()

api = j.get("BEAAPI", {})
if "Error" in api:
    raise SystemExit(f"BEA Error: {api['Error']}")

values = api.get("Results", {}).get("ParamValue", [])
print(f"[bea:debug] Found {len(values)} GeoFips\n")
# print a tight sample so you can see the format
for v in values[:80]:
    print(f"{v.get('Key',''):>8}  -  {(v.get('Desc') or '').strip()}")
