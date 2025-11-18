#!/usr/bin/env python
"""
List available TableName values for the BEA Regional dataset,
so we can find the correct quarterly real GDP-by-state table.
"""

from dotenv import load_dotenv
load_dotenv()

import os
import requests

BEA_API_URL = "https://apps.bea.gov/api/data"
API_KEY = os.getenv("BEA_API_KEY") or os.getenv("BEA_API_USER_ID")

if not API_KEY:
    raise SystemExit("Set BEA_API_KEY or BEA_API_USER_ID in your environment first.")

params = {
    "UserID": API_KEY,
    "method": "GetParameterValues",
    "DataSetName": "Regional",
    "ParameterName": "TableName",
    "ResultFormat": "JSON",
}

print("[bea:debug] Requesting Regional TableName list...")
r = requests.get(BEA_API_URL, params=params, timeout=60)
r.raise_for_status()
j = r.json()

api = j.get("BEAAPI", {})
if not api:
    print("[bea:debug] Unexpected response (no BEAAPI):", j)
    raise SystemExit(1)

if "Error" in api:
    print("[bea:debug] ERROR:", api["Error"])
    raise SystemExit(1)

results = api.get("Results", {})
values = results.get("ParamValue", [])

print(f"[bea:debug] Found {len(values)} Regional tables.\n")

for v in values:
    key = v.get("Key", "")
    desc = (v.get("Desc") or "").strip()
    print(f"{key:15s}  -  {desc}")
