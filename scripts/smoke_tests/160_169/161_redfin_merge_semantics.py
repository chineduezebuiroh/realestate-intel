"""Smoke 161: drop wins overlap while baseline preserves older history."""
import pandas as pd
from sources.redfin.ingest import merge_precedence

def frame(values,priority):
 return pd.DataFrame([{"geo_id":"g","metric_id":"inventory","date":pd.Timestamp(month+"-28"),"property_type_id":"all","property_type":"all","geography_family":"nation","value":value,"_priority":priority} for month,value in values])
baseline=frame([("2026-05","A"),("2026-06","B")],1); drop=frame([("2026-06","C"),("2026-07","D")],2)
candidate=merge_precedence([baseline,drop]); assert list(candidate.value)==["A","C","D"]
assert list(candidate.date.dt.strftime("%Y-%m"))==["2026-05","2026-06","2026-07"]
assert not candidate.duplicated(["geo_id","metric_id","date","property_type_id"]).any()
print("redfin merge semantics smoke: ok")
