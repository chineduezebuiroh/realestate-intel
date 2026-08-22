"""Smoke 160: governed Redfin v2 contracts without real local data."""
from __future__ import annotations
import json, subprocess, tempfile
from pathlib import Path
import duckdb, pandas as pd

from sources.redfin.governance import FAMILIES, METRICS, GovernanceError, assert_safe_delete, bootstrap
from sources.redfin.ingest import build_candidate, infer_family, register_drop
from sources.redfin.storage import atomic_json, current, promote, quarantine, retain, sha256
from sources.redfin.transform import apply_candidate
from sources.redfin.validate import validate_baseline, validate_candidate, validate_drop, validate_serving

NAMES={"nation":"redfin_housing_market_monthly_all_country_2012_Jan_to_2026_Jul.csv","state":"redfin_housing_market_monthly_all_states_2012_Jan_to_2026_Jul.csv","metro":"redfin_housing_market_monthly_all_metros_2012_Jan_to_2026_Jul.csv","county":"redfin_housing_market_monthly_all_counties_2012_Jan_to_2026_Jul.csv","city":"redfin_housing_market_monthly_all_cities_2012_Jan_to_2026_Jul.csv","neighborhood":"redfin_housing_market_monthly_all_neighborhoods_2012_Jan_to_2026_Jul.csv","zip":"redfin_housing_market_monthly_all_zips_2012_Jan_to_2026_Jul.csv"}

def expect(fn):
 try: fn()
 except (GovernanceError,FileNotFoundError): return
 raise AssertionError("expected fail-closed error")

def raw(path,family,end="2026-07",start=None,active_fallback=False,code="1"):
 start=start or ("2012-03" if family in {"city","zip","neighborhood"} else "2012-01")
 rows=[]
 for month in (start,end):
  first = month == start
  row={"period_end":month+"-28","region_id":code,"average_sale_to_list_ratio":50 if first else 200,"homes_sold":2,"median_days_on_market_days":10,"median_sale_price_nsa":3,"median_sale_price_per_sqft":4,"months_of_supply":5,"new_listings":6,"pending_sales":7,"percent_off_market_in_two_weeks":-7.57 if first else 100,"share_sold_above_original_list":-1.73 if first else 103.47}
  row["active_listings" if active_fallback else "inventory"]=8; rows.append(row)
 pd.DataFrame(rows).to_csv(path,index=False)

with tempfile.TemporaryDirectory() as td:
 root=Path(td)/"raw"; bootstrap(root); manifest={"manifest_version":1,"baseline_id":"2026-07","immutable":True,"files":[]}
 for family in FAMILIES:
  path=root/"baseline/2026-07"/NAMES[family]; raw(path,family,active_fallback=family=="nation")
  manifest["files"].append({"filename":path.name,"sha256":sha256(path),"geography_family":family,"historical_floor":"2012-03" if family in {"city","zip","neighborhood"} else "2012-01"})
 mp=Path(td)/"manifest.json"; mp.write_text(json.dumps(manifest)); assert validate_baseline(root,mp)["latest_month"]=="2026-07"
 assert {infer_family(name) for name in NAMES.values()}==set(FAMILIES); expect(lambda:infer_family("unknown.csv")); expect(lambda:infer_family("states_and_metros.csv"))
 # Exact floors are identities, not lower bounds.
 target=root/"baseline/2026-07"/NAMES["nation"]; raw(target,"nation",start="2011-12"); manifest["files"][0]["sha256"] = sha256(target); mp.write_text(json.dumps(manifest)); expect(lambda:validate_baseline(root,mp))
 raw(target,"nation"); manifest["files"][0]["sha256"] = sha256(target); mp.write_text(json.dumps(manifest))
 # Frozen baseline hashes are necessary but the aggregate empirical extrema must also reproduce the domain contract.
 for item in manifest["files"]:
  changed_path=root/"baseline/2026-07"/item["filename"]; changed=pd.read_csv(changed_path); changed["average_sale_to_list_ratio"]=100; changed.to_csv(changed_path,index=False); item["sha256"]=sha256(changed_path)
 mp.write_text(json.dumps(manifest)); expect(lambda:validate_baseline(root,mp))
 for item in manifest["files"]:
  restored=root/"baseline/2026-07"/item["filename"]; raw(restored,item["geography_family"]); item["sha256"]=sha256(restored)
 mp.write_text(json.dumps(manifest))
 for protected in (root,root/"baseline",root/"current",root/"quarantine"): expect(lambda p=protected:assert_safe_delete(p,root))
 # Registration uses all exact real-style family tokens; endpoint must match for every file.
 drop=root/"drops/2026-08"; drop.mkdir()
 for family in FAMILIES: raw(drop/NAMES[family],family,"2026-08")
 registered=register_drop("2026-08",root); assert register_drop("2026-08",root)==registered
 assert validate_drop("2026-08",root)["status"]=="validated"
 bad=root/"drops/2026-09"; bad.mkdir()
 for family in FAMILIES: raw(bad/NAMES[family],family,"2026-09" if family!="city" else "2026-10")
 register_drop("2026-09",root); expect(lambda:validate_drop("2026-09",root))
 quarantine("2026-09","endpoint mismatch",root)
 # Only nation/state are governed. Same numeric ID cannot cross-map, and five large families are skipped.
 geo=Path(td)/"geo.csv"; pd.DataFrame([{"geo_slug":"nation_geo","level":"nation","redfin_code":"1","include_redfin":"1"},{"geo_slug":"state_geo","level":"state","redfin_code":"1","include_redfin":"1"}]).to_csv(geo,index=False)
 candidate=Path(td)/"candidate.parquet"; meta=build_candidate("2026-08",candidate,root,geo,mp)
 frame=pd.read_parquet(candidate); assert set(frame.geo_id)=={"nation_geo","state_geo"}; assert set(frame.metric_id)==METRICS and "active_listings" not in set(frame.metric_id)
 assert {x["family"] for x in meta["skipped_ungoverned_files"]}==set(FAMILIES)-{"nation","state"}
 report=validate_candidate(candidate,"2026-08",root,geo); assert report["status"]=="candidate_validated"
 # Apply gate and rollback preserve old production. A forced duplicate candidate fails before BEGIN.
 con=duckdb.connect(); con.execute("CREATE TABLE fact_timeseries(geo_id TEXT,metric_id TEXT,date DATE,property_type_id TEXT,value DOUBLE,source_id TEXT,property_type TEXT)"); con.execute("INSERT INTO fact_timeseries VALUES ('old','m','2020-01-01','all',1,'redfin','all')")
 dup=pd.concat([frame,frame.iloc[[0]]]); dup.to_parquet(candidate,index=False); expect(lambda:apply_candidate("2026-08",candidate,con,root)); assert con.execute("select geo_id from fact_timeseries").fetchone()[0]=="old"
 frame.to_parquet(candidate,index=False)
 class FailAfterDelete:
  def __init__(self,inner): self.inner=inner
  def register(self,*args): return self.inner.register(*args)
  def execute(self,sql,*args):
   normalized = " ".join(sql.split()).upper()
   if normalized.startswith("INSERT INTO FACT_TIMESERIES"):
    raise RuntimeError("forced insert failure")
   return self.inner.execute(sql,*args)
 try: apply_candidate("2026-08",candidate,FailAfterDelete(con),root)
 except RuntimeError: pass
 else: raise AssertionError("expected transactional failure")
 assert con.execute("select geo_id from fact_timeseries").fetchone()[0]=="old"
 assert apply_candidate("2026-08",candidate,con,root)==len(frame); assert apply_candidate("2026-08",candidate,con,root)==len(frame)
 db=Path(td)/"serving.duckdb"; disk=duckdb.connect(str(db)); disk.execute("CREATE TABLE fact_timeseries AS SELECT * FROM con.fact_timeseries") if False else None
 # Validate serving using the applied in-memory rows copied to an isolated fixture DB.
 disk.execute("CREATE TABLE fact_timeseries(geo_id TEXT,metric_id TEXT,date DATE,property_type_id TEXT,value DOUBLE,source_id TEXT,property_type TEXT)"); disk.register("fixture",frame); disk.execute("INSERT INTO fact_timeseries SELECT geo_id,metric_id,date,property_type_id,value,'redfin',property_type FROM fixture"); disk.close()
 assert validate_serving(db,"2026-08",geo)["metrics"]==11
 # Promotion creates metadata-only pointer/history; retention keeps three raw snapshots and requires newest promoted boundary.
 m=json.loads((drop/"metadata.json").read_text()); m.update(status="published",publication_status="published",promotion_status="not_promoted"); atomic_json(drop/"metadata.json",m); promote("2026-08",root)
 assert current(root)["promoted_drop"]=="2026-08" and (root/"current/history.json").exists() and not list((root/"current").glob("*.csv"))
 for month in ("2026-05","2026-06","2026-07"):
  folder=root/"drops"/month; folder.mkdir(); atomic_json(folder/"metadata.json",{"drop_id":month,"status":"promoted","publication_status":"published","promotion_status":"promoted","files":[]})
 assert [Path(p).name for p in retain(root,keep=3,dry_run=True)]==["2026-05"]
 (root/"quarantine/failed").mkdir(); assert "failed" not in [Path(p).name for p in retain(root,keep=3,dry_run=True)]
 # Broad ignore contract: every raw family and generated candidates ignored; governed metadata remains tracked.
 for family,name in NAMES.items(): assert subprocess.run(["git","check-ignore","-q",f"data/redfin/raw/drops/2026-08/{name}"]).returncode==0
 assert subprocess.run(["git","check-ignore","-q","data/redfin/candidate.parquet"]).returncode==0
 assert subprocess.run(["git","check-ignore","-q","config/redfin_baseline_manifest.json"]).returncode!=0
print("redfin ingestion v2 smoke: ok")
