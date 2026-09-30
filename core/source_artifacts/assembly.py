from __future__ import annotations
from pathlib import Path
import duckdb, pandas as pd
from .models import CANONICAL_COLUMNS, CANONICAL_KEY
from .source_set import validate_source_set

def assemble(source_set_path: Path, output: Path, resolver, *, metric_registry: Path|None=None, geo_manifest: Path|None=None) -> dict:
    if output.resolve() in {Path("data/market.duckdb").resolve(),Path("data/market_serving.duckdb").resolve()}: raise ValueError("production database path prohibited in vertical-slice mode")
    checked=validate_source_set(source_set_path,resolver); frames=[]
    metric_owner={}; governed_geo=None
    if metric_registry:
        reg=pd.read_csv(metric_registry,dtype=str); metric_owner=dict(zip(reg.metric_id,reg.source_id))
    if geo_manifest:
        governed_geo=set(pd.read_csv(geo_manifest,dtype=str).geo_slug)
    for sid,directory in checked["resolved"].items():
        frame=pd.read_parquet(directory/"data.parquet")
        if metric_owner:
            bad=[m for m in frame.metric_id.unique() if metric_owner.get(m)!=sid]
            if bad: raise ValueError(f"unauthorized metric ownership for {sid}: {bad}")
        if governed_geo is not None and not set(frame.geo_id).issubset(governed_geo): raise ValueError(f"ungoverned geography in {sid}")
        frames.append(frame)
    facts=pd.concat(frames,ignore_index=True).sort_values(CANONICAL_KEY,kind="mergesort")
    if facts[CANONICAL_KEY].duplicated().any(): raise ValueError("unexpected cross-source canonical key collision")
    output.parent.mkdir(parents=True,exist_ok=True)
    if output.exists(): output.unlink()
    con=duckdb.connect(str(output)); con.register("facts",facts)
    con.execute('CREATE TABLE fact_timeseries AS SELECT CAST(geo_id AS VARCHAR) geo_id, CAST(metric_id AS VARCHAR) metric_id, CAST(date AS DATE) date, CAST(property_type_id AS VARCHAR) property_type_id, CAST("value" AS DOUBLE) AS "value", CAST(source_id AS VARCHAR) source_id, CAST(property_type AS VARCHAR) property_type FROM facts ORDER BY geo_id,metric_id,date,property_type_id')
    con.execute("CREATE UNIQUE INDEX fact_timeseries_key ON fact_timeseries(geo_id,metric_id,date,property_type_id)")
    con.execute("CREATE TABLE source_artifact_metadata(source_id VARCHAR, artifact_id VARCHAR, source_set_id VARCHAR)")
    for entry in checked["source_set"]["sources"]: con.execute("INSERT INTO source_artifact_metadata VALUES (?,?,?)",[entry["source_id"],entry["artifact_id"],checked["source_set"]["source_set_id"]])
    rows=con.execute("select count(*) from fact_timeseries").fetchone()[0]; con.close()
    return {"status":"passed","rows":rows,"sources":sorted(facts.source_id.unique()),"partial_vertical_slice":True}
