"""Deterministic contract smoke test for the Demand redundancy diagnostic."""
from __future__ import annotations
import hashlib
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd

from regime.experiments import demand_metric_redundancy as d
from regime.pandas_compat import MONTH_END

ROOT=Path(__file__).resolve().parents[3]

def fixture(root: Path) -> Path:
    run=root/"macro_regime_v1_0_release_20260810"; run.mkdir()
    dates=pd.date_range("2018-01-31",periods=72,freq=MONTH_END)
    rows=[]
    for gi,geo in enumerate(d.REVIEW_GEOS):
      for i,date in enumerate(dates):
       for mi,m in enumerate(d.METRICS):
        score=np.sin((i+mi)/7)+gi/20 if m!="laus_unemployment_rate" else -np.cos(i/8)+gi/20
        rows.append({"geo_id":geo,"evaluation_date":date,"canonical_metric_key":m,"metric_score":score})
    metrics=pd.DataFrame(rows); metrics.to_parquet(run/"aligned_metric_scores.parquet",index=False)
    weights={m:.1667 for m in d.METRICS}; inc=d._score(d._metric_long(metrics),weights,set(d.METRICS)); ds=inc[["geo_id","date","demand_dimension"]].drop_duplicates()
    dims=[]
    for r in ds.itertuples():
      dims.extend([{"geo_id":r.geo_id,"evaluation_date":r.date,"dimension":"demand","dimension_score":r.demand_dimension},
                   {"geo_id":r.geo_id,"evaluation_date":r.date,"dimension":"price","dimension_score":.2},
                   {"geo_id":r.geo_id,"evaluation_date":r.date,"dimension":"affordability","dimension_score":-.1},
                   {"geo_id":r.geo_id,"evaluation_date":r.date,"dimension":"capital_markets","dimension_score":.05}])
    dimensions=pd.DataFrame(dims); dimensions.to_parquet(run/"dimension_scores.parquet",index=False)
    ar=pd.read_csv(ROOT/"config/axis_registry.csv").query("axis=='demand' and enabled"); aw=dict(zip(ar.dimension,ar.dimension_weight)); ax,_=d._axis(dimensions,ds,aw)
    ax.rename(columns={"date":"evaluation_date","demand_axis":"axis_score"}).assign(axis="demand").to_parquet(run/"axis_scores.parquet",index=False)
    for name in ("source_metrics","features","normalized_features","regime_assignments"): pd.DataFrame({"fixture":[1]}).to_parquet(run/f"{name}.parquet",index=False)
    return run

def test_contract_and_bundle():
  contract,weights=d.production_contract(ROOT)
  assert set(contract.canonical_metric_key)==set(d.METRICS)
  labor=contract[contract.canonical_metric_key.isin(d.LABOR)]
  for metric in d.LABOR:
    q=labor[labor.canonical_metric_key.eq(metric)]
    assert set(q.feature_weight)=={.25,.35,.40}
  with tempfile.TemporaryDirectory() as td:
    root=Path(td); run=fixture(root); tables=d.build(run,ROOT); out=root/"out"; d.write_review(tables,out)
    assert set(tables)==set(d.OUTPUTS)
    assert not tables["demand_metric_pairwise_redundancy"].empty
    assert tables["demand_metric_pairwise_redundancy"].polarity_aligned.all()
    assert set(tables["demand_metric_parity_audit"].status)=={"pass"}
    assert tables["demand_metric_parity_audit"].max_abs_error.max() <= 1e-12
    assert set(tables["demand_metric_policy_registry"].policy)==set(d.POLICIES)
    decision=tables["demand_metric_decision_matrix"]
    assert len(decision)==4 and set(decision.Decision)=={"pending"}
    gov=tables["demand_metric_governance_status"].iloc[0]
    assert (gov.recommendation_state,gov.promotion_state,gov.human_decision)==("none","none","pending") and not bool(gov.automated_winner)
    # Every ablation preserves persisted metric scores; only membership/effective weights differ.
    x=d._metric_long(pd.read_parquet(run/"aligned_metric_scores.parquet"))
    for drops in d.ABLATIONS.values():
      q=d._score(x,weights,set(d.METRICS)-drops)
      merged=q.merge(x,on=["geo_id","date","metric"],suffixes=("_candidate","_persisted"))
      assert np.array_equal(merged.score_candidate.to_numpy(),merged.score_persisted.to_numpy())
      assert np.allclose(q.groupby(["geo_id","date"]).effective_weight.sum(),1)
    digest=lambda: hashlib.sha256((out/"demand_metric_decision_matrix.csv").read_bytes()).hexdigest()
    before=digest(); d.write_review(tables,out); assert before==digest()

def test_fail_closed():
  with tempfile.TemporaryDirectory() as td:
    root=Path(td); run=fixture(root)
    m=pd.read_parquet(run/"aligned_metric_scores.parquet"); m=m[m.geo_id.ne(d.REVIEW_GEOS[-1])]; m.to_parquet(run/"aligned_metric_scores.parquet",index=False)
    try: d.build(run,ROOT)
    except ValueError as e: assert "coverage missing" in str(e)
    else: raise AssertionError("missing governed geography did not fail closed")

def test_movement_residual_audit():
  geo="fixture_county__county"; dates=pd.to_datetime(["2020-01-31","2020-02-29","2020-03-31"])
  weights={"a":.5,"b":.5}
  def scored(rows):
    raw=pd.DataFrame(rows,columns=["geo_id","date","metric","score"])
    return d._score(raw,weights,{"a","b"})
  complete=scored([(geo,date,metric,float(i+mi)) for i,date in enumerate(dates) for mi,metric in enumerate(("a","b"))])
  dims=complete[["geo_id","date","demand_dimension"]].drop_duplicates().rename(columns={"demand_dimension":"value"})
  audit=d.build_movement_audit(complete,dims)["demand_movement_residual_audit"]
  assert audit.abs_movement_residual.max() <= d.TOL
  assert not audit.metric_set_changed.any() and not audit.any_effective_weight_change.any()

  missing=scored([(geo,dates[0],"a",0.),(geo,dates[0],"b",1.),
                  (geo,dates[1],"a",1.),
                  (geo,dates[2],"a",2.),(geo,dates[2],"b",3.)])
  dims=missing[["geo_id","date","demand_dimension"]].drop_duplicates().rename(columns={"demand_dimension":"value"})
  audit=d.build_movement_audit(missing,dims)["demand_movement_residual_audit"].set_index("date")
  assert audit.loc[dates[1],"metric_set_changed"] and audit.loc[dates[1],"any_effective_weight_change"]
  assert audit.loc[dates[2],"metric_set_changed"] and audit.loc[dates[2],"any_effective_weight_change"]
  assert audit.loc[dates[2],"any_nonconsecutive_metric_observation"]

  # Force a residual solely to exercise exact consecutive score/weight/interaction reconciliation.
  bad_dims=dims.copy(); bad_dims.loc[bad_dims.date.eq(dates[1]),"value"] += .01
  tables=d.build_movement_audit(missing,bad_dims)
  dec=tables["demand_movement_effect_decomposition"]
  reconciled=dec[dec.decomposition_status.eq("reconciled")]
  assert not reconciled.empty and reconciled.decomposition_error.max() <= d.TOL
  skipped=dec[(dec.date.eq(dates[2])) & dec.metric.eq("b")].iloc[0]
  assert skipped.decomposition_status == "nonconsecutive" and skipped.months_between_observations == 2
  assert pd.isna(skipped.score_t_minus_1)

if __name__=="__main__": test_contract_and_bundle(); test_fail_closed(); test_movement_residual_audit(); print("demand metric redundancy diagnostic smoke passed")
