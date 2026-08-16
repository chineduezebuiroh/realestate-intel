#!/usr/bin/env python3
"""Smoke 135: final Affordability MA closed-grid and fail-closed contracts."""
from pathlib import Path
import hashlib,tempfile,re
import numpy as np,pandas as pd
from regime._01_feature_engine import _compute_feature
from regime.diagnostics.affordability_feature_weight_calibration import REVIEW_GEOS,TARGET_METRICS
from regime.diagnostics.affordability_final_ma_calibration import SCENARIOS,EXPORTS,build,load_run,write_review

def fixture():
 dates=pd.date_range('2014-01-31',periods=84,freq='ME'); source=[]; dims=[]; axes=[]
 for j,geo in enumerate(REVIEW_GEOS[:2]):
  for i,date in enumerate(dates):
   vals={"price_to_income":5*(1+.04*np.sin(i/7+j/20)+.001*i),"payment_burden":.28*(1+.08*np.sin(i/5+j/20)+.001*i)}
   for m,v in vals.items(): source.append(dict(geo_id=geo,date=date,metric_key=m,value=v))
   dims.append(dict(geo_id=geo,date=date,dimension='affordability',dimension_score=np.sin(i/9)))
   axes.append(dict(geo_id=geo,date=date,axis='demand',axis_score=np.sin(i/11)))
 return dict(source_metrics=pd.DataFrame(source),dimension_scores=pd.DataFrame(dims),axis_scores=pd.DataFrame(axes))
protected=[Path('config/feature_registry.csv'),Path('config/normalization_registry.csv'),Path('config/metric_dimension_registry.csv'),Path('config/axis_registry.csv')]
before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
assert tuple(x[0] for x in SCENARIOS)==('MA12__P3','MA12__P4','MA9__P3','MA9__P4')
assert {(x[1],x[2],x[3:]) for x in SCENARIOS}=={(12,'P3',(.4,.15,.45)),(12,'P4',(.35,.2,.45)),(9,'P3',(.4,.15,.45)),(9,'P4',(.35,.2,.45))}
assert not any(x[1] in (10,11,6) or x[2] in ('P2','P5') for x in SCENARIOS)
t=build(fixture(),Path('.')); assert set(EXPORTS).issubset(t)
assert set(t['raw_cycle_comparison'].reference_type)=={'oriented_raw_cycle'}
assert set(t['feature_reference_comparison'].reference_type)=={'level_feature_reference','short_feature_reference','long_feature_reference'}
assert set(t['controlled_ma_comparisons'].comparison_type)=={'P3_fixed','P4_fixed'} and set(t['policy_comparisons'].comparison_type)=={'MA12_fixed','MA9_fixed'}
g=t['governance_status'].iloc[0]; assert g.raw_cycle_orientation=='governed' and not g.derive_first_lineage_changed and g.candidate_grid_closed and not g.production_policy_changed
# Shared constructor proves complete-calendar MA9 and fixed calendar lags.
q=pd.DataFrame({'date':pd.date_range('2020-01-31',periods=22,freq='ME'),'value':np.arange(22.),'metric_origin':'x'}); q.loc[5,'value']=np.nan
assert _compute_feature(q,'ma_level','9m','x').iloc[:8].isna().all(); assert _compute_feature(q,'ma_pct_change','9m/lag3m','x').iloc[:11].isna().all(); assert _compute_feature(q,'ma_pct_change','9m/lag12m','x').iloc[:20].isna().all()
with tempfile.TemporaryDirectory() as d:
 out=Path(d); write_review(t,out); assert all((out/f'affordability_final_ma_{x}.csv').is_file() for x in EXPORTS)
 svgs=list(out.glob('*.svg')); assert svgs
 for p in svgs:
  text=p.read_text().lower(); assert '<path' in text and not re.search(r'(?<![a-z])(nan|[+-]?inf)(?![a-z])',text)
  coords=[float(x) for x in re.findall(r'(?<=[ml ])-?\d+(?:\.\d+)?',text)]; assert coords and all(np.isfinite(coords))
 chronology=next(out.glob('*price_to_income_dc_chronology.svg')).read_text(); assert chronology.count('class="series"')==4 and len(set(re.findall(r'd="([^"]+)"',chronology)))>=4
 # Row-order changes cannot affect deterministic rendering.
 shuffled={k:(v.sample(frac=1,random_state=7).reset_index(drop=True) if isinstance(v,pd.DataFrame) else v) for k,v in t.items()}
 out2=out/'reordered'; write_review(shuffled,out2); assert (out2/'affordability_final_ma_price_to_income_dc_chronology.svg').read_bytes()==(out/'affordability_final_ma_price_to_income_dc_chronology.svg').read_bytes()
 # Missing calendar months create separate SVG path subpaths rather than bridges.
 damaged={**t}; damaged['metric_chronology']=t['metric_chronology'].copy(); mask=(damaged['metric_chronology'].scenario_id=='MA12__P3')&(damaged['metric_chronology'].metric=='price_to_income')&(damaged['metric_chronology'].geo_id==REVIEW_GEOS[0]); idx=damaged['metric_chronology'][mask].index[30]; damaged['metric_chronology'].loc[idx,'metric_score']=np.nan
 out3=out/'gap'; write_review(damaged,out3); gap=(out3/'affordability_final_ma_price_to_income_dc_chronology.svg').read_text(); p3=re.search(r'data-series="MA12__P3" d="([^"]+)"',gap).group(1); assert p3.count('M')>=2
 assert 'independent y-domains' in (out/'affordability_final_ma_price_to_income_dc_raw_cycle.svg').read_text()
 try: load_run(out/'missing')
 except FileNotFoundError as e: assert 'no substitute permitted' in str(e)
 else: raise AssertionError('did not fail closed')
assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
print('Smoke 135 passed: closed grid, derive-first invariant, governed orientation, calendar MA/lags, comparisons, SVGs, governance, fail closed')
