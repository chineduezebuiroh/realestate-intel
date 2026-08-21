from pathlib import Path
import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import _build_axis_weights, score_axes
from regime.contribution_lineage import build_contribution_lineage

ROOT=Path(__file__).resolve().parents[3]
cfg=load_regime_config(validate=True)
truth=lambda s:s.astype(str).str.lower().eq('true')
active=cfg.metric_dimensions[truth(cfg.metric_dimensions.enabled)&~truth(cfg.metric_dimensions.diagnostic_only)&truth(cfg.metric_dimensions.macro_enabled)]
canon=active.drop_duplicates(['dimension','canonical_metric_key'])
assert set(canon.query("dimension=='demand'").canonical_metric_key)=={'labor_force','employment','laus_unemployment_rate'}
assert set(canon.query("dimension=='market_context'").canonical_metric_key)=={'population','median_household_income','gdp_annual'}
assert active.demand_block.eq('').all() and active.block_weight.eq('').all()
laus=cfg.features[cfg.features.metric_key.isin({'laus_labor_force','laus_employment','laus_unemployment_rate'})]
for _,g in laus.groupby('metric_key'):
 assert dict(zip(g.feature_type,g.feature_weight.astype(float)))=={'level':.4,'short_term_change':.15,'long_term_change':.45}
 assert dict(zip(g.feature_type,g.feature_window))=={'level':'9m','short_term_change':'9m/lag3m','long_term_change':'9m/lag12m'}
axis=_build_axis_weights(); demand=axis.query("axis=='demand'")
assert dict(zip(demand.dimension,demand.dimension_weight))=={'demand':.65,'price':.175,'affordability':.075,'capital_markets':.10}
assert 'market_context' not in set(axis.dimension) and np.isclose(demand.dimension_weight.sum(),1)

# Structural rows materialize a dimension but are inner-excluded before axis
# availability normalization. Shuffling every input proves order independence.
date=pd.Timestamp('2025-01-31')
metrics=pd.DataFrame([
 {'geo_id':'g__county','evaluation_date':date,'metric_date':date,'canonical_metric_key':k,'metric_score':s,'feature_count':3,'feature_weight_sum':1,'min_feature_score':s,'max_feature_score':s,'metric_age_days':age}
 for k,s,age in [('labor_force',.3,0),('employment',.6,0),('laus_unemployment_rate',-.3,0),('population',.9,365),('median_household_income',.7,365),('gdp_annual',.8,365),('median_sale_price',.2,0),('median_ppsf',.2,0),('price_to_income',-.1,0),('payment_burden',-.1,0),('mortgage_30y',.3,0),('mortgage_15y',.3,0),('fedfunds',.3,0),('treasury_10y',.3,0),('spread_10y_2y',.3,0),('spread_10y_fedfunds',.3,0)]])
dims=score_dimensions(metrics); axes=score_axes(dims)
no_context=score_axes(dims[dims.dimension.ne('market_context')])
pd.testing.assert_frame_equal(axes,no_context)
shuffled=score_axes(score_dimensions(metrics.sample(frac=1,random_state=8))).sort_values(['geo_id','date','axis']).reset_index(drop=True)
pd.testing.assert_frame_equal(axes,shuffled)

# Contribution surfaces retain the governed arithmetic and parent chain.
features=pd.DataFrame([{'geo_id':'g__county','date':date,'canonical_metric_key':'labor_force','feature_key':k,'feature_score':s,'source_family':'laus','raw_feature_value':1,'percentile':.5,'normalization_method':'expanding_percentile','score_direction':'positive','lookback_periods':'120','min_periods':'36'} for k,s in [('laus_labor_force_level',.2),('laus_labor_force_short',.4),('laus_labor_force_long',.6)]])
lineage=build_contribution_lineage(features,metrics,dims)
for frame in lineage.values():
 assert {'score','configured_weight','effective_weight','weighted_contribution','parent_identifier'}<=set(frame)
assert np.isclose(lineage['feature_contributions'].effective_weight.sum(),1)

norm=pd.read_csv(ROOT/'config/normalization_registry.csv',dtype=str).fillna('')
laus_norm=norm[(norm.policy_scope=='source_family')&(norm.policy_key=='laus')].iloc[0]
assert (laus_norm.normalization_method,laus_norm.lookback_periods,laus_norm.min_periods,laus_norm.clip_low,laus_norm.clip_high)==('expanding_percentile','120','36','0.01','0.99')
print('smoke 126 passed')
