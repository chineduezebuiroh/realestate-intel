"""Smoke 193: coherent, read-only BPS provisional verification."""
from __future__ import annotations
import argparse, hashlib, json, tempfile
from pathlib import Path
import pandas as pd
from jobs.monthly_refresh.bps_provisional_verification import LEVELS, provisional_applicable_registry, read_member, release_from_url, resolve_inputs, run, verify
from jobs.monthly_refresh.bps_bootstrap import equivalence
from sources.census_bps.artifact import load_registry

base='https://www2.census.gov/econ/bps/'
urls={'state':base+'State/st2607c.txt','county':base+'County/co2607c.txt','cbsa_metro':base+'CBSA%20%28beginning%20Jan%202024%29/cbsa2607c.txt'}
release,pinned=resolve_inputs(urls)
assert release=='2607' and pinned==urls
assert release_from_url('state',urls['state'])=='2607'
try: resolve_inputs({**urls,'county':base+'County/co2606c.txt'})
except ValueError as exc: assert 'mixed provisional releases' in str(exc)
else: raise AssertionError('mixed provider state accepted')
try: release_from_url('county',base+'County/not-current.txt')
except ValueError: pass
else: raise AssertionError('invalid discovery result accepted')
fixture=Path('tests/fixtures/census_bps/provisional_2607')
files={'state':fixture/'st2607c.txt','county':fixture/'co2607c.txt','cbsa_metro':fixture/'cbsa2607c.txt'}
frames={level:read_member(files[level],level)[0] for level in LEVELS}
canonical,coverage,outside,diagnostics,examples=verify(frames,release_id=release)
assert len(canonical)==2 and set(canonical.value)=={8.}
assert set(canonical.geo_id)=={'california__state','alameda_county_ca__county'}
assert diagnostics['present_governed_geography_count']==2
assert diagnostics['configured_geography_count']==168
assert diagnostics['provisional_applicable_geography_count']==167
assert diagnostics['present_provisional_applicable_geography_count']==2
assert diagnostics['out_of_governance_geography_count']==1
assert diagnostics['nonnumeric_or_unavailable_token_counts']=={}
assert diagnostics['current_month_only'] is True and examples==[]
assert list(map(str,canonical.date))==['2026-07-01','2026-07-01']
# The physical provisional family covers every governed state/county, not nation.
registry=load_registry(); applicable=provisional_applicable_registry(registry)
assert len(registry)==168 and len(applicable)==167
assert {r['geo_id'] for r in registry}-{r['geo_id'] for r in applicable}=={'united_states__nation'}
expanded={level: frames[level].iloc[0:0].copy() for level in LEVELS}
for item in applicable:
 level='state' if item['provider_location_type']=='State' else 'county'
 row=frames[level].iloc[0].copy()
 if level=='state': row['state_fips']=item['provider_identifier']
 else:
  row['state_fips']=item['provider_identifier'][:2]
  row['county_fips_3']=item['provider_identifier'][2:]
 expanded[level]=pd.concat([expanded[level],row.to_frame().T],ignore_index=True)
expanded['cbsa_metro']=frames['cbsa_metro']
full,full_coverage,full_outside,full_diagnostics,_=verify(expanded,release_id=release)
assert len(full)==167 and 'united_states__nation' not in set(full.geo_id)
assert full_diagnostics['contract_gate']=='provider_layout_and_mapping_verified'
assert full_diagnostics['present_provisional_applicable_geography_count']==167
assert not full_coverage.loc[full_coverage.geo_id.eq('united_states__nation'),'provisional_applicable'].item()
missing_one={**expanded,'county':expanded['county'].iloc[1:].reset_index(drop=True)}
assert verify(missing_one,release_id=release)[3]['contract_gate'].startswith('blocked_')
# Existing provisional values compare through the same governed categories.
detail,summary=equivalence(canonical,canonical.assign(source_id='census_bps_provisional'))
assert summary['exact_match_count']==2 and len(detail)==2
# Out-of-registry CBSA observations are diagnostic inventory, not governed conflicts.
outside_legacy=pd.DataFrame([{**canonical.iloc[0].to_dict(),'geo_id':'41860__metro'}])
detail,summary=equivalence(canonical.iloc[0:0],outside_legacy)
assert summary['out_of_governance_count']==1 and summary['identity_conflict_count']==0
assert detail.comparison_category.item()=='OUT_OF_GOVERNANCE'
# Identical duplicates collapse; conflicting values fail closed.
duplicate={**frames,'state':pd.concat([frames['state'],frames['state']],ignore_index=True)}
assert verify(duplicate,release_id=release)[3]['identical_duplicate_key_count']==1
conflict=frames['state'].copy(); conflict.loc[0,'units_1']='99'
try: verify({**duplicate,'state':pd.concat([frames['state'],conflict],ignore_index=True)},release_id=release)
except ValueError as exc: assert 'conflicting duplicate' in str(exc)
else: raise AssertionError('conflicting provisional duplicate accepted')
# A missing component is diagnosed and never manufactured as zero.
token=frames['county'].copy(); token.loc[0,'units_1']='(X)'
missing=verify({**frames,'county':token},release_id=release)
assert missing[3]['nonnumeric_or_unavailable_token_counts']=={'(X)':1}
assert 'alameda_county_ca__county' not in set(missing[0].geo_id)
# Fixed pinned inputs yield byte-identical evidence and cannot touch catalog/pointers.
catalog=Path('config/artifact_catalog.json'); before=hashlib.sha256(catalog.read_bytes()).hexdigest()
databases=[Path('data/market_public.duckdb'),Path('data/market_serving.duckdb')]
database_hashes={path:hashlib.sha256(path.read_bytes()).hexdigest() for path in databases}
with tempfile.TemporaryDirectory() as tmp:
 root=Path(tmp)
 for name in ('one','two'):
  args=argparse.Namespace(output=root/name,retrieved_at='2026-09-02T00:00:00Z',legacy=None,
    state_url=urls['state'],county_url=urls['county'],cbsa_metro_url=urls['cbsa_metro'],
    state_file=files['state'],county_file=files['county'],cbsa_metro_file=files['cbsa_metro'])
  run(args)
 for name in ('raw_evidence_manifest.json','provider_contract_diagnostics.json','geography_coverage.csv','out_of_governance_geographies.csv'):
  assert (root/'one'/name).read_bytes()==(root/'two'/name).read_bytes()
assert hashlib.sha256(catalog.read_bytes()).hexdigest()==before
assert {path:hashlib.sha256(path.read_bytes()).hexdigest() for path in databases}==database_hashes
assert json.loads(catalog.read_text()).get('accepted',{}).get('source',{}).get('bps') is None
print('[smoke] BPS provisional verification passed')
