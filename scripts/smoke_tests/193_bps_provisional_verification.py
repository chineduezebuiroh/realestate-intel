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
assert diagnostics['configured_geography_count']==221
assert diagnostics['provisional_applicable_geography_count']==220
assert diagnostics['present_provisional_applicable_geography_count']==2
assert diagnostics['out_of_governance_geography_count']==15
assert 'united_states__nation' not in set(canonical.geo_id)
national=outside[outside.classification.eq('PROVIDER_NATIONAL_SUMMARY')]
assert national[['provider_location_type','provider_identifier','geo_name']].to_dict('records')==[
    {'provider_location_type':'Country','provider_identifier':'US','geo_name':'United States'}]
regions=outside[outside.classification.eq('PROVIDER_REGION_SUMMARY')]
assert regions[['provider_identifier','geo_name']].to_records(index=False).tolist()==[
    ('R1','Northeast Region'),('R2','Midwest Region'),('R3','South Region'),('R4','West Region')]
divisions=outside[outside.classification.eq('PROVIDER_DIVISION_SUMMARY')]
assert divisions[['provider_identifier','geo_name']].to_records(index=False).tolist()==[
    ('D1','New England Division'),('D2','Middle Atlantic Division'),
    ('D3','East North Central Division'),('D4','West North Central Division'),
    ('D5','South Atlantic Division'),('D6','East South Central Division'),
    ('D7','West South Central Division'),('D8','Mountain Division'),('D9','Pacific Division')]
assert diagnostics['raw_provider_geography_classification_counts']=={
    'PROVIDER_NATIONAL_SUMMARY':1,'PROVIDER_REGION_SUMMARY':4,
    'PROVIDER_DIVISION_SUMMARY':9,'GOVERNED_CANDIDATE':2,'OUT_OF_GOVERNANCE':1}
assert diagnostics['nonnumeric_or_unavailable_token_counts']=={}
assert diagnostics['current_month_only'] is True and examples==[]
assert list(map(str,canonical.date))==['2026-07-01','2026-07-01']
# The physical provisional family covers every governed state/county, not nation.
registry=load_registry(); applicable=provisional_applicable_registry(registry)
assert len(registry)==221 and len(applicable)==220
assert {r['geo_id'] for r in registry}-{r['geo_id'] for r in applicable}=={'united_states__nation'}
expanded={level: frames[level].iloc[0:0].copy() for level in LEVELS}
for item in applicable:
 level={'State':'state','County':'county','Metro':'cbsa_metro'}[item['provider_location_type']]
 row=frames[level].iloc[0].copy()
 if level=='state': row['state_fips']=item['provider_identifier']
 elif level=='county':
  row['state_fips']=item['provider_identifier'][:2]
  row['county_fips_3']=item['provider_identifier'][2:]
 else: row['cbsa_code']=item['provider_identifier']
 expanded[level]=pd.concat([expanded[level],row.to_frame().T],ignore_index=True)
# Preserve all 14 exact provider aggregates alongside governed physical rows.
expanded['state']=pd.concat([expanded['state'],frames['state'].loc[
    ~frames['state'].state_fips.str.fullmatch(r'\d{1,2}')]],ignore_index=True)
full,full_coverage,full_outside,full_diagnostics,_=verify(expanded,release_id=release)
assert len(full)==220 and 'united_states__nation' not in set(full.geo_id)
assert len(full_outside[full_outside.classification.str.startswith('PROVIDER_')])==14
assert full_diagnostics['contract_gate']=='provider_layout_exact_mapping_and_stable_coverage_verified'
assert full_diagnostics['present_provisional_applicable_geography_count']==220
assert not full_coverage.loc[full_coverage.geo_id.eq('united_states__nation'),'provisional_applicable'].item()
missing_one={**expanded,'county':expanded['county'].iloc[1:].reset_index(drop=True)}
assert verify(missing_one,release_id=release)[3]['contract_gate'].startswith('blocked_')
# Release-variable Metro omission is diagnosed but does not weaken stable levels.
missing_metro={**expanded,'cbsa_metro':expanded['cbsa_metro'].iloc[3:].reset_index(drop=True)}
metro_diagnostics=verify(missing_metro,release_id=release)[3]
assert metro_diagnostics['contract_gate']=='provider_layout_exact_mapping_and_stable_coverage_verified'
assert metro_diagnostics['missing_provisional_applicable_geography_count']==3
assert {item['provider_location_type'] for item in
        metro_diagnostics['missing_provisional_applicable_geographies']}=={'Metro'}
# Exact 2607 promoted contract: 217 physical rows, 220 logical, three named CBSAs absent.
promoted={**expanded,'cbsa_metro':expanded['cbsa_metro'][~expanded['cbsa_metro'].cbsa_code.isin(
    ['32300','36140','42020'])].reset_index(drop=True)}
promoted_rows,_,promoted_outside,promoted_diagnostics,_=verify(promoted,release_id=release)
assert len(promoted_rows)==217 and promoted_diagnostics['canonical_row_count']==217
assert promoted_diagnostics['provisional_applicable_geography_count']==220
assert promoted_diagnostics['present_provisional_applicable_geography_count_by_type']=={
    'State':5,'County':162,'Metro':50}
assert [(item['provider_identifier'],item['geo_id']) for item in
        promoted_diagnostics['missing_provisional_applicable_geographies']]==[
    ('32300','martinsville_va_metro_area__cbsa_metro'),
    ('36140','ocean_city_nj_metro_area__cbsa_metro'),
    ('42020','san_luis_obispo_ca_metro_area__cbsa_metro')]
assert set(promoted_outside.loc[promoted_outside.classification.eq(
    'PROVIDER_NATIONAL_SUMMARY'),'provider_identifier'])=={'US'}
# The explicit aggregate inventory does not admit arbitrary alphabetic codes or
# any contradictory national, region, or division tuple.
for code,field,value in [('US','geo_name','Not United States'),
                         ('R1','region_code','2'),('R1','division_code','1'),
                         ('D1','region_code','2'),('D1','division_code','2')]:
 malformed=frames['state'].loc[frames['state'].state_fips.eq(code)].copy()
 malformed.loc[:,field]=value
 try: verify({**frames,'state':malformed},release_id=release)
 except ValueError as exc: assert 'unsafe provisional state summary' in str(exc)
 else: raise AssertionError('contradictory provisional aggregate accepted')
for value in ('XX','R5','D0','REGION1','D10'):
 malformed=frames['state'].loc[frames['state'].state_fips.eq('US')].copy()
 malformed.loc[:,'state_fips']=value
 try: verify({**frames,'state':malformed},release_id=release)
 except ValueError as exc: assert 'unsafe provisional state summary' in str(exc)
 else: raise AssertionError('malformed provisional state identity accepted')
# County and CBSA identifiers retain their strict numeric-only validation.
for level,field in [('county','county_fips_3'),('cbsa_metro','cbsa_code')]:
 malformed=frames[level].copy(); malformed.loc[:,field]='X1'
 try: verify({**frames,level:malformed},release_id=release)
 except ValueError as exc: assert f'unsafe provisional {"CBSA" if level == "cbsa_metro" else "county"} identifier' in str(exc)
 else: raise AssertionError(f'malformed provisional {level} identity accepted')
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
