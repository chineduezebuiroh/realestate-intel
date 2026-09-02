"""Smoke 192: deterministic, read-only BPS pinned-release verification."""
from __future__ import annotations
import argparse, hashlib, json, tempfile, zipfile
from pathlib import Path
import pandas as pd
from jobs.monthly_refresh.bps_bootstrap import _resolve_columns, equivalence, inspect_zip, release_url, run, verify
from sources.census_bps.artifact import _numeric, load_registry

fixture=Path('tests/fixtures/census_bps/pinned_compiled_fixture.csv')
manifest=json.loads(Path('tests/fixtures/census_bps/pinned_evidence_manifest.json').read_text())
assert hashlib.sha256(fixture.read_bytes()).hexdigest()==manifest['fixture_csv_sha256']
assert release_url('2026-04').endswith('BPS_Compiled_File_202604.zip')
try: _resolve_columns(['period','year','month','location_type','state_fips','county_code','total_units'])
except ValueError as exc: assert 'county_fips exactly once' in str(exc)
else: raise AssertionError('three-digit county component accepted as full FIPS')
try: release_url('latest')
except ValueError: pass
else: raise AssertionError('mutable latest accepted')
with tempfile.TemporaryDirectory() as tmp:
 root=Path(tmp); archive=root/'fixture.zip'
 with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED) as z:
  z.writestr('compiled.csv',fixture.read_bytes())
 frame,raw=inspect_zip(archive)
 canonical,coverage,diagnostics,examples=verify(frame,release_month='2026-04')
 assert raw['selected_csv_sha256']==manifest['fixture_csv_sha256']
 assert diagnostics['authoritative_total_field']=='total_units'
 assert diagnostics['nonnumeric_token_counts']=={'D':1}
 assert diagnostics['authoritative_total_field_proven'] is True
 assert diagnostics['contract_gate']=='compiled_contract_proven'
 assert len(canonical)==3 and set(canonical.value)=={0.,17.,1234.}
 assert set(map(str,canonical.date))=={'2026-04-01'}
 assert len(coverage)==len(load_registry())==168
 assert coverage.present_in_release.sum()==3
 assert examples[0]['raw_total']=='D'
 # Deterministic verification output and byte-identical catalog.
 catalog=Path('config/artifact_catalog.json'); before=catalog.read_bytes()
 args=argparse.Namespace(release_month='2026-04',url=None,zip=archive,legacy=None,
                         output=root/'one',retrieved_at='2026-05-15T00:00:00Z')
 run(args)
 args.output=root/'two'; run(args)
 for name in ('raw_evidence_manifest.json','provider_contract_diagnostics.json',
              'nonnumeric_examples.json','geography_coverage.csv'):
  assert (root/'one'/name).read_bytes()==(root/'two'/name).read_bytes()
 assert catalog.read_bytes()==before
 assert json.loads(catalog.read_text()).get('accepted',{}).get('source',{}).get('bps') is None
 # Comparison categories are stable.
 legacy=canonical.copy(); legacy.loc[legacy.geo_id.eq('united_states__nation'),'value']=1200
 legacy=pd.concat([legacy,pd.DataFrame([{**canonical.iloc[0].to_dict(),'date':pd.Timestamp('2026-03-01').date()}])],ignore_index=True)
 detail,summary=equivalence(canonical,legacy)
 assert summary=={'exact_match_count':2,'provider_revision_count':1,'provider_only_count':0,
                  'prior_only_count':1,'identity_conflict_count':0}
 assert list(detail.comparison_category).count('PRIOR_ONLY')==1
for token in ('D','(X)',''):
 try: _numeric(token)
 except ValueError: pass
 else: raise AssertionError(f'unproven token admitted: {token!r}')
assert _numeric(None) is None and _numeric(float('nan')) is None and _numeric('0')==0.
print('[smoke] BPS pinned verification passed')
