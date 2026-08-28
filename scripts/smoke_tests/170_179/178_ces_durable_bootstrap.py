"""Smoke 178: offline CES artifact publication, activation, resolution, idempotency."""
import copy
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
from core.source_artifacts.catalog import activate_source, add_record, empty_catalog
from core.source_artifacts.fixture_remote import CatalogPackageResolver, OfflineArtifactPublisher
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh.ces_bootstrap import (activation_summary, catalog_record,
 create_bootstrap_artifact, publication_metadata)
from sources.bls_ces.artifact import build_request_plan

def expect(error,fn):
 try: fn()
 except error: return
 raise AssertionError(f'expected {error.__name__}')

rows=[{'geo_id':'g','series_id':'SMS00000000000000001','metric_base':'ces_total_nonfarm','seasonal':'S'},
 {'geo_id':'g','series_id':'SMS00000000000000002','metric_base':'ces_total_private','seasonal':'S'},
 {'geo_id':'g','series_id':'SMS00000000000000003','metric_base':'ces_construction','seasonal':'S'}]
plan=build_request_plan(rows,start_year=2022,end_year=2024,acquisition_mode='ordinary_overlap',config_hashes={'x':'0'*64})
frame=pd.DataFrame([{'geo_id':'g','metric_id':m,'date':pd.Timestamp('2024-01-31').date(),'property_type_id':'all','value':v,'source_id':'ces','property_type':'all'} for m,v in
 [('ces_total_nonfarm_sa',100.),('ces_total_private_sa',80.),('ces_construction_sa',10.)]])
diag={'target_month':'2024-01'}
with TemporaryDirectory() as td:
 root=Path(td); first=create_bootstrap_artifact(root/'artifact',frame,plan,diag,retrieved_at='2026-01-01T00:00:00Z',artifact_created_at='2026-01-01T00:00:00Z')
 second=create_bootstrap_artifact(root/'artifact2',frame,plan,diag,retrieved_at='2026-01-01T00:00:00Z',artifact_created_at='2026-01-01T00:00:00Z')
 assert first['artifact_id']==second['artifact_id'] and first['data_sha256']==second['data_sha256']
 package=root/'artifact.tar'; info=build_publication_package(root/'artifact',package)
 metadata=publication_metadata(root/'artifact',info)
 # Fixture publisher needs deterministic remote fields normally supplied by GitHub.
 metadata.update(remote_backend='fixture',remote_repository='owner/repo',release_tag='source-artifact/ces/'+first['artifact_id'],
  release_id=10,asset_id=20,asset_filename=first['artifact_id']+'.tar',published_at='2026-01-01T00:00:00Z',verified_at='2026-01-01T00:00:01Z')
 publisher=OfflineArtifactPublisher(); uri=first['artifact_uri']; publisher.prepare(uri,package.read_bytes(),metadata); publisher.upload(uri); publisher.verify(uri); receipt=publisher.finalize(uri)
 record=catalog_record(first,receipt); catalog=add_record(empty_catalog(),record,receipt)
 assert 'ces' not in catalog['accepted']['source']
 # Same exact record/receipt is an idempotent no-op.
 assert add_record(catalog,record,receipt)==catalog
 active=activate_source(catalog,'ces',first['artifact_id']); assert active['accepted']['source']['ces']==first['artifact_id']
 summary=activation_summary(catalog,first['artifact_id'],root/'artifact'); assert summary['package_sha256']==info['package_sha256']
 resolver=CatalogPackageResolver(active,{20:package},{receipt['receipt_id']:receipt},root/'fresh')
 resolved=resolver.resolve(uri); assert (resolved/'manifest.json').is_file()
 assert resolver.resolve(uri)==resolved
 collision=OfflineArtifactPublisher(); collision.prepare(uri,package.read_bytes(),metadata)
 expect(IdentityCollisionError,lambda:collision.prepare(uri,package.read_bytes()+b'x',metadata))
 expect(Exception,lambda:activate_source(catalog,'ces','wrong-artifact'))
 before=copy.deepcopy(active); assert activate_source(active,'ces',first['artifact_id'])==before
print('Smoke 178 CES durable bootstrap passed')
