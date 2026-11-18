"""Pure, bounded August ACS replacement; no production transport or rollback."""
from copy import deepcopy
from pathlib import Path

import duckdb
import pandas as pd

from core.source_artifacts.catalog import validate_catalog
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.publication import PublicationError
from core.source_artifacts.source_set_v2 import create_source_set_v2, validate_source_set_v2

VERSION = 'august_acs_forward_correction_v1'
CYCLE = 'monthly_cycle__2026-08__a9e022a980d29cd7'
REPOSITORY = 'chineduezebuiroh/realestate-intel'
OLD_SET = 'source_set__2026-08__v2__b429053765fb9fed'
OLD_MARKET = 'market__2026-08__r2__7793c3e80612bf80'
OLD_ACS = 'src__acs__2024-12__r1__366cf814efe424ef'
NEW_ACS = 'src__acs__2024-12__r1__7bad02240d41e76c'
NEW_SHA = 'a20d1f34d1b8e8d867dfcdc9beabca3ca0977f9f1b3ed97459cccd06411da869'
RESOLUTION = 'acs_family_resolution__9bb174c384bc46be9de6b3e0de367493806905b8ca4f5db487dabed60af3703f'
RECORD_PATH = f'config/acs_august_corrections/{CYCLE}.json'
JOURNAL_PATH = f'config/acs_august_corrections/{CYCLE}.publication.json'
PACKAGES = {'source_set':'source-set.tar','canonical_market':'canonical-market.tar'}
RESOLUTION_PATH = f'config/acs_family_resolutions/{RESOLUTION}.json'
READINESS_PATH = 'config/monthly_refresh_readiness.json'
CATALOG_PATH = 'config/artifact_catalog.json'
SOURCES = {'acs', 'bps', 'fred_macro', 'fred_unemp', 'ces', 'laus', 'redfin',
           'bea_gdp_qtr', 'bea_gdp_ann', 'census_nrc'}
BUILD_TIME = '2026-08-01T00:00:00Z'
BUILD_ID = VERSION


def seal(value, field, kind):
    value = deepcopy(value)
    value[field] = kind + '__' + sha256_json({k:v for k,v in value.items() if k != field})
    return value


def check(value, field, kind):
    if value != seal(value, field, kind):
        raise PublicationError('ACS correction semantic identity mismatch')


def exact(catalog, kind, identity):
    matches = [r for r in catalog['immutable_records']
               if r['object_type'] == kind and r['object_id'] == identity]
    if len(matches) != 1 or matches[0]['publication_state'] != 'published_immutable_verified':
        raise PublicationError('exact immutable correction object unavailable')
    return matches[0]


def origin(catalog):
    validate_catalog(catalog)
    accepted = catalog['accepted']
    if (accepted.get('source_set') != OLD_SET or accepted.get('canonical_market') != OLD_MARKET
            or accepted.get('source', {}).get('acs') != OLD_ACS
            or set(accepted['source']) != SOURCES):
        raise PublicationError('not the exact accepted August ACS predecessor')
    for source, identity in accepted['source'].items():
        if exact(catalog, 'source', identity)['metadata']['source_id'] != source:
            raise PublicationError('accepted source ownership mismatch')
    exact(catalog, 'source_set', OLD_SET); exact(catalog, 'canonical_market', OLD_MARKET)
    return deepcopy(accepted)


def successor(output: Path, *, old, catalog, resolution, manifest, config_hashes):
    """Preserve nine complete entries and BPS lineage; replace only ACS lineage."""
    validate_source_set_v2(old)
    if old['source_set_id'] != OLD_SET or set(old['included_source_inventory']) != SOURCES:
        raise PublicationError('wrong accepted Source Set')
    acs = exact(catalog, 'source', NEW_ACS)
    if (acs['package_sha256'] != NEW_SHA or resolution.get('resolution_id') != RESOLUTION
            or resolution.get('schema_version') != 'acs_family_resolution_record_v3'
            or resolution.get('output_artifact_id') != NEW_ACS
            or resolution.get('output_content_hash') != acs['artifact_content_hash']
            or resolution.get('output_package_sha256') != NEW_SHA
            or manifest.get('artifact_id') != NEW_ACS
            or manifest.get('artifact_content_hash') != acs['artifact_content_hash']):
        raise PublicationError('new ACS does not bind exact historical resolution')
    for entry in old['sources']:
        record = exact(catalog, 'source', entry['artifact_id'])
        if any(entry[k] != record[k] for k in ('artifact_content_hash','package_sha256')):
            raise PublicationError('accepted Source Set/catalog commitment drift')
    entries = deepcopy(old['sources'])
    entry = next(e for e in entries if e['source_id'] == 'acs')
    if entry['artifact_id'] != OLD_ACS:
        raise PublicationError('unexpected prior ACS entry')
    entry.update(artifact_id=NEW_ACS, logical_artifact_uri=acs['logical_artifact_uri'],
        package_sha256=NEW_SHA, artifact_content_hash=acs['artifact_content_hash'],
        provider_release_id=manifest['provider_release_id'], observation_max=manifest['observation_max'],
        validation_status='passed', monthly_status='refreshed', release_tag=acs['release_tag'],
        asset_id=acs['asset_id'], publication_receipt_id=acs['publication_receipt_id'],
        cycle_check_succeeded=True, carried_forward=False, carry_forward_policy_allowed=False)
    family = deepcopy(old['family_resolution'])
    if family.get('cycle_id') != CYCLE or {f['logical_source_id'] for f in family['families']} != {'acs','bps'}:
        raise PublicationError('unexpected predecessor family map')
    item = next(f for f in family['families'] if f['logical_source_id'] == 'acs')
    item.update(resolution_id=RESOLUTION, output_artifact_id=NEW_ACS,
        output_content_hash=acs['artifact_content_hash'], output_package_sha256=NEW_SHA,
        physical_sources=sorted([{k:p[k] for k in ('source_id','artifact_id','artifact_content_hash','package_sha256')}
                                 for p in resolution['parents']], key=lambda p:p['source_id']))
    return create_source_set_v2(output, target_month='2026-08', created_at=BUILD_TIME,
        builder_git_sha=BUILD_ID, entries=entries, config_hashes=config_hashes,
        contract_versions=old['contract_versions'], family_resolution=family)


def factual_delta(old_database, new_database):
    """Independent exact non-ACS fact comparison, including duplicates and types."""
    def facts(path):
        with duckdb.connect(str(path), read_only=True) as c:
            return c.execute("SELECT * FROM fact_timeseries WHERE source_id <> 'acs' ORDER BY ALL").fetchdf()
    try:
        pd.testing.assert_frame_equal(facts(old_database), facts(new_database), check_exact=True)
    except AssertionError as exc:
        raise PublicationError('non-ACS canonical facts changed') from exc


def review_check(review):
    check(review, 'review_id', VERSION + '_review')
    if (review['schema_version'] != VERSION or review['repository'] != REPOSITORY
            or review['resolution_id'] != RESOLUTION or review['acs_package_sha256'] != NEW_SHA
            or review['predecessor']['accepted']['source_set'] != OLD_SET
            or review['predecessor']['accepted']['canonical_market'] != OLD_MARKET
            or review['predecessor']['accepted']['source']['acs'] != OLD_ACS
            or set(review['predecessor']['accepted']['source']) != SOURCES
            or set(review['targets']) != {'source_set','canonical_market'}):
        raise PublicationError('invalid bounded ACS review')
    return review


def target_accepted(review):
    review_check(review)
    accepted = deepcopy(review['predecessor']['accepted'])
    accepted.update(source_set=review['targets']['source_set']['object_id'],
                    canonical_market=review['targets']['canonical_market']['object_id'])
    accepted['source']['acs'] = NEW_ACS
    return accepted


def atomic_delta(catalog, review):
    """One catalog value switches all three pointers; mixed states are rejected."""
    validate_catalog(catalog); review_check(review)
    acs = exact(catalog,'source',NEW_ACS)
    if acs['package_sha256'] != NEW_SHA or acs['metadata']['source_id'] != 'acs':
        raise PublicationError('accepted ACS target commitment mismatch')
    for kind, target in review['targets'].items():
        record = exact(catalog, kind, target['object_id'])
        if any(record[k] != target[k] for k in ('artifact_content_hash','package_sha256','metadata')):
            raise PublicationError('published successor commitments differ from review')
    wanted = target_accepted(review)
    if catalog['accepted'] == wanted:
        return deepcopy(catalog), False
    if catalog['accepted'] != review['predecessor']['accepted']:
        raise PublicationError('accepted predecessor changed or mixed promotion state')
    result = deepcopy(catalog); result['accepted'] = wanted
    validate_catalog(result)
    return result, True


def authorization(mode, value):
    return f'AUTHORIZE_AUGUST_ACS_{mode.upper()}__' + sha256_json(value)
