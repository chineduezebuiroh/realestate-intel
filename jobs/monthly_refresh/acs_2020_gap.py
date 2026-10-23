"""Proposed documented standard ACS1 2020 nonpublication; never approval."""
from pathlib import Path

from core.source_artifacts.hashing import sha256_file, sha256_json
from jobs.monthly_refresh.acs_2024_compatibility import read_json
from jobs.monthly_refresh.acs_scope import GAP_VERSION, check_identity, identify
from sources.census_acs.artifact import VARIABLES, geography_plan, governed_config_hashes

SOURCE_PATH = 'docs/audit/acs1_2020_nonpublication_sources.json'
SOURCE_SHA256 = '4ad2769dc2da426cd47db8dafe7bb77b95d72dc574c4d3caa72be8596aad8b8b'
GAP_FILE = 'census_acs1.2020.gap.proposal.json'
CANDIDATE_FILE = 'census_acs1.scope.if-gap-approved.json'


def generate_gap(root: Path) -> dict:
    """Bind independently retrieved documentation and exact current request scope."""
    root = Path(root)
    if sha256_file(root / SOURCE_PATH) != SOURCE_SHA256:
        raise ValueError('provider documentation capture changed or unsupported')
    capture = read_json(root / SOURCE_PATH)
    plan, excluded = geography_plan(root / 'config/geo_manifest.generated.csv')
    if len(plan) != 223 or len(excluded) != 10:
        raise ValueError('2020 gap review requires the existing 223/10 request plan')
    return identify(GAP_VERSION, {'schema_version': GAP_VERSION, 'namespace': 'production',
        'source_id': 'census_acs1', 'product': 'acs/acs1', 'vintage': 2020,
        'status': 'proposed', 'approval_provenance': None,
        'provider_evidence': {
            'basis': 'explicit_official_provider_nonpublication_documentation',
            'classification': 'standard_whole_vintage_not_published',
            'requested_provider_release_id': 'acs/acs1:2020',
            'requested_endpoint_identity': 'https://api.census.gov/data/2020/acs/acs1',
            'provider_published_dataset_identifier': None,
            'endpoint_response_verified': False,
            'capture': {'path': SOURCE_PATH, 'sha256': SOURCE_SHA256},
            'references': capture['sources'],
            'excluded_substitutions': ['2020_acs1_experimental_estimates', '2016_2020_acs5'],
            'not_inferred_from': ['missing_acquisition', 'http_404', 'http_204', 'geography_specific_omission'],
            'request_scope': {'variables': dict(VARIABLES), 'source_id': 'census_acs1', 'vintage': 2020,
                'requested_geographies': len(plan), 'membership_sha256': sha256_json(plan),
                'excluded_geographies': excluded, 'config_hashes': governed_config_hashes(root)},
            'limitations': capture['limitations']},
        'scope_approval': 'separate_human_review_required', 'activation': False}, 'evidence_id')


def validate_gap(value: dict, root: Path) -> None:
    """Proposal integrity, not production gap-evidence authorization."""
    check_identity(GAP_VERSION, value, 'evidence_id')
    if value != generate_gap(root):
        raise ValueError('unsupported gap, configuration drift, request scope, substitution, or unauthorized approval')
