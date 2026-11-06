"""One initial ACS bootstrap publication; never installs or activates anything.

Retained files and journals are evidence only. Every invocation reconstructs all
parents/candidates and independently reads current governance and human consent.
"""
from __future__ import annotations

import argparse
import base64
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from urllib.parse import quote

from core.source_artifacts.catalog import add_record, validate_record
from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS, GitHubReleaseArtifactPublisher
from core.source_artifacts.hashing import canonical_json_bytes, sha256_file, sha256_json
from core.source_artifacts.package import build_publication_package, extract_publication_package, PACKAGE_VERSION
from core.source_artifacts.publication import PublicationError, validate_receipt
from jobs.monthly_refresh import acs_history_bootstrap as boot
from jobs.monthly_refresh import acs_governance_provenance as provenance
from jobs.monthly_refresh.acs_authoritative_write import CurrentCatalog, read_at_commit, verify_at_commit
from jobs.monthly_refresh.acs_scope import ACSGovernance

REPOSITORY = boot.REPOSITORY
CATALOG = 'config/artifact_catalog.json'
REQUEST_PATH = 'docs/proposals/acs_history_bootstrap_publication.json'
VERSION = 'acs_history_bootstrap_publication_request_v1'
BUNDLE = 'acs_history_bootstrap_review_v1__99e0abccc424165f2c5ec6e4f603904087ceff4562f3c0725e0b0aedcd2fd7cf'
BASELINE = '4ef518d37d1ee0da55a19a83dabb205363a97d44'
REGISTRY = '87fd4c909fb3b1e314e2646e00fa9a2f60bec70023d3a541b25979f412db7141'
CANDIDATES = {
    'census_acs1': {
        'artifact_id': 'src__census_acs1__2024-12__r1__57a4580db2986248',
        'package_sha256': '1abc68022f7ad50ced1e734a51e40c6f4cf995979ae3cee65688c316dfd563ad',
        'state_id': 'acs_physical_historical_state_v2__6595317ece328607538fafbd3ac113939876797550f77d72290f9892338365cb'},
    'census_acs5': {
        'artifact_id': 'src__census_acs5__2024-12__r1__177a1404c91f6594',
        'package_sha256': '26e838730b84fb08401a8f9ac189a3982f98f975d403510b907b1b9bb689756d',
        'state_id': 'acs_physical_historical_state_v2__3d2b106bcb9ef914b260857ed6c58593bc19e6bc6ad4328dd5fbdea7907b316a'}}
ACKNOWLEDGEMENTS = [
    'Publish only the exact two retained bootstrap packages; catalog addition is not acceptance.',
    'ACS1 2020 is approved standard-product nonpublication, with no synthetic acquisition or observations.',
    'ACS5 2023 retains four HTTP 204 receipts and original physical source identity.',
    '2024 limitations remain: unknown manifest git_sha, separately recovered code evidence, process-attested absence, 67 ACS5-only geographies and four jointly missing geographies.',
    'No registry installation, accepted-pointer changes, readiness changes, Source Set, canonical, serving or Macro authorization.']


def read(path):
    return json.loads(Path(path).read_bytes(), object_pairs_hook=provenance._unique)


def _save(path, value):
    """Append-only local evidence; never replace an audit or uncertain outcome."""
    with Path(path).open('xb') as stream:
        stream.write(canonical_json_bytes(value))
        stream.flush()
        os.fsync(stream.fileno())


def _blob(raw):
    return hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()


def _executable(path):
    return (path.startswith(('core/', 'jobs/', 'sources/')) and path.endswith('.py')
            or path.startswith('.github/workflows/') or path.startswith('requirements')
            or path.startswith(('config/acs_backfill_reviews/provider-', 'config/monthly_source_input_pins/acs_backfill__',
                                'docs/audit/acs_2024_qualification_reports/'))
            or path in {'config/geo_manifest.generated.csv', 'config/monthly_refresh_policy.json',
                        'config/source_metric_registry.csv', 'config/source_refresh_revision_policy_v0_2.json',
                        'config/acs_historical_governance.json'})


def code_blobs(root):
    if (root / 'jobs/monthly_refresh/acs_history_bootstrap_publication.py').resolve() != Path(__file__).resolve():
        raise ValueError('execute the reviewed repository checkout, not another installed module')
    names = subprocess.check_output(['git', '-C', str(root), 'ls-files'], text=True).splitlines()
    return {p: _blob((root / p).read_bytes()) for p in names if _executable(p)}


def evidence_files(spec):
    paths = {**{'package:' + k: v for k, v in spec['packages'].items()},
             **{'provider:' + k: v for k, v in spec['evidence'].items()},
             **{'report:' + k: v for k, v in spec['reports'].items()},
             **{k: spec[k] for k in ('catalog', 'governance', 'acs5_2023_pin')}}
    return {k: sha256_file(Path(v)) for k, v in paths.items()}


def request(spec, bundle, directory, root):
    """Only an inactive review subject; no decision, reviewer or provenance."""
    semantic = {k: v for k, v in bundle.items() if k != 'bundle_id'}
    if (bundle.get('bundle_id') != BUNDLE
            or BUNDLE != boot.BUNDLE_VERSION + '__' + sha256_json(semantic)
            or bundle.get('namespace') != 'production' or bundle.get('status') != 'proposed'
            or bundle.get('active') is not False or bundle.get('approval_provenance') is not None
            or bundle.get('publication') != 'not_established'
            or bundle.get('input_sha256') != sha256_json(spec)
            or bundle['baseline']['validated_main_commit'] != BASELINE
            or bundle['baseline']['registry_sha256'] != REGISTRY
            or bundle['baseline']['namespace'] != 'production'
            or set(bundle['candidates']) != set(CANDIDATES)):
        raise ValueError('exact inactive production bootstrap bundle required')
    for source, expected in CANDIDATES.items():
        candidate = bundle['candidates'][source]
        if ({k: candidate[k] for k in ('artifact_id', 'package_sha256')} !=
                {k: expected[k] for k in ('artifact_id', 'package_sha256')}
                or candidate['state']['state_id'] != expected['state_id']
                or candidate['artifact_path'] != f'candidates/{source}'
                or candidate['package_path'] != f'packages/{source}.tar'
                or sha256_file(directory / candidate['package_path']) != expected['package_sha256']):
            raise ValueError('candidate identity or retained TAR changed')
    value = {'schema_version': VERSION, 'status': 'proposed', 'approval_provenance': None,
        'repository': REPOSITORY, 'bundle_id': BUNDLE,
        'bundle_file_sha256': sha256_file(directory / 'bundle.json'),
        'input_sha256': bundle['input_sha256'], 'baseline': bundle['baseline'],
        'candidates': bundle['candidates'], 'parents': bundle['parents'],
        'compatibility': bundle['compatibility'],
        'proposed_acceptance': bundle['proposed_acceptance'],
        'code_blobs': code_blobs(root), 'evidence_file_sha256': evidence_files(spec),
        'acknowledgements': ACKNOWLEDGEMENTS,
        'operations': ['publish_exact_retained_packages', 'add_two_immutable_catalog_records'],
        'registry_installation_authorized': False, 'activation_authorized': False}
    return {**value, 'request_id': VERSION + '__' + sha256_json(value)}


def _remote_code(api, commit, expected):
    bound = provenance._get(api, f'/git/commits/{commit}')
    if bound.get('sha') != commit:
        raise ValueError('code commit identity differs')
    tree = provenance._get(api, f'/git/trees/{bound["tree"]["sha"]}?recursive=1')
    if tree.get('truncated') is not False:
        raise ValueError('complete current-main code tree required')
    actual = {v['path']: v['sha'] for v in tree['tree'] if v['type'] == 'blob' and _executable(v['path'])}
    if actual != expected:
        raise ValueError('current-main code/workflow/requirements drift')


def verify_authorization(review, locator, token, root):
    """Fresh owner-only decision, using the existing provenance scan contracts."""
    if set(locator) != {'reviewed_pr', 'reviewed_commit', 'comment_id'}:
        raise ValueError('exact live decision locator required')
    pr, commit, cid = (locator[k] for k in ('reviewed_pr', 'reviewed_commit', 'comment_id'))
    if (type(pr) is not int or pr <= 0 or type(cid) is not int or cid <= 0
            or not isinstance(commit, str) or not re.fullmatch('[0-9a-f]{40}', commit)):
        raise ValueError('invalid decision locator')
    if subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip() != commit:
        raise ValueError('execute exact human-reviewed checkout')
    api = GitHubAPI(REPOSITORY, token, read_only=True)
    repository = provenance._get(api, '')
    owner = repository.get('owner', {})
    if (repository.get('full_name') != REPOSITORY or type(repository.get('id')) is not int
            or repository['id'] <= 0 or owner.get('login') != REPOSITORY.split('/')[0]
            or owner.get('type') != 'User' or type(owner.get('id')) is not int or owner['id'] <= 0):
        raise ValueError('live personal repository owner required')
    permission_cache = {}
    def authorized(user):
        if user.get('type') != 'User' or type(user.get('id')) is not int:
            return False
        if user['id'] == owner['id']:
            return user.get('login') == owner['login']
        login = user.get('login', '')
        if not isinstance(login, str) or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9-]{0,38}', login):
            return False
        if login not in permission_cache:
            permission_cache[login] = provenance._get(api, f'/collaborators/{login}/permission').get('permission') in {'admin', 'write', 'maintain'}
        return permission_cache[login]
    def scan():
        pull = provenance._get(api, f'/pulls/{pr}')
        if (pull.get('number') != pr or pull.get('base', {}).get('ref') != 'main'
                or pull.get('base', {}).get('repo', {}).get('full_name') != REPOSITORY
                or pull.get('head', {}).get('repo', {}).get('full_name') != REPOSITORY
                or pull['head'].get('sha') != commit
                or (pull.get('state') != 'open' and pull.get('merged') is not True)):
            raise ValueError('wrong PR, fork or stale reviewed head')
        comments, cp = provenance._pages(api, f'/issues/{pr}/comments', repository['id'])
        reviews, rp = provenance._pages(api, f'/pulls/{pr}/reviews', repository['id'])
        inline, ip = provenance._pages(api, f'/pulls/{pr}/comments', repository['id'])
        if (type(pull.get('comments')) is not int or pull['comments'] != len(comments)
                or type(pull.get('review_comments')) is not int or pull['review_comments'] != len(inline)):
            raise ValueError('incomplete or concurrently changed decision streams')
        return {'pull': pull, 'comments': comments, 'reviews': reviews, 'inline': inline, 'pages': cp + rp + ip}
    first = scan()
    obj = provenance._get(api, f'/issues/comments/{cid}')
    matches = [v for v in first['comments'] if v.get('id') == cid]
    capture = provenance.comment_snapshot(obj)
    if (obj.get('id') != cid or len(matches) != 1
            or provenance._evidence_consistency(obj) != provenance._evidence_consistency(matches[0])
            or capture['user'] != {k: owner[k] for k in ('id', 'login', 'type')}
            or capture['performed_via_github_app'] is not None
            or capture['html_url'] != f'https://github.com/{REPOSITORY}/pull/{pr}#issuecomment-{cid}'
            or capture['issue_url'] != api.api + f'/issues/{pr}'
            or capture['created_at'] != capture['updated_at']):
        raise ValueError('genuine unedited personal owner comment required')
    body = json.loads(capture['body'], object_pairs_hook=provenance._unique)
    rationale = body.get('decision_rationale')
    expected = {'schema_version': 'acs_history_bootstrap_publication_decision_v1',
        'repository': REPOSITORY, 'request_id': review['request_id'],
        'request_sha256': sha256_json(review), 'reviewed_pr': pr, 'reviewed_commit': commit,
        'decision': 'authorize_exact_publication_only', 'personally_reviewed_and_authorized': True,
        'authorship': 'human_personal', 'acknowledgements': ACKNOWLEDGEMENTS,
        'decision_rationale': rationale}
    if not isinstance(rationale, str) or not rationale.strip() or sha256_json(body) != sha256_json(expected):
        raise ValueError('exact explicit human publication decision required')
    content = provenance._get(api, f'/contents/{REQUEST_PATH}?ref={commit}')
    reviewed = json.loads(base64.b64decode(content['content']), object_pairs_hook=provenance._unique)
    if sha256_json(reviewed) != sha256_json(review):
        raise ValueError('decision request differs from reviewed GitHub contents')
    _remote_code(api, commit, review['code_blobs'])
    earliest = provenance._time(capture['created_at'])
    for stream in ('comments', 'reviews', 'inline'):
        for item in first[stream]:
            if stream == 'comments' and item['id'] == cid:
                continue
            stamp = item.get('updated_at') or item.get('submitted_at') or item.get('created_at')
            if stamp is None or (provenance._time(stamp) >= earliest and authorized(item.get('user', {}))):
                raise ValueError('later authorized discussion or undated evidence; supersession unresolved')
    second = scan()
    if provenance._scan_consistency(first) != provenance._scan_consistency(second):
        raise ValueError('decision streams changed during verification')
    if provenance.comment_snapshot(provenance._get(api, f'/issues/comments/{cid}')) != capture:
        raise ValueError('decision changed at completion')
    value = {'schema_version': 'acs_history_bootstrap_publication_audit_v1',
        'checked_at': datetime.now(timezone.utc).isoformat(), 'request_id': review['request_id'],
        'locator': locator, 'owner_comment': capture, 'complete_scan': first,
        'production_authority': False, 'activation': False}
    return {**value, 'snapshot_sha256': sha256_json(value)}


def _metadata(path, candidate, commit):
    manifest = read(path / 'manifest.json')
    return {'logical_artifact_uri': manifest['artifact_uri'], 'object_id': candidate['artifact_id'],
        'object_type': 'source', 'object_metadata': {'source_id': manifest['source_id'],
            'data_sha256': manifest['data_sha256'], 'provider_release_id': manifest['provider_release_id'],
            'observation_max': manifest['observation_max']},
        'artifact_content_hash': candidate['artifact_content_hash'],
        'member_hashes': {p.name: sha256_file(p) for p in path.iterdir()},
        'publisher_git_sha': commit, 'contract_versions': [PACKAGE_VERSION, boot.history.STATE_VERSION]}


def _record(receipt):
    return {k: receipt[k] for k in ('object_type', 'object_id', 'logical_artifact_uri', 'remote_repository',
        'release_tag', 'release_id', 'asset_id', 'asset_filename', 'package_sha256', 'artifact_content_hash',
        'publication_state')} | {'publication_receipt_id': receipt['receipt_id'], 'metadata': receipt['object_metadata']}


def verify_remote(api, receipt, metadata, candidate, workspace):
    """A copied receipt never establishes live remote bytes or numeric identity."""
    validate_receipt(receipt, require_eligible=True)
    if (receipt['remote_repository'] != REPOSITORY or receipt['remote_backend'] != 'github_releases'
            or receipt['package_sha256'] != candidate['package_sha256']
            or any(receipt[k] != v for k, v in metadata.items())
            or receipt['release_tag'] != f'source-artifact/{metadata["object_metadata"]["source_id"]}/{candidate["artifact_id"]}'
            or receipt['asset_filename'] != candidate['artifact_id'] + '.tar'):
        raise ValueError('receipt differs from exact authorized candidate')
    release = provenance._get(api, f'/releases/{receipt["release_id"]}')
    by_tag = provenance._get(api, '/releases/tags/' + quote(receipt['release_tag'], safe=''))
    assets = [a for a in release.get('assets', []) if a.get('name') == receipt['asset_filename']]
    if (release.get('id') != receipt['release_id'] or release.get('draft') is not False
            or release.get('tag_name') != receipt['release_tag'] or by_tag.get('id') != receipt['release_id']
            or receipt['published_at'] != release.get('published_at')
            or len(release.get('assets', [])) != 1 or len(assets) != 1 or assets[0].get('id') != receipt['asset_id']):
        raise ValueError('remote release or asset identity changed')
    package = workspace / 'remote.tar'
    api.download_asset(receipt['asset_id'], package)
    extracted = extract_publication_package(package, workspace / 'artifact', expected_sha256=candidate['package_sha256'])
    GitHubReleaseArtifactPublisher._validate_package(package, workspace / 'check', metadata)
    manifest = read(extracted / 'manifest.json')
    if manifest.get('acs_history') != candidate['state'] or manifest.get('identity_context') != candidate['state']:
        raise ValueError('remote physical state differs')
    record = _record(receipt)
    validate_record(record, receipt)
    return record


def catalog_transition(original, current, records, original_blob):
    """Only an ACS1-first prefix of these exact two additions, including CAS."""
    possibilities = [deepcopy(original)]
    for source in CANDIDATES:
        if source not in records:
            break
        value = deepcopy(possibilities[-1])
        previous_blob = original_blob if len(possibilities) == 1 else _blob(canonical_json_bytes(value))
        if any(r['object_id'] == records[source]['object_id'] for r in value['immutable_records']):
            raise ValueError('candidate already exists in original baseline')
        value['immutable_records'].append(records[source])
        value['immutable_records'].sort(key=lambda v: (v['object_type'], v['object_id']))
        value['compare_and_swap']['expected_git_blob_sha'] = previous_blob
        possibilities.append(value)
    digests = [sha256_json(v) for v in possibilities]
    if sha256_json(current) not in digests:
        raise ValueError('unauthorized catalog transition or accepted pointer drift')
    return digests.index(sha256_json(current))


def verify_local(spec, bundle, directory, root, governance, current_catalog):
    """Reuse Phase 1 reconstruction against a separately verified catalog witness.

    The original input and bundle files are never edited or re-identified. Only
    the catalog read location is substituted in a temporary verification copy.
    Phase 1 freshness rules still apply to that current, independently read copy.
    """
    with tempfile.TemporaryDirectory() as td:
        workspace = Path(td)
        catalog_path = workspace / 'current-catalog.json'
        catalog_path.write_bytes(canonical_json_bytes(current_catalog))
        witness = {**spec, 'catalog': str(catalog_path)}
        checked = boot.reconcile_inventory(witness, governance=governance, root=root, workspace=workspace / 'parents')
        if (bundle['parents'] != sorted(spec['parents'], key=lambda p: (p['source_id'], p['vintage']))
                or bundle['parent_verification'] != checked['parent_verification']
                or bundle['compatibility'] != checked['compatibility']
                or bundle['verification'] != {'verified_original_parents': 35, 'independent_state_reconstruction': 'passed', 'evidence_kind': 'production_inputs'}
                or bundle['proposed_acceptance'] != boot.propose_acceptance(
                    {s: c['state'] for s, c in bundle['candidates'].items()}, baseline_registry=read(spec['governance']))):
            raise ValueError('original parent evidence, limitations or acceptance delta changed')
        metadata = {}
        for source, candidate in bundle['candidates'].items():
            path = extract_publication_package(directory / candidate['package_path'], workspace / source,
                expected_sha256=candidate['package_sha256'])
            _, _, state = boot.history.verify_state(path, governance=governance, paths=checked['paths'],
                records=checked['records'], repository_root=root)
            boot.history.verify_state(directory / candidate['artifact_path'], governance=governance,
                paths=checked['paths'], records=checked['records'], repository_root=root)
            manifest = read(path / 'manifest.json')
            envelope = build_publication_package(directory / candidate['artifact_path'], workspace / (source + '.tar'))
            if (state != candidate['state'] or state['inventory'] != checked['inventories'][source]
                    or manifest['artifact_id'] != candidate['artifact_id']
                    or manifest['artifact_content_hash'] != candidate['artifact_content_hash']
                    or manifest['artifact_created_at'] != boot._created_at(spec)
                    or envelope['package_sha256'] != candidate['package_sha256']):
                raise ValueError('candidate state, package, lineage or provenance differs')
            metadata[source] = path  # Used only within this scope, below.
        # Return metadata computed from verified extracted bytes, never loose claims.
        return {s: _metadata(p, bundle['candidates'][s], subprocess.check_output(
            ['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()) for s, p in metadata.items()}


class Session:
    """Not an authority token. Each fence independently revalidates live evidence."""
    def __init__(self, *, spec, directory, root, review, locator, journal):
        self.stage = 'session_inputs'
        self.spec, self.directory, self.root = spec, directory, root
        self.bundle = read(directory / 'bundle.json')
        if sha256_json(request(spec, self.bundle, directory, root)) != sha256_json(review):
            raise ValueError('review request changed')
        self.review, self.locator, self.journal = review, locator, journal
        if journal.resolve().is_relative_to(root.resolve()) or journal.resolve().is_relative_to(directory.resolve()):
            raise ValueError('new separate journal directory required')
        journal.mkdir(parents=True, exist_ok=True)
        self.token = os.environ.get('GITHUB_TOKEN', '')
        if not self.token:
            raise ValueError('GITHUB_TOKEN required')
        self.api = GitHubAPI(REPOSITORY, self.token)
        self.reader = GitHubAPI(REPOSITORY, self.token, read_only=True)
        self.receipts, self.records = {}, {}
        # Receipt files are not trusted: fence rechecks remote bytes and metadata.
        for source in CANDIDATES:
            path = journal / (source + '.receipt.json')
            if path.exists():
                self.receipts[source] = read(path)
        self.metadata = None
        self.last_audit = None
        if 'census_acs5' in self.receipts and 'census_acs1' not in self.receipts:
            raise ValueError('ACS5 receipt without preceding ACS1 publication')
        self.lock = (journal / '.lock').open('a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            binding = {'request_id': review['request_id'], 'locator': locator}
            path = journal / 'binding.json'
            if path.exists():
                if read(path) != binding:
                    raise ValueError('journal belongs to another authorization')
            else:
                _save(path, binding)
        except Exception:
            self.lock.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *args):
        try:
            if args and args[0] is not None:
                self.evidence('blocked', {'failed_stage': self.stage, 'error_type': args[0].__name__})
        except OSError:
            pass  # Preserve the original error if durable evidence cannot be written.
        finally:
            self.close()

    def close(self):
        self.lock.close()

    def evidence(self, kind, value):
        count = len(list(self.journal.glob('*.event.json')))
        _save(self.journal / f'{count:06d}-{kind}.event.json', value)

    def fence(self, *, reconstruct=False):
        self.stage = 'local_input_integrity'
        if sha256_json(request(self.spec, self.bundle, self.directory, self.root)) != sha256_json(self.review):
            raise ValueError('local input, bundle or code changed')
        # Re-read the file too; self.bundle must not mask local mutation.
        if read(self.directory / 'bundle.json') != self.bundle:
            raise ValueError('retained bundle changed')
        self.stage = 'live_owner_authorization'
        audit = verify_authorization(self.review, self.locator, self.token, self.root)
        self.stage = 'live_governance'
        governance = ACSGovernance.load_production(REPOSITORY, self.token)
        governance.require_destination(REPOSITORY)
        if governance.production_snapshot()['registry_sha256'] != REGISTRY:
            raise ValueError('governance baseline changed')
        tip = governance.current_commit(REPOSITORY)
        self.stage = 'current_main_code_and_baseline'
        _remote_code(self.reader, tip, self.review['code_blobs'])
        comparison = provenance._get(self.reader, f'/compare/{BASELINE}...{tip}')
        if comparison.get('merge_base_commit', {}).get('sha') != BASELINE:
            raise ValueError('preparation baseline is not current-main ancestry')
        original, original_blob = read_at_commit(self.reader, CATALOG, BASELINE)
        current, current_blob = read_at_commit(self.reader, CATALOG, tip)
        registry, _ = read_at_commit(self.reader, 'config/acs_historical_governance.json', BASELINE)
        if (sha256_json(original) != self.bundle['baseline']['catalog_sha256']
                or sha256_json(registry) != REGISTRY or registry != read(self.spec['governance'])
                or registry['states'] or registry['current_state']):
            raise ValueError('original reviewed-main baseline differs')
        boot._scopes(self.spec, governance, self.root)
        # Verify saved receipts before permitting even a partial catalog prefix.
        if self.metadata is None or reconstruct:
            # Before expensive reconstruction, require exact authorized tuples;
            # full metadata/receipt verification and transition follow below.
            allowed = {c['artifact_id'] for c in self.bundle['candidates'].values()}
            old = {r['object_id']: r for r in original['immutable_records']}
            if (current['accepted'] != original['accepted']
                    or any(r != old.get(r['object_id']) for r in current['immutable_records'] if r['object_id'] not in allowed)
                    or not set(old) <= {r['object_id'] for r in current['immutable_records']}):
                raise ValueError('catalog baseline record or pointer drift')
            self.stage = 'independent_reconstruction'
            self.metadata = verify_local(self.spec, self.bundle, self.directory, self.root, governance, current)
        self.stage = 'remote_receipts'
        self.records = {}
        for source, receipt in self.receipts.items():
            with tempfile.TemporaryDirectory() as td:
                self.records[source] = verify_remote(self.reader, receipt, self.metadata[source], self.bundle['candidates'][source], Path(td))
        self.stage = 'catalog_transition'
        prefix = catalog_transition(original, current, self.records, original_blob)
        if current_blob != (original_blob if prefix == 0 else _blob(canonical_json_bytes(current))):
            raise ValueError('catalog serialization changed outside authorized writes')
        if governance.current_commit(REPOSITORY) != tip:
            raise ValueError('concurrent main change during fence')
        self.last_audit = audit
        self.evidence('fence', {'authorization': audit, 'main_commit': tip,
            'registry_sha256': REGISTRY, 'catalog_sha256': sha256_json(current)})
        return governance, tip, current, current_blob, prefix

    def known_remote(self, source):
        """Known numeric objects cannot be replaced through a reassigned tag."""
        events = [read(p) for p in sorted(self.journal.glob('*.event.json'))]
        known = [v for v in events if v.get('source') == source and v.get('release_id') is not None]
        if not known:
            return
        releases = {v['release_id'] for v in known}
        assets = {v['asset_id'] for v in known if v.get('asset_id') is not None}
        if len(releases) != 1 or len(assets) > 1:
            raise ValueError('ambiguous recovery remote identity')
        rid = next(iter(releases))
        release = provenance._get(self.reader, f'/releases/{rid}')
        candidate = self.bundle['candidates'][source]
        tag = f'source-artifact/{source}/{candidate["artifact_id"]}'
        by_tag = provenance._get(self.reader, '/releases/tags/' + quote(tag, safe=''))
        if release.get('id') != rid or release.get('tag_name') != tag or by_tag.get('id') != rid:
            raise ValueError('known release missing or tag reassigned')
        if assets:
            matches = [a for a in release.get('assets', []) if a.get('name') == candidate['artifact_id'] + '.tar']
            if len(matches) != 1 or matches[0].get('id') != next(iter(assets)):
                raise ValueError('known asset missing or replaced')

    def publish(self):
        self.fence(reconstruct=True)
        for source in CANDIDATES:
            if source not in self.receipts:
                self.known_remote(source)
                publisher = GitHubReleaseArtifactPublisher(self.api)
                metadata = self.metadata[source]
                uri = metadata['logical_artifact_uri']
                payload = (self.directory / self.bundle['candidates'][source]['package_path']).read_bytes()
                if hashlib.sha256(payload).hexdigest() != CANDIDATES[source]['package_sha256']:
                    raise ValueError('retained upload bytes changed')
                # No automatic mutation retries. On any uncertain response exit;
                # a new invocation discovers and verifies the exact remote asset.
                for label, action in [('prepare', lambda: publisher.prepare(uri, payload, metadata)),
                                      ('upload', lambda: publisher.upload(uri)),
                                      ('verify', lambda: publisher.verify(uri))]:
                    self.fence()
                    self.known_remote(source)
                    self.evidence('intent', {'source': source, 'operation': label})
                    self.stage = 'publication_' + label
                    action()
                    item = publisher.pending[uri]
                    self.evidence('remote', {'source': source, 'release_id': item['release_id'],
                        'asset_id': item.get('asset', {}).get('id'), 'operation': label})
                self.fence()
                self.known_remote(source)
                self.evidence('intent', {'source': source, 'operation': 'finalize'})
                self.stage = 'publication_finalize'
                receipt = publisher.finalize(uri)
                with tempfile.TemporaryDirectory() as td:
                    verify_remote(self.reader, receipt, metadata, self.bundle['candidates'][source], Path(td))
                _save(self.journal / (source + '.receipt.json'), receipt)
                self.receipts[source] = receipt
            governance, tip, current, blob, prefix = self.fence()
            record = self.records[source]
            updated = add_record(current, record, self.receipts[source])
            if updated != current:
                # write_current fences the parent main tip and exact catalog blob;
                # this wrapper checks catalog semantics again at its write boundary.
                session = self
                written_catalog = deepcopy(updated)
                written_catalog['compare_and_swap']['expected_git_blob_sha'] = blob
                class Boundary:
                    def __init__(self):
                        self.write_events = []
                    def require_destination(self, repository):
                        governance.require_destination(repository)
                    def current_commit(self, repository):
                        _, now, value, oid, _ = session.fence()
                        before = now == tip and value == current and oid == blob
                        own_write = (self.write_events and now == self.write_events[-1]['commit_sha']
                            and value == written_catalog and oid == _blob(canonical_json_bytes(written_catalog)))
                        if not (before or own_write):
                            raise ValueError('concurrent catalog/main movement')
                        return governance.current_commit(repository)
                store = GitHubCatalogCAS(self.api, CATALOG, 'main', fixture=False)
                self.evidence('intent', {'source': source, 'operation': 'catalog', 'parent_commit': tip})
                self.stage = 'catalog_write'
                CurrentCatalog(store, Boundary()).add(record, self.receipts[source])
            self.fence()
        return self.verify_installation()

    def verify_installation(self):
        governance, tip, catalog, blob, prefix = self.fence(reconstruct=True)
        if prefix != 2 or set(self.receipts) != set(CANDIDATES):
            raise ValueError('both published cataloged candidates required; installation blocked')
        verify_at_commit(self.reader, CATALOG, tip, catalog)
        value = {'schema_version': 'acs_history_bootstrap_installation_evidence_v1',
            'bundle_id': BUNDLE, 'request_id': self.review['request_id'],
            'publication_authorization': self.last_audit,
            'preparation_baseline': self.bundle['baseline'], 'verified_main_commit': tip,
            'registry_sha256': REGISTRY, 'catalog_sha256': sha256_json(catalog), 'catalog_blob_sha': blob,
            'receipts': self.receipts, 'records': self.records,
            'proposed_acceptance': self.bundle['proposed_acceptance'],
            'verification': 'passed_publication_only', 'original_parents_verified': 35,
            'registry_installation': 'not_performed', 'installation_authorization': 'required_separately',
            'installation_readiness': 'blocked_pending_separate_authorization_and_fresh_checks',
            'activation': False}
        value['evidence_id'] = 'acs_history_bootstrap_installation_evidence_v1__' + sha256_json(value)
        self.evidence('installation-evidence', value)
        return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, default=Path('.'))
    parser.add_argument('--inputs', type=Path, required=True)
    parser.add_argument('--bundle-directory', type=Path, required=True)
    parser.add_argument('--request', type=Path, required=True)
    parser.add_argument('--decision', type=Path, help='Only reviewed_pr, reviewed_commit and comment_id; never copied approval')
    parser.add_argument('--journal', type=Path, help='Separate durable recovery directory outside checkout/bundle')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--review-request', action='store_true')
    mode.add_argument('--publish', action='store_true')
    mode.add_argument('--verify-publication', action='store_true')
    args = parser.parse_args()
    session = None
    try:
        root, directory = args.repository_root.resolve(), args.bundle_directory.resolve()
        spec = read(args.inputs)
        if args.review_request:
            if args.request.resolve().is_relative_to(root) or args.request.resolve().is_relative_to(directory):
                raise ValueError('review request output must be outside checkout and retained bundle')
            _save(args.request, request(spec, read(directory / 'bundle.json'), directory, root))
            print(json.dumps({'review_request': 'proposed', 'publication': 'not_performed', 'activation': False}))
            return 0
        if args.decision is None or args.journal is None:
            raise ValueError('live decision locator and durable journal required')
        with Session(spec=spec, directory=directory, root=root, review=read(args.request),
                locator=read(args.decision), journal=args.journal.resolve()) as session:
            result = session.publish() if args.publish else session.verify_installation()
        print(json.dumps({'verification': result['verification'], 'evidence_id': result['evidence_id'],
            'registry_installation': 'not_performed', 'activation': False}))
        return 0
    except Exception as error:
        print(json.dumps({'verification': 'blocked', 'error_type': type(error).__name__,
            'failed_stage': session.stage if session is not None else 'local_inputs_or_session_setup',
            'registry_installation': 'not_performed', 'activation': False}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
