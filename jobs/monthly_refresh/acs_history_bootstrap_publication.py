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
VERSION = 'acs_history_bootstrap_publication_request_v2'
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


# Historical evidence only; never authorization for this executable revision.
ORIGINAL_LOCATOR = {'reviewed_pr': 315,
    'reviewed_commit': '055e332fc7e4c872d8c97b676ef2c47f1ac5418b', 'comment_id': 6713551209}
ORIGINAL_REQUEST = 'acs_history_bootstrap_publication_request_v1__ca0f24785f35dd82773a7d9d4a07ae0936b20120886e4dd7d232b2097f5e0651'
RECOVERY_RELEASE = 437874694


def _path_hash(path):
    return hashlib.sha256(str(path.resolve()).encode()).hexdigest()


def handoff(prior, journal, bundle):
    """Read-only snapshot of the exact failed initial run, not an authority."""
    prior, journal = prior.resolve(), journal.resolve()
    if prior == journal or prior in journal.parents or journal in prior.parents:
        raise ValueError('recovery journal must be separate from the original journal')
    names = ['binding.json', '.lock', '000000-fence.event.json', '000001-fence.event.json',
        '000002-intent.event.json', '000003-remote.event.json', '000004-fence.event.json',
        '000005-blocked.event.json']
    if {p.name for p in prior.iterdir()} != set(names):
        raise ValueError('original journal is incomplete or contains unexpected evidence')
    raw = {}
    for name in names:
        path = prior / name
        if path.is_symlink() or not path.is_file():
            raise ValueError('original journal requires regular immutable files')
        raw[name] = path.read_bytes()
    parsed = {n: json.loads(v, object_pairs_hook=provenance._unique) for n,v in raw.items() if n != '.lock'}
    if raw['.lock'] or parsed['binding.json'] != {'request_id': ORIGINAL_REQUEST, 'locator': ORIGINAL_LOCATOR}:
        raise ValueError('original request/decision journal binding mismatch')
    if (parsed['000002-intent.event.json'] != {'source': 'census_acs1', 'operation': 'prepare'}
            or parsed['000003-remote.event.json'] != {'source': 'census_acs1', 'operation': 'prepare',
                'release_id': RECOVERY_RELEASE, 'asset_id': None}
            or parsed['000005-blocked.event.json'] != {'failed_stage': 'catalog_transition', 'error_type': 'ValueError'}):
        raise ValueError('original remote outcome differs from the reviewed recovery')
    captures = []
    for name in ('000000-fence.event.json', '000001-fence.event.json', '000004-fence.event.json'):
        fence = parsed[name]
        audit = deepcopy(fence['authorization']); digest = audit.pop('snapshot_sha256', None)
        capture = audit.get('owner_comment', {})
        body = json.loads(capture.get('body', ''), object_pairs_hook=provenance._unique)
        if (digest != sha256_json(audit) or audit.get('request_id') != ORIGINAL_REQUEST
                or audit.get('locator') != ORIGINAL_LOCATOR or audit.get('production_authority') is not False
                or audit.get('activation') is not False or capture.get('id') != ORIGINAL_LOCATOR['comment_id']
                or capture.get('created_at') != capture.get('updated_at')
                or body.get('request_id') != ORIGINAL_REQUEST
                or body.get('reviewed_pr') != ORIGINAL_LOCATOR['reviewed_pr']
                or body.get('reviewed_commit') != ORIGINAL_LOCATOR['reviewed_commit']
                or fence['registry_sha256'] != REGISTRY
                or fence['catalog_sha256'] != bundle['baseline']['catalog_sha256']
                or not re.fullmatch('[0-9a-f]{40}', fence['main_commit'])):
            raise ValueError('original fence/owner evidence is inconsistent')
        captures.append(capture)
    if any(c != captures[0] for c in captures):
        raise ValueError('original owner snapshots disagree')
    # Detect concurrent changes during offline request generation as well.
    if {p.name for p in prior.iterdir()} != set(names) or any((prior/n).read_bytes() != v for n,v in raw.items()):
        raise ValueError('original journal changed during handoff snapshot')
    value = {'schema_version': 'acs_history_bootstrap_recovery_handoff_v1',
        'original_request_id': ORIGINAL_REQUEST, 'original_decision': ORIGINAL_LOCATOR,
        'original_owner_comment': captures[0], 'source_journal_path_sha256': _path_hash(prior),
        'destination_journal_path_sha256': _path_hash(journal),
        'files': {n: {'sha256': hashlib.sha256(v).hexdigest(), 'size_bytes': len(v)} for n,v in raw.items()},
        'source': 'census_acs1', 'release_id': RECOVERY_RELEASE,
        'candidate': CANDIDATES['census_acs1'], 'bundle_id': bundle['bundle_id']}
    return {**value, 'handoff_id': 'acs_history_bootstrap_recovery_handoff_v1__' + sha256_json(value)}


def verify_handoff(api, review, audit):
    """Historical evidence is compared live, but only the NEW decision grants consent."""
    recovery = review['recovery_handoff']
    old, _ = read_at_commit(api, REQUEST_PATH, ORIGINAL_LOCATOR['reviewed_commit'])
    old_semantic = {k:v for k,v in old.items() if k != 'request_id'}
    if (old.get('request_id') != ORIGINAL_REQUEST
            or ORIGINAL_REQUEST != 'acs_history_bootstrap_publication_request_v1__' + sha256_json(old_semantic)):
        raise ValueError('historical request identity mismatch')
    for key in ('bundle_id', 'bundle_file_sha256', 'input_sha256', 'baseline', 'candidates', 'parents',
                'compatibility', 'proposed_acceptance', 'evidence_file_sha256'):
        if sha256_json(old[key]) != sha256_json(review[key]):
            raise ValueError('historical candidate or parent evidence changed')
    capture = provenance.comment_snapshot(provenance._get(api, f'/issues/comments/{ORIGINAL_LOCATOR["comment_id"]}'))
    body = json.loads(capture['body'], object_pairs_hook=provenance._unique)
    if (capture != recovery['original_owner_comment']
            or capture['user'] != audit['owner_comment']['user'] or capture['performed_via_github_app'] is not None
            or capture['issue_url'] != api.api + '/issues/315'
            or capture['html_url'] != f'https://github.com/{REPOSITORY}/pull/315#issuecomment-6713551209'
            or body.get('request_sha256') != sha256_json(old)
            or body.get('personally_reviewed_and_authorized') is not True
            or body.get('authorship') != 'human_personal'
            or body.get('decision') != 'authorize_exact_publication_only'):
        raise ValueError('historical owner evidence differs; no inferred authorization')


class BootstrapPublisher(GitHubReleaseArtifactPublisher):
    """Only this bootstrap adopts checked numeric drafts; generic behavior is unchanged."""
    def select(self, tag, release):
        self.selected_tag, self.selected_release = tag, release
        self.create_permit = release is None

    def _pending_release(self, item):
        release = super()._pending_release(item)
        if (type(release.get('draft')) is not bool or len(release['assets']) > 1
                or (not release['draft'] and not release['assets'])):
            raise ValueError('release state changed before operation; never replace missing assets')
        if any(a.get('name') != item['asset_name'] or a.get('state', 'uploaded') != 'uploaded'
               for a in release['assets']):
            raise ValueError('unexpected or incomplete asset before operation')
        selected = self.selected_release or {}
        pinned = item.get('asset') or next(iter(selected.get('assets', [])), None)
        if pinned is not None:
            matches = [a for a in release['assets'] if a.get('name') == item['asset_name']]
            if len(matches) != 1 or matches[0].get('id') != pinned['id']:
                raise ValueError('pinned asset disappeared or changed; never replace it')
        return release

    def _release(self, tag):
        if tag != self.selected_tag:
            raise ValueError('bootstrap release selection differs')
        if self.selected_release is not None:
            release = self._release_by_id(self.selected_release['id'], tag)
            if release is None:
                raise ValueError('selected numeric release disappeared; never replace it')
            return release
        if not self.create_permit:
            raise ValueError('uncertain create cannot be repeated')
        self.create_permit = False
        return None

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


def request(spec, bundle, directory, root, *, prior_journal, journal):
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
        'recovery_handoff': handoff(prior_journal, journal, bundle),
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
    if locator['comment_id'] == ORIGINAL_LOCATOR['comment_id']:
        raise ValueError('original decision is historical evidence only; fresh authorization required')
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
    def __init__(self, *, spec, directory, root, review, locator, journal, prior_journal):
        self.stage = 'session_inputs'
        self.spec, self.directory, self.root = spec, directory, root
        self.bundle = read(directory / 'bundle.json')
        self.prior_journal = prior_journal.resolve()
        self.prior_lock = (self.prior_journal / '.lock').open('rb')
        try:
            fcntl.flock(self.prior_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._initialize(spec, directory, root, review, locator, journal)
        except Exception:
            self.prior_lock.close()
            raise

    def _initialize(self, spec, directory, root, review, locator, journal):
        if sha256_json(request(spec, self.bundle, directory, root, prior_journal=self.prior_journal, journal=journal)) != sha256_json(review):
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
            binding = {'request_id': review['request_id'], 'locator': locator,
                'handoff_id': review['recovery_handoff']['handoff_id']}
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
        self.prior_lock.close()

    def evidence(self, kind, value):
        count = len(list(self.journal.glob('*.event.json')))
        _save(self.journal / f'{count:06d}-{kind}.event.json', value)

    def fence(self, *, reconstruct=False):
        self.stage = 'local_input_integrity'
        if sha256_json(request(self.spec, self.bundle, self.directory, self.root, prior_journal=self.prior_journal, journal=self.journal)) != sha256_json(self.review):
            raise ValueError('local input, bundle or code changed')
        # Re-read the file too; self.bundle must not mask local mutation.
        if read(self.directory / 'bundle.json') != self.bundle:
            raise ValueError('retained bundle changed')
        self.stage = 'live_owner_authorization'
        audit = verify_authorization(self.review, self.locator, self.token, self.root)
        self.stage = 'historical_handoff_verification'
        verify_handoff(self.reader, self.review, audit)
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
            self.known_remote(source)
            self.stage = 'remote_receipts'
            with tempfile.TemporaryDirectory() as td:
                self.records[source] = verify_remote(self.reader, receipt, self.metadata[source], self.bundle['candidates'][source], Path(td))
        self.stage = 'catalog_transition'
        prefix = catalog_transition(original, current, self.records, original_blob)
        if current_blob != (original_blob if prefix == 0 else _blob(canonical_json_bytes(current))):
            raise ValueError('catalog serialization changed outside authorized writes')
        self.stage = 'final_authority_check'
        if governance.current_commit(REPOSITORY) != tip:
            raise ValueError('concurrent main change during fence')
        self.last_audit = audit
        self.stage = 'journal_fence'
        self.evidence('fence', {'authorization': audit, 'main_commit': tip,
            'registry_sha256': REGISTRY, 'catalog_sha256': sha256_json(current)})
        return governance, tip, current, current_blob, prefix

    def known_remote(self, source):
        """Exhaustive draft-aware discovery, then independent numeric verification."""
        self.stage = 'remote_release_discovery'
        paths = sorted(self.journal.glob('*.event.json'))
        events = [read(p) for p in paths]
        prepare_intent = any(p.name.endswith('-intent.event.json') and v ==
            {'source': source, 'operation': 'prepare'} for p, v in zip(paths, events))
        known = [v for v in events if v.get('source') == source and v.get('release_id') is not None]
        releases = {v['release_id'] for v in known}
        if source == 'census_acs1':
            releases.add(self.review['recovery_handoff']['release_id'])
        assets = {v['asset_id'] for v in known if v.get('asset_id') is not None}
        if source in self.receipts:
            releases.add(self.receipts[source]['release_id'])
            assets.add(self.receipts[source]['asset_id'])
        if len(releases) > 1 or len(assets) > 1:
            raise ValueError('ambiguous recovery remote identity')
        repository = provenance._get(self.reader, '')
        if (repository.get('full_name') != REPOSITORY or type(repository.get('id')) is not int
                or repository.get('permissions', {}).get('push') is not True):
            raise ValueError('authenticated draft-list access cannot be established')
        candidate = self.bundle['candidates'][source]
        tag = f'source-artifact/{source}/{candidate["artifact_id"]}'
        observations = {(v['release_id'], v.get('asset_id')) for v in known}
        def observe(rid, aid=None):
            # Observation pins identity, never grants permission or verifies bytes.
            if type(rid) is not int or rid <= 0 or (aid is not None and (type(aid) is not int or aid <= 0)):
                raise ValueError('invalid observed remote identity')
            if (rid, aid) not in observations:
                self.evidence('observed', {'source': source, 'release_id': rid, 'asset_id': aid,
                    'handoff_id': self.review['recovery_handoff']['handoff_id']})
                observations.add((rid, aid))
        def scan():
            values, _ = provenance._pages(self.reader, '/releases', repository['id'])
            matches = [v for v in values if v.get('tag_name') == tag]
            if len(matches) > 1:
                for match in matches:
                    observe(match['id'])
                raise ValueError('duplicate or conflicting releases for exact candidate tag')
            return matches
        def identity(value):
            # Download counters/profile presentation are not immutable identity.
            return {**{k:value.get(k) for k in ('id', 'tag_name', 'draft', 'published_at')},
                'assets': [{k:a[k] for k in ('id', 'name', 'state', 'size', 'digest') if k in a}
                           for a in value.get('assets', [])]}
        first = scan()
        if not first:
            if releases:
                raise ValueError('known numeric release missing from complete listing; never replace it')
            if prepare_intent:
                raise ValueError('uncertain create outcome unresolved; never repeat creation')
            if scan():
                raise ValueError('release appeared during discovery; retry read-only reconciliation')
            self.stage = 'remote_tag_identity'
            by_tag, _ = self.reader.request('GET', '/releases/tags/' + quote(tag, safe=''), expected=(200,404))
            if by_tag is not None:
                if isinstance(by_tag, dict) and type(by_tag.get('id')) is int:
                    observe(by_tag['id'])
                raise ValueError('release listing/tag disagreement; do not create')
            return None
        rid = first[0].get('id')
        observe(rid)
        if source != 'census_acs1' and not prepare_intent:
            raise ValueError('unexpected remote release without a recorded create intent; review required')
        if type(rid) is not int or rid <= 0 or (releases and releases != {rid}):
            raise ValueError('known release missing or tag reassigned')
        self.stage = 'remote_release_identity'
        release = provenance._get(self.reader, f'/releases/{rid}')
        GitHubReleaseArtifactPublisher._validate_release(release, tag)
        if (release['id'] != rid or type(release.get('draft')) is not bool
                or identity(release) != identity(first[0])
                or (release['draft'] and release.get('published_at') is not None)
                or (not release['draft'] and not release.get('published_at'))):
            raise ValueError('incompatible release state or identity')
        self.stage = 'remote_tag_identity'
        by_tag, _ = self.reader.request('GET', '/releases/tags/' + quote(tag, safe=''), expected=(200,404))
        if by_tag is None:
            if not release['draft']:
                raise ValueError('published release must resolve by exact tag')
        elif identity(by_tag) != identity(release):
            raise ValueError('by-tag release conflicts with numeric identity')
        remote_assets = release['assets']
        for asset in remote_assets:
            observe(rid, asset.get('id'))
        if len(remote_assets) > 1 or (not release['draft'] and not remote_assets):
            raise ValueError('unexpected or missing release assets')
        if assets and (not remote_assets or assets != {remote_assets[0].get('id')}):
            raise ValueError('known asset missing or replaced')
        if remote_assets:
            asset = remote_assets[0]
            if (asset.get('name') != candidate['artifact_id'] + '.tar'
                    or type(asset.get('id')) is not int or asset['id'] <= 0
                    or asset.get('state', 'uploaded') != 'uploaded'):
                raise ValueError('unexpected release asset identity or incomplete upload')
            self.stage = 'remote_asset_verification'
            with tempfile.TemporaryDirectory() as td:
                path = Path(td) / 'remote.tar'
                self.reader.download_asset(asset['id'], path)
                if sha256_file(path) != candidate['package_sha256']:
                    raise ValueError('existing remote asset bytes conflict; never overwrite')
                GitHubReleaseArtifactPublisher._validate_package(path, Path(td)/'artifact', self.metadata[source])
        self.stage = 'remote_release_stability'
        second = scan()
        final = provenance._get(self.reader, f'/releases/{rid}')
        if len(second) != 1 or identity(second[0]) != identity(release) or identity(final) != identity(release):
            raise ValueError('remote release changed during reconciliation')
        asset_id = remote_assets[0]['id'] if remote_assets else None
        verification = 'package_verified' if remote_assets else 'release_verified'
        if not any(v.get('release_id') == rid and v.get('asset_id') == asset_id and
                   v.get('verification') == verification for v in known):
            self.evidence('adopted', {'source': source, 'release_id': rid, 'asset_id': asset_id,
                'verification': verification, 'handoff_id': self.review['recovery_handoff']['handoff_id']})
        return release

    def publish(self):
        self.fence(reconstruct=True)
        for source in CANDIDATES:
            if source not in self.receipts:
                self.known_remote(source)
                publisher = BootstrapPublisher(self.api)
                metadata = self.metadata[source]
                uri = metadata['logical_artifact_uri']
                self.stage = 'retained_upload_bytes'
                payload = (self.directory / self.bundle['candidates'][source]['package_path']).read_bytes()
                if hashlib.sha256(payload).hexdigest() != CANDIDATES[source]['package_sha256']:
                    raise ValueError('retained upload bytes changed')
                # No automatic mutation retries. On any uncertain response exit;
                # a new invocation discovers and verifies the exact remote asset.
                for label, action in [('prepare', lambda: publisher.prepare(uri, payload, metadata)),
                                      ('upload', lambda: publisher.upload(uri)),
                                      ('verify', lambda: publisher.verify(uri))]:
                    self.fence()
                    selected = self.known_remote(source)
                    if label == 'prepare':
                        publisher.select(f'source-artifact/{source}/{self.bundle["candidates"][source]["artifact_id"]}', selected)
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
                self.stage = 'remote_receipt_verification'
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
                        session.stage = 'catalog_write_confirmation'
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
        self.stage = 'installation_evidence'
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
    parser.add_argument('--prior-journal', type=Path, required=True, help='Original journal, read-only; never rebound')
    parser.add_argument('--journal', type=Path, required=True, help='Separate durable recovery directory outside checkout/bundle')
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
            if (args.request.resolve().is_relative_to(root) or args.request.resolve().is_relative_to(directory)
                    or args.request.resolve().is_relative_to(args.prior_journal.resolve())):
                raise ValueError('review request output must be outside checkout, retained bundle and original journal')
            _save(args.request, request(spec, read(directory / 'bundle.json'), directory, root, prior_journal=args.prior_journal, journal=args.journal))
            print(json.dumps({'review_request': 'proposed', 'publication': 'not_performed', 'activation': False}))
            return 0
        if args.decision is None or args.journal is None:
            raise ValueError('live decision locator and durable journal required')
        with Session(spec=spec, directory=directory, root=root, review=read(args.request),
                locator=read(args.decision), journal=args.journal.resolve(), prior_journal=args.prior_journal.resolve()) as session:
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
