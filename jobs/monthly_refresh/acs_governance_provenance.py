"""Read-only initial ACS governance bootstrap authorization evidence verification.

Not a recurring monthly refresh gate. No approval, comment-posting, registry
writer or production capability is supplied. GitHub identifies the account;
personal human authorship additionally requires the owner's truthful attestation
and human review (an API cannot distinguish shared credentials from a person).
"""
from __future__ import annotations

import argparse
import base64
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from urllib.parse import urlparse, parse_qs

from core.source_artifacts.github_release import GitHubAPI
from core.source_artifacts.hashing import sha256_json

DECISION_VERSION = 'acs_initial_governance_decision_v1'
AUDIT_VERSION = 'acs_initial_governance_authorization_audit_v1'
SUBJECT_FIELDS = ('subject', 'documentary_evidence', 'approved_request_scope', 'config_hashes', 'limitations')
COMMENT_FIELDS = ('id', 'html_url', 'issue_url', 'user', 'created_at', 'updated_at', 'body', 'performed_via_github_app')


def _time(value):
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if stamp.utcoffset() is None:
            raise ValueError('timezone absent')
        return stamp
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError('GitHub timezone timestamp required') from exc


def body_hash(body):
    if not isinstance(body, str):
        raise ValueError('exact UTF-8 decision body required')
    return hashlib.sha256(body.encode('utf-8')).hexdigest()


def comment_snapshot(comment):
    """Stable audit fields; no token, avatar/profile counters or derived approval."""
    if not isinstance(comment, dict) or any(k not in comment for k in COMMENT_FIELDS):
        raise ValueError('incomplete GitHub comment evidence')
    result = {k: deepcopy(comment[k]) for k in COMMENT_FIELDS}
    user = result['user']
    if not isinstance(user, dict) or any(k not in user for k in ('id', 'login', 'type')):
        raise ValueError('GitHub account identity absent')
    result['user'] = {k: user[k] for k in ('id', 'login', 'type')}
    return result


def validate_comment_metadata(provenance):
    value = provenance.get('owner_comment')
    keys = {'comment_id', 'comment_url', 'author_login', 'author_user_id', 'created_at', 'updated_at', 'body_sha256', 'snapshot_sha256'}
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError('exact owner-comment metadata required')
    if any(type(value[k]) is not int or value[k] <= 0 for k in ('comment_id', 'author_user_id')):
        raise ValueError('numeric GitHub comment/author identities required')
    if value['author_login'] != provenance['reviewer_login'] or value['comment_url'] != provenance['review_url']:
        raise ValueError('owner-comment author/URL differs from provenance')
    from jobs.monthly_refresh.acs_governance_installation import REPOSITORY
    expected = f'https://github.com/{REPOSITORY}/pull/{provenance["reviewed_pr"]}#issuecomment-{value["comment_id"]}'
    if value['comment_url'] != expected or provenance['reviewed_at'] != value['created_at']:
        raise ValueError('owner-comment PR/identity/time mismatch')
    if _time(value['updated_at']) != _time(value['created_at']):
        raise ValueError('edited authorization is blocked; post a fresh personal decision')
    for field in ('body_sha256', 'snapshot_sha256'):
        if not isinstance(value[field], str) or not re.fullmatch('[0-9a-f]{64}', value[field]):
            raise ValueError('exact comment evidence digest required')


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('ambiguous authorization JSON')
        result[key] = value
    return result


def decision_body(proposal, pr, commit, rationales):
    """Specification of the human-authored JSON body, not an authorization API.

    This pure formatter is intentionally not exposed as a posting command.
    Generated text is never evidence that a human decided anything.
    """
    from jobs.monthly_refresh.acs_governance_installation import REPOSITORY
    return {'schema_version': DECISION_VERSION, 'repository': REPOSITORY,
            'reviewed_pr': pr, 'reviewed_commit': commit,
            'personally_reviewed_and_authorized': True, 'authorship': 'human_personal',
            'decisions': {name: {**{k: deepcopy(template[k]) for k in SUBJECT_FIELDS},
                'decision': 'approved', 'decision_rationale': rationale}
                for name, rationale in rationales.items()
                for template in [proposal['approval_templates'][name]]}}


def _pages(api, endpoint, repository_id):
    """Never trust a missing Link or short page as end-of-list; probe to empty."""
    result = []; seen = set(); pages = []; must_continue = False
    for page in range(1, 1001):
        path = f'{endpoint}?per_page=100&page={page}'
        values, headers = api.request('GET', path)
        if not isinstance(values, list):
            raise ValueError('paginated GitHub evidence unavailable')
        link = next((v for k, v in headers.items() if k.lower() == 'link'), '')
        next_links = re.findall(r'<([^>]+)>;\s*rel="next"', link)
        if 'rel="next"' in link:
            if len(next_links) != 1:
                raise ValueError('ambiguous pagination continuation')
            parsed = urlparse(next_links[0])
            allowed_paths = {urlparse(api.api).path + endpoint, f'/repositories/{repository_id}' + endpoint}
            if (parsed.scheme != 'https' or parsed.netloc != 'api.github.com' or parsed.fragment
                    or parsed.path not in allowed_paths
                    or parse_qs(parsed.query) != {'per_page': ['100'], 'page': [str(page + 1)]}):
                raise ValueError('pagination omission or unexpected continuation')
        pages.append({'path': path, 'ids': [item.get('id') for item in values], 'link': link})
        if not values:
            if next_links or must_continue:
                raise ValueError('empty page with omitted continuation evidence')
            return result, pages
        must_continue = bool(next_links)
        for item in values:
            key = item.get('id')
            if type(key) is not int or key <= 0 or key in seen:
                raise ValueError('duplicate/invalid paginated authorization evidence')
            seen.add(key); result.append(item)
    raise ValueError('pagination bound exhausted; no partial verification')


def _get(api, path):
    value, _ = api.request('GET', path, expected=(200, 404))
    if not isinstance(value, dict):
        raise ValueError('authorization evidence deleted or unavailable')
    return value


def verify_authorizations(provenances, token, root, *, prior_snapshot=None):
    """Fresh constrained GETs only. Snapshots/local fixtures never authorize.

    Returns an audit result, never production authority. No injectable API is
    accepted. Unit tests intercept HTTP below this existing transport boundary.
    """
    from jobs.monthly_refresh.acs_governance_installation import (
        REPOSITORY, PROPOSAL_PATH, generate, validate_proposal, _provenance,
    )
    proposal = generate(root)
    if not isinstance(provenances, dict) or set(provenances) != set(proposal['records']):
        raise ValueError('all three independently bound decisions required')
    for name, value in provenances.items():
        _provenance(value, proposal['approval_templates'][name])
    api = GitHubAPI(REPOSITORY, token, read_only=True)
    repository = _get(api, '')
    if repository.get('full_name') != REPOSITORY:
        raise ValueError('authorization repository mismatch')
    repository_id = repository.get('id')
    if type(repository_id) is not int or repository_id <= 0:
        raise ValueError('numeric repository identity required')
    owner = repository.get('owner', {})
    if (owner.get('type') != 'User' or owner.get('login') != REPOSITORY.split('/')[0]
            or type(owner.get('id')) is not int or owner['id'] <= 0):
        raise ValueError('personal repository owner identity required')
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
            item = _get(api, f'/collaborators/{login}/permission')
            permission_cache[login] = item.get('permission') in {'admin', 'write', 'maintain'}
        return permission_cache[login]
    prs = sorted({v['reviewed_pr'] for v in provenances.values()})
    evidence = {}; scans = {}
    for pr in prs:
        selected = {k: v for k, v in provenances.items() if v['reviewed_pr'] == pr}
        def scan():
            pull = _get(api, f'/pulls/{pr}')
            if (pull.get('number') != pr or pull.get('base', {}).get('repo', {}).get('full_name') != REPOSITORY
                    or pull.get('base', {}).get('ref') != 'main'
                    or pull.get('head', {}).get('repo', {}).get('full_name') != REPOSITORY):
                raise ValueError('wrong repository/PR association')
            comments, cp = _pages(api, f'/issues/{pr}/comments', repository_id)
            reviews, rp = _pages(api, f'/pulls/{pr}/reviews', repository_id)
            inline, ip = _pages(api, f'/pulls/{pr}/comments', repository_id)
            if type(pull.get('comments')) is not int or pull['comments'] != len(comments):
                raise ValueError('pagination omitted comments or concurrent PR change')
            if type(pull.get('review_comments')) is not int or pull['review_comments'] != len(inline):
                raise ValueError('pagination omitted inline review comments or concurrent PR change')
            return {'pull': pull, 'comments': comments, 'reviews': reviews, 'inline': inline, 'pages': cp + rp + ip}
        first = scan()
        selected_ids = set()
        times = []
        for name, provenance in selected.items():
            sha = provenance['reviewed_commit']
            if first['pull'].get('head', {}).get('sha') != sha:
                raise ValueError('stale reviewed SHA; fresh authorization required')
            content = _get(api, f'/contents/{PROPOSAL_PATH}?ref={sha}')
            reviewed = json.loads(base64.b64decode(content['content'], validate=True), object_pairs_hook=_unique)
            validate_proposal(reviewed, root)
            kind = provenance['provenance_kind']
            is_comment = kind == 'github_owner_decision_comment'
            object_id = (provenance['owner_comment']['comment_id'] if is_comment
                         else int(provenance['review_url'].rsplit('-', 1)[1]))
            path = f'/issues/comments/{object_id}' if is_comment else f'/pulls/{pr}/reviews/{object_id}'
            obj = _get(api, path)
            stream = first['comments'] if is_comment else first['reviews']
            if obj.get('id') != object_id or obj not in stream:
                raise ValueError('direct evidence missing from complete paginated stream')
            user = obj.get('user', {})
            if user.get('login') != provenance['reviewer_login'] or not authorized(user):
                raise ValueError('unauthorized or fixture/bot author')
            if obj.get('performed_via_github_app') is not None:
                raise ValueError('application-authored decision cannot establish personal authorization')
            if obj.get('html_url') != provenance['review_url']:
                raise ValueError('canonical GitHub decision URL mismatch')
            if is_comment:
                meta = provenance['owner_comment']
                capture = comment_snapshot(obj)
                if (user['id'] != owner['id'] or user['login'] != owner['login']
                        or meta['author_user_id'] != user['id']
                        or obj['issue_url'] != api.api + f'/issues/{pr}'
                        or obj['created_at'] != meta['created_at'] or obj['updated_at'] != meta['updated_at']
                        or body_hash(obj['body']) != meta['body_sha256']
                        or sha256_json(capture) != meta['snapshot_sha256']):
                    raise ValueError('owner comment edited, forged or mismatched')
                submitted = obj['created_at']
            else:
                if (obj.get('state') != 'APPROVED' or obj.get('commit_id') != sha
                        or obj.get('submitted_at') != provenance['reviewed_at']
                        or obj.get('pull_request_url') != api.api + f'/pulls/{pr}'
                        or user.get('login') == first['pull'].get('user', {}).get('login')):
                    raise ValueError('genuine non-self APPROVED review required')
                capture = deepcopy(obj)
                submitted = obj['submitted_at']
            body = json.loads(obj['body'], object_pairs_hook=_unique)
            names = {k for k, v in selected.items() if v['provenance_kind'] == kind and v['review_url'] == provenance['review_url']}
            expected = decision_body(proposal, pr, sha, {k: selected[k]['decision_rationale'] for k in names})
            if sha256_json(body) != sha256_json(expected):
                raise ValueError('personal human attestation, exact decisions/subjects/commitments required')
            times.append(_time(submitted)); selected_ids.add((kind, object_id))
            evidence[name] = {'provenance': deepcopy(provenance), 'github_record': capture,
                              'reviewed_proposal_sha256': sha256_json(reviewed)}
        earliest = min(times)
        # Conservative rule: any later authorized human discussion/review outside
        # the selected decisions blocks until fresh decisions resolve it. This
        # catches plain-language withdrawals too; no NLP inference of consent.
        for kind, stream in [('github_owner_decision_comment', first['comments']), ('github_approved_review', first['reviews']), ('github_inline_review_comment', first['inline'])]:
            for item in stream:
                if (kind, item['id']) in selected_ids:
                    continue
                stamp = item.get('updated_at') or item.get('submitted_at') or item.get('created_at')
                if stamp is None:
                    raise ValueError('undated review evidence; cannot exclude supersession')
                if _time(stamp) >= earliest and authorized(item.get('user', {})):
                    raise ValueError('subsequent authorized discussion/review: withdrawal, supersession or contradiction unresolved')
        second = scan()
        if second != first:
            raise ValueError('mutable authorization evidence changed during verification')
        for name, value in selected.items():
            kind = value['provenance_kind']
            object_id = value['owner_comment']['comment_id'] if kind == 'github_owner_decision_comment' else int(value['review_url'].rsplit('-', 1)[1])
            path = f'/issues/comments/{object_id}' if kind == 'github_owner_decision_comment' else f'/pulls/{pr}/reviews/{object_id}'
            current = _get(api, path)
            capture = comment_snapshot(current) if kind == 'github_owner_decision_comment' else current
            if capture != evidence[name]['github_record']:
                raise ValueError('authorization evidence edited/deleted at completion')
        scans[str(pr)] = first
    result = {'schema_version': AUDIT_VERSION, 'namespace': 'audit_only', 'repository': REPOSITORY,
              'checked_at': datetime.now(timezone.utc).isoformat(), 'owner': {k: owner[k] for k in ('login', 'id', 'type')},
              'evidence': evidence, 'complete_scans': scans,
              'checks': {'live_identity_and_ownership': True, 'exact_reviewed_proposal': True,
                         'all_subjects_bound': True, 'pagination_exhausted': True,
                         'no_later_authorized_discussion': True, 'stable_double_scan': True},
              'personal_authorship': 'human attestation; account APIs cannot prove who operated credentials',
              'production_authority': False, 'activation': False}
    if prior_snapshot is not None:
        retained = deepcopy(prior_snapshot)
        digest = retained.pop('snapshot_sha256', None)
        stable_fields = set(result) - {'checked_at', 'complete_scans'}
        if (digest != sha256_json(retained) or set(retained) != set(result)
                or any(sha256_json(retained[k]) != sha256_json(result[k]) for k in stable_fields)):
            raise ValueError('forged/stale retained authorization snapshot')
    return {**result, 'snapshot_sha256': sha256_json(result)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provenance', type=Path, required=True, help='Three genuine provenance objects; read only')
    parser.add_argument('--repository-root', type=Path, default=Path('.'))
    parser.add_argument('--prior-snapshot', type=Path)
    parser.add_argument('--audit-output', type=Path, required=True, help='New audit file outside config; never overwrite')
    args = parser.parse_args()
    root = args.repository_root.resolve(); dest = args.audit_output.resolve()
    if dest.is_relative_to(root / 'config') or dest.exists():
        parser.error('new audit path outside production config required')
    def read(path): return json.loads(path.read_bytes(), object_pairs_hook=_unique)
    result = verify_authorizations(read(args.provenance), os.environ.get('GITHUB_TOKEN', ''), root,
                                   prior_snapshot=read(args.prior_snapshot) if args.prior_snapshot else None)
    # x prevents replacement, including a symlink created since the path check.
    with dest.open('x') as output:
        output.write(json.dumps(result, sort_keys=True, indent=2) + '\n')
    print(json.dumps({'verification': 'passed', 'snapshot_sha256': result['snapshot_sha256'],
                      'production_authority': False, 'activation': False}))


if __name__ == '__main__':
    main()
