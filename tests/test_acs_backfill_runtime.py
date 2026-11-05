"""Offline runtime counterexamples; synthetic wheels are never production-approved."""
from copy import deepcopy
import base64
import csv
import hashlib
import importlib.metadata
import io
import json
from pathlib import Path
import subprocess
import zipfile

import pytest
import yaml

from core.source_artifacts.hashing import sha256_file, sha256_json
from jobs.monthly_refresh import acs_backfill as backfill
from jobs.monthly_refresh import acs_backfill_runtime as runtime
from test_acs_backfill import Harness, IDENTITY, ReviewAPI, auth, plan


@pytest.fixture
def installed(tmp_path, monkeypatch):
    """Build one tiny package and its real installed RECORD; no network required."""
    site = tmp_path / 'site'; site.mkdir()
    metadata = 'fixture-1.0.dist-info'
    payload = {'fixture/__init__.py': b'value = 1\n',
               metadata + '/METADATA': b'Metadata-Version: 2.1\nName: fixture\nVersion: 1.0\n',
               metadata + '/WHEEL': b'Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n'}
    for name, content in payload.items():
        target = site / name; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(content)
    def record(extra=None):
        content = io.StringIO(); writer = csv.writer(content)
        for name, data in {**payload, **(extra or {})}.items():
            encoded = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip('=')
            writer.writerow([name, 'sha256=' + encoded, len(data)])
        writer.writerow([metadata + '/RECORD', '', ''])
        (site / metadata / 'RECORD').write_text(content.getvalue())
    record()
    wheel = tmp_path / 'fixture-1.0-py3-none-any.whl'
    with zipfile.ZipFile(wheel, 'w') as z:
        for name, data in payload.items(): z.writestr(name, data)
        z.writestr(metadata + '/RECORD', (site / metadata / 'RECORD').read_bytes())
    expected = {'version': '1.0', 'wheel': wheel.name, 'sha256': sha256_file(wheel)}
    distribution = importlib.metadata.Distribution.at(site / metadata)
    monkeypatch.setattr(runtime.importlib.metadata, 'distribution', lambda name: distribution)
    policy = {**runtime.profile(), 'distributions': {'fixture': expected}}
    monkeypatch.setattr(runtime, 'profile', lambda: deepcopy(policy))
    environment = {'implementation': 'CPython', 'python_version': '3.11.17', 'system': 'Linux', 'machine': 'x86_64'}
    monkeypatch.setattr(runtime, 'host', lambda: deepcopy(environment))
    monkeypatch.setenv('ACS_BACKFILL_WHEELHOUSE', str(tmp_path))
    return site, metadata, record, environment, wheel, expected


def test_supported_patch_build_and_installer_metadata_differences(installed):
    site, metadata, record, environment, _, _ = installed
    first = runtime.observe(); identity = runtime.compatibility_identity()
    environment['python_version'] = '3.11.18'
    installer = site / metadata / 'INSTALLER'; installer.write_bytes(b'pip\n')
    record({metadata + '/INSTALLER': b'pip\n'})
    second = runtime.observe()
    assert first['implementations'] != second['implementations']
    assert first['python_version'] != second['python_version']
    assert runtime.compatibility_identity() == identity
    assert 'python_executable_sha256' in second and 'python_full_version' in second


@pytest.mark.parametrize('field,value', [('implementation','PyPy'), ('python_version','3.11.19'),
    ('python_version','3.12.0'), ('system','Darwin'), ('system','Windows'), ('machine','aarch64')])
def test_unsupported_hosts_fail(installed, field, value):
    installed[3][field] = value
    with pytest.raises(ValueError, match=field): runtime.compatibility_identity()


@pytest.mark.parametrize('change', ['wheel', 'version', 'bytes', 'forged_record', 'missing_record', 'missing_wheelhouse'])
def test_unapproved_or_tampered_distributions_fail(installed, monkeypatch, change):
    site, metadata, record, _, wheel, expected = installed
    if change == 'wheel': wheel.write_bytes(wheel.read_bytes() + b'changed')
    if change == 'version': expected['version'] = '2.0'
    if change == 'bytes': (site / 'fixture/__init__.py').write_bytes(b'changed')
    if change == 'forged_record':
        # Rehashing local metadata cannot bless bytes absent from the approved wheel.
        target = site / 'fixture/__init__.py'; target.write_bytes(b'changed')
        path = site / metadata / 'RECORD'
        original = path.read_text()
        original_hash = base64.urlsafe_b64encode(hashlib.sha256(b'value = 1\n').digest()).decode().rstrip('=')
        changed_hash = base64.urlsafe_b64encode(hashlib.sha256(b'changed').digest()).decode().rstrip('=')
        path.write_text(original.replace(original_hash, changed_hash))
    if change == 'missing_record': (site / metadata / 'RECORD').unlink()
    if change == 'missing_wheelhouse': monkeypatch.delenv('ACS_BACKFILL_WHEELHOUSE')
    with pytest.raises(ValueError, match='runtime drift'): runtime.compatibility_identity()


def test_lifecycle_across_patch_drift_including_authorization_archive(installed, tmp_path, monkeypatch):
    from test_acs_backfill import GEOGRAPHY, evidence, missing
    identity = {**IDENTITY, 'serialization_runtime': runtime.compatibility_identity()}
    p = backfill.make_plan(source_id='census_acs1', vintages=[2019], batch_size=1,
        identity=identity, code_commit='a'*40, inventory=[missing()], evidence={'2019': evidence()},
        geography=GEOGRAPHY, excluded=[], authority={'repository':backfill.REPOSITORY,'branch':'main','commit':'a'*40})
    installed[3]['python_version'] = '3.11.18'
    current = {**IDENTITY, 'serialization_runtime': runtime.compatibility_identity()}
    authorization = backfill.authorize(p, reviewed_plan_id=p['plan_id'], identity=current, actor='reviewer')
    ReviewAPI(p, authorization).verify(p, authorization)
    import test_acs_backfill
    monkeypatch.setattr(test_acs_backfill, 'IDENTITY', current)
    h = Harness(tmp_path / 'execution'); h.plan = p
    # Harness uses its fixture identity by default; pass the real checked profile.
    h.execute(authorization=authorization, identity=current)
    assert h.acquisitions
    installed[3]['python_version'] = '3.12.0'
    authority = backfill.PlanAuthority(api=h.api, plan=p, root=Path('.'))
    monkeypatch.setattr(backfill, 'repository_identity', lambda root: {**IDENTITY, 'serialization_runtime':runtime.compatibility_identity()})
    with pytest.raises(ValueError, match='python_version'): authority.current_commit(backfill.REPOSITORY)


@pytest.mark.parametrize('changed_path', ['jobs/work.py', 'core/work.py', 'requirements.txt',
    '.github/workflows/acs-historical-backfill.yml', 'config/acs_historical_governance.json',
    'jobs/monthly_refresh/acs_backfill_runtime_profile.json'])
def test_real_git_plan_adoption_is_not_code_or_configuration_drift(tmp_path, monkeypatch, changed_path):
    """Repository identity computed before/after an actual Git adoption commit."""
    root = tmp_path / 'repo'; root.mkdir()
    def git(*args): return subprocess.check_output(['git','-C',str(root),*args], text=True).strip()
    git('init'); git('config','user.email','fixture@example.invalid'); git('config','user.name','Offline fixture')
    git('remote','add','origin',f'https://github.com/{backfill.REPOSITORY}.git')
    for folder in ('core','jobs','sources','config'): (root/folder).mkdir()
    (root/'jobs/work.py').write_text('original\n'); (root/'requirements.txt').write_text('fixed\n')
    git('add','.'); git('commit','-m','baseline'); baseline=git('rev-parse','HEAD')
    monkeypatch.setattr(backfill,'BASELINE',baseline)
    monkeypatch.setattr(backfill,'governed_config_hashes',lambda root:{'fixture':sha256_file(root/'requirements.txt')})
    monkeypatch.setattr(backfill,'runtime_identity',lambda:{'fixture':'checked'})
    (root/'jobs/monthly_refresh').mkdir(); (root/'jobs/monthly_refresh/acs_backfill_runtime_profile.json').write_text('{}')
    git('add','.'); git('commit','-m','profile')
    before=backfill.repository_identity(root)
    review=root/'config/acs_backfill_reviews';review.mkdir();(review/'plan.json').write_text('{}')
    git('add','.');git('commit','-m','reviewed plan adoption')
    after=backfill.repository_identity(root);backfill.verify_repository_identity(before,after)
    changed = root / changed_path; changed.parent.mkdir(parents=True, exist_ok=True); changed.write_text('changed\n')
    git('add', '.')
    with pytest.raises(ValueError, match='code_sha256|config_hashes'):backfill.verify_repository_identity(before,backfill.repository_identity(root))


@pytest.mark.parametrize('field', ['repository','baseline','code_sha256','config_hashes','serialization_runtime'])
def test_field_diagnostics_do_not_disclose_values(field):
    current={**IDENTITY,'serialization_runtime':{'profile_sha256':'a'*64}}
    changed=deepcopy(current);changed[field]='SECRET_NEVER_PRINT'
    with pytest.raises(ValueError,match=field) as error:backfill.verify_repository_identity(changed,current)
    assert 'SECRET_NEVER_PRINT' not in str(error.value)


def test_old_plan_is_preserved_and_rejected():
    path=Path('config/acs_backfill_reviews/plan-acs5-2023.json')
    old=backfill.read_json(path)
    assert old['plan_id']=='acs_historical_execution_plan_v1__60606dd220af6eca66a79ffcf5d56f214e7308d0afacc4b84be4802e5535e5d7'
    with pytest.raises(ValueError,match='content identity mismatch'):backfill.verify_plan(old,old['repository_identity'])


def test_workflow_profile_and_observations_are_used_in_both_jobs():
    workflow=yaml.safe_load(Path('.github/workflows/acs-historical-backfill.yml').read_text())
    for job in workflow['jobs'].values():
        assert 'ACS_BACKFILL_WHEELHOUSE' in job['env']
        steps=job['steps']; commands=[s.get('run','') for s in steps]
        assert sum('acs_backfill_runtime --prepare' in c for c in commands)==1
        assert sum('acs_backfill_runtime --observe' in c for c in commands)==1
        observation=next(s for s in steps if s.get('with',{}).get('name','').startswith('acs-backfill-runtime-'))
        assert 'observation.json' in observation['with']['path']


def test_observed_build_and_executable_hash_are_not_cross_run_requirements(installed, monkeypatch):
    import sys
    before = runtime.observe(); identity = runtime.compatibility_identity()
    original = runtime.sha256_file
    monkeypatch.setattr(runtime, 'sha256_file', lambda p: 'f'*64 if p == Path(sys.executable) else original(p))
    monkeypatch.setattr(runtime.sys, 'version', '3.11.18 reviewed compatible alternative build')
    after = runtime.observe()
    assert before['python_full_version'] != after['python_full_version']
    assert before['python_executable_sha256'] != after['python_executable_sha256']
    assert runtime.compatibility_identity() == identity


@pytest.mark.parametrize('boundary', ['pin', 'result', 'catalog'])
def test_each_durable_write_rechecks_actual_runtime_before_mutation(installed, tmp_path, monkeypatch, boundary):
    from test_acs_backfill import FencedContentsAPI
    from core.source_artifacts.catalog import empty_catalog
    from core.source_artifacts.github_release import GitHubCatalogCAS, GitHubReleaseArtifactPublisher
    import test_acs_backfill
    current = {**IDENTITY, 'serialization_runtime': runtime.compatibility_identity()}
    monkeypatch.setattr(test_acs_backfill, 'IDENTITY', current)
    receipts = []; finalize = GitHubReleaseArtifactPublisher.finalize
    def capture(self, uri):
        receipt = finalize(self, uri); receipts.append(receipt); return receipt
    monkeypatch.setattr(GitHubReleaseArtifactPublisher, 'finalize', capture)
    h = Harness(tmp_path / 'source'); published = h.execute()[0]
    api = FencedContentsAPI(); p = h.plan
    authority = backfill.PlanAuthority(api=api, plan=p, root=Path('.'))
    monkeypatch.setattr(backfill,'repository_identity',lambda root:{**IDENTITY,'serialization_runtime':runtime.compatibility_identity()})
    monkeypatch.setattr(backfill,'_main_snapshot',lambda api:api.tip)
    monkeypatch.setattr(backfill,'_authority_files',lambda *args:None)
    monkeypatch.setattr(backfill,'_code_authority',lambda *args:None)
    # A runtime that was valid at authorization changes before the durable write.
    installed[3]['python_version']='3.12.0'
    if boundary=='pin':
        operation=lambda:backfill.FencedPinStore(api,authority).put(published['pin'])
    elif boundary=='result':
        operation=lambda:backfill.FencedResultStore(api,authority).put(h.results[0])
    else:
        api.objects[backfill.CATALOG_PATH]=empty_catalog()
        store=backfill.FencedPhysicalCatalog(GitHubCatalogCAS(api,backfill.CATALOG_PATH,'main'),authority)
        operation=lambda:store.add(published['publication']['record'],receipts[0])
    with pytest.raises(ValueError,match='python_version'):operation()
    assert not api.writes


@pytest.mark.parametrize('corrupt_download', [False, True])
def test_preparation_requires_hash_locked_wheels_and_never_resolves_install_dependencies(installed, tmp_path, monkeypatch, corrupt_download):
    import shutil
    calls=[]; original_wheel=installed[4]; destination=tmp_path/'prepared'
    def pip(command, *, check):
        assert check is True
        calls.append(command)
        if 'download' in command:
            assert '--only-binary=:all:' in command and '--no-deps' in command and '--require-hashes' in command
            lock=Path(command[command.index('-r')+1]).read_text()
            assert '--hash=sha256:'+installed[5]['sha256'] in lock
            target=destination/original_wheel.name
            shutil.copyfile(original_wheel,target)
            if corrupt_download:target.write_bytes(b'unapproved')
        else:
            assert '--no-index' in command and '--no-deps' in command and '--force-reinstall' in command
            assert command[-1]==str(destination/original_wheel.name)
    monkeypatch.setattr(runtime.subprocess,'run',pip)
    if corrupt_download:
        with pytest.raises(ValueError,match='downloaded_wheel'):runtime.prepare(destination)
        assert len(calls)==1
    else:
        runtime.prepare(destination)
        assert len(calls)==2
