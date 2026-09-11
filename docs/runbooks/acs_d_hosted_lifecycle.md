# ACS-D hosted physical lifecycle proof

## Boundary

This manual proof runs exactly one physical product (`census_acs1` or
`census_acs5`) at a time. The workflow code is checked out from the explicitly
dispatched ref, while pins, the artifact catalog, and cycle results use `main`
as durable production control-plane authority. The workflow has no schedule and
is not a member of the production cohort.

The only permitted mutations are creation/reuse of an exact provider-input pin,
immutable Release candidate/catalog publication, and creation/reuse of the
product's durable source-result record. Stop if an accepted source pointer,
Source Set, canonical market, serving market, logical ACS family, or production
cohort is involved. `CENSUS_API_KEY` must exist only as a repository Actions
secret and runtime environment value.

## Preflight

Run from a clean checkout of the proposed execution ref:

```bash
python -m pytest -q tests/test_acs_c_physical_integration.py tests/test_acs_d_hosted_dispatch.py
PYTHONPATH=. python scripts/smoke_tests/170_179/175_monthly_source_cohort_orchestrator.py
git diff --check
```

After the workflow change is merged and available to GitHub Actions, choose a
single existing governed cycle ID and keep it unchanged for every command:

```bash
export REPO=chineduezebuiroh/realestate-intel
export EXECUTION_REF=monthly-refresh-orchestration
export CYCLE_ID='<exact-governed-cycle-id>'
```

Confirm the repository Actions secret exists without reading its value:

```bash
gh secret list --repo "$REPO" | awk '$1=="CENSUS_API_KEY" {found=1} END {exit !found}'
```

## Normal proof, independently

Run ACS1 first and wait for success before starting ACS5:

```bash
gh workflow run acs-monthly-source.yml --repo "$REPO" --ref "$EXECUTION_REF" \
  -f source_id=census_acs1 -f invocation_mode=normal -f cycle_id="$CYCLE_ID"
gh run watch --repo "$REPO" --exit-status "$(gh run list --repo "$REPO" \
  --workflow acs-monthly-source.yml --branch "$EXECUTION_REF" --limit 1 --json databaseId --jq '.[0].databaseId')"

gh workflow run acs-monthly-source.yml --repo "$REPO" --ref "$EXECUTION_REF" \
  -f source_id=census_acs5 -f invocation_mode=normal -f cycle_id="$CYCLE_ID"
gh run watch --repo "$REPO" --exit-status "$(gh run list --repo "$REPO" \
  --workflow acs-monthly-source.yml --branch "$EXECUTION_REF" --limit 1 --json databaseId --jq '.[0].databaseId')"
```

Normal must discover the current eligible vintage independently, persist the
exact snapshot pin to `main` before candidate construction, execute that pin,
publish/verify an immutable candidate, and record the durable result.

## Inspect durable identities and stop boundaries

For each product, inspect the exact pin and result on `main`:

```bash
for SOURCE in census_acs1 census_acs5; do
  gh api -H 'Accept: application/vnd.github+json' \
    "repos/$REPO/contents/config/monthly_source_input_pins/$CYCLE_ID/$SOURCE.json?ref=main" \
    --jq .content | tr -d '\n' | base64 --decode | jq \
      '{source_id,pin_id,provider_release_id,members}'
  gh api -H 'Accept: application/vnd.github+json' \
    "repos/$REPO/contents/config/monthly_source_cycle_results/$CYCLE_ID/$SOURCE.json?ref=main" \
    --jq .content | tr -d '\n' | base64 --decode | jq \
      '{source_id,cycle_id,result:{candidate_artifact_id:.result.candidate_artifact_id,artifact_content_hash:.result.artifact_content_hash,package_sha256:.result.package_sha256,provider_release_id:.result.provider_release_id,accepted_pointer_changed:.result.accepted_pointer_changed}}'
done
```

Also inspect the workflow summaries and catalog record on `main`. Confirm each
result resolves to exactly one published and remotely verified immutable source
record. Confirm the API key is absent from logs, summaries, pins, catalog
metadata, results, URLs, artifact identities, and hashes. Confirm accepted
source pointers, accepted Source Set, canonical, and serving identities did not
move. Stop immediately on any contradiction; do not promote anything.

## Resume and replay proof

Resume both products independently:

```bash
gh workflow run acs-monthly-source.yml --repo "$REPO" --ref "$EXECUTION_REF" \
  -f source_id=census_acs1 -f invocation_mode=resume -f cycle_id="$CYCLE_ID"
gh run watch --repo "$REPO" --exit-status "$(gh run list --repo "$REPO" \
  --workflow acs-monthly-source.yml --branch "$EXECUTION_REF" --limit 1 --json databaseId --jq '.[0].databaseId')"

gh workflow run acs-monthly-source.yml --repo "$REPO" --ref "$EXECUTION_REF" \
  -f source_id=census_acs5 -f invocation_mode=resume -f cycle_id="$CYCLE_ID"
gh run watch --repo "$REPO" --exit-status "$(gh run list --repo "$REPO" \
  --workflow acs-monthly-source.yml --branch "$EXECUTION_REF" --limit 1 --json databaseId --jq '.[0].databaseId')"
```

After both runs succeed, compare their summaries and durable identities with the
normal proof. Resume must resolve the existing pin, perform no provider
discovery, reacquire and hash-check that exact product/vintage snapshot, and
reuse the same immutable candidate and result identity.

Replay uses the same pinned-only semantics and can be proven independently:

```bash
gh workflow run acs-monthly-source.yml --repo "$REPO" --ref "$EXECUTION_REF" \
  -f source_id=census_acs1 -f invocation_mode=replay -f cycle_id="$CYCLE_ID"
gh run watch --repo "$REPO" --exit-status "$(gh run list --repo "$REPO" \
  --workflow acs-monthly-source.yml --branch "$EXECUTION_REF" --limit 1 --json databaseId --jq '.[0].databaseId')"

gh workflow run acs-monthly-source.yml --repo "$REPO" --ref "$EXECUTION_REF" \
  -f source_id=census_acs5 -f invocation_mode=replay -f cycle_id="$CYCLE_ID"
gh run watch --repo "$REPO" --exit-status "$(gh run list --repo "$REPO" \
  --workflow acs-monthly-source.yml --branch "$EXECUTION_REF" --limit 1 --json databaseId --jq '.[0].databaseId')"
```

Watch each returned run to completion and repeat the identity/stop-boundary
inspection. A missing pin in resume or replay is an expected hard failure. A
newer provider vintage appearing after normal must not affect this cycle.

ACS-D ends after this evidence review. It does not authorize accepted-pointer
promotion, Source Set inclusion, canonical/serving mutation, logical ACS
resolution, production-cohort registration, or routine scheduling.
