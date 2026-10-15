# Bounded ACS provider evidence and ACS5 2023 handoff

This extends the successful PR #284 pilot. The manual workflow selects one exact
physical product (`census_acs1` or `census_acs5`) and 1–5 unique explicit years,
using the existing `acs_backfill.scope` bounds. Years execute ascending. Syntax
bounds 2005–2100 are not provider availability claims. There is no latest-year
fallback, product substitution, automatic exclusion or scope/gap approval.
Ordinary dataset discovery must succeed independently for every selected year;
ACS1 2020 cannot be replaced by experimental data.

## First recommended reusable invocation

After external review and merge, and only when an operator authorizes capture:

```bash
gh workflow run acs-provider-evidence.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f source_id=census_acs5 -f vintages=2020,2021,2022 -f batch_size=3
```

This captures the next recent ACS5 years after the existing 2023 pilot. It does
not authorize physical acquisition. The former fixed-year workflow is replaced;
historical pilot runs remain linked to their original commit and workflow path.
No workflow is dispatched by this implementation change.

## Contracts, limits and prerequisites

`historical.preflight` and `validate_evidence` retain all interpretation: exact
ordinary dataset/year/identifier/accessURL, label/concept/numeric metadata for
`B01003_001E` and `B19013_001E`, exact headers/geography codes, complete accounting,
and omission receipts. The transport only records bounded HTTP responses. Every
year requests the current complete 223-member `geography_plan()` with the ten
existing exclusions. Changed counts or variables fail closed. Configuration
hashes are bound and checked before and after each year. Current county/CBSA
identities are not assumed to have existed historically; incompatible responses
remain failures, not new exclusions or synthesized values.

The workflow is main-only, `workflow_dispatch` only, `contents: read`, with full
checkout ancestry and no persisted Git credentials. No repository token is passed
to capture. `CENSUS_API_KEY` enters only the capture step and observation transport
parameters. Response URLs/headers and exception details are never persisted.
Raw, URL-escaped and JSON-escaped credential echoes are rejected before body
persistence. Never enable HTTP debug logs.

Limits: 225 GETs/year, 1,125/five-year batch, no redirects/retries, 60 seconds per
request, 15 minutes/year, 30 minutes/batch, 40-minute job. Metadata bodies are
capped at 32 MiB, observation bodies at 1 MiB, total output at 256 MiB with a
2 MiB reserve for bounded audit records. Batch-size/year duplicates, malformed
input and unsupported syntax bounds are rejected before output/network access.

Operator dispatch permission, a valid repository Census secret and hosted Census
HTTPS are required. The successful pilot establishes its own live behavior, not
availability for every other year. Generalized hosted execution is not performed
during review. There are no catalog, pin, release, governance, historical-state,
acceptance or serving writers. Macro F4 and BPS policy remain unchanged.

## Artifact, failure isolation and recovery

Download `acs-provider-evidence` (30-day retention). Upload is attempted with
`if: always()`; setup failures or hard timeouts may still prevent an artifact.

- Each `<year>/provider-evidence.json` exists only after full validation, in the
  existing year-keyed `acs_historical_provider_preflight_v1` map format.
- `<year>/responses/*.body` preserves HTTP content bytes after Requests content
  decoding, not compressed wire bytes. Each year independently numbers its
  discovery, variable metadata and 223 member responses.
- `<year>/response-manifest.json` binds safe exact URL, status, raw length/SHA-256
  and canonical decoded JSON hash. Empty 204 binds zero body bytes and decoded
  null, matching omission receipts. Invalid responses may retain raw hashes
  without a validated decoded hash.
- `<year>/summary.json` records validation, code commit, configuration, accounting,
  receipt IDs and file hashes. `capture_id` hashes source/year/config/snapshot/
  evidence/response-manifest facts. Identical facts have identical IDs; changed
  raw/provider/configuration bytes change identity. Code/run metadata remains
  traceability, not approval.
- Root `provider-evidence.json` contains **successful years only**, when it fits
  the output budget. `batch-summary.json` retains every requested year, per-year
  results, successful/failed lists, request count and deterministic batch ID.
  Any year or aggregation failure sets `validated=false` and exits nonzero.

Failures do not hide or erase independent successes. Later years are attempted
within the shared budgets. A failed year has no valid evidence file. Safe partial
responses survive for inspection; `last_request` is progress, not necessarily the
incompatible member because historical validation occurs after collection.
Only observed empty 204, null and three known sentinels can produce receipts;
errors, missing geography/headers and unknown sentinels fail closed. Receipts
record observations, not eligibility or whole-vintage gap approval.

Recover with a new explicit invocation containing **only failed years**, in fresh
output. Preserve prior successful artifacts and compare identities. There is no
implicit cache/resume trust, overwrite or hidden retry. An aggregate-size failure
leaves individual successes available but the batch remains failed. Provider or
configuration drift requires new external review.

## Staged remaining evidence schedule

This is proposed scope, not production authority. It covers 32 remaining
missing-year captures after the existing ACS5 2023 pilot. Both 2024 conflicts stay
in an independent parent-review lane; do not duplicate physical acquisition or
select a parent from equal data hashes.

| Stage | Product | Exact years | batch_size |
|---|---|---|---:|
| 1 | census_acs5 | 2020,2021,2022 | 3 |
| 2 | census_acs5 | 2015,2016,2017,2018,2019 | 5 |
| 3 | census_acs5 | 2010,2011,2012,2013,2014 | 5 |
| 4 | census_acs5 | 2009 | 1 |
| 5 | census_acs1 | 2021,2022,2023 | 3 |
| 6 | census_acs1 | 2015,2016,2017,2018,2019 | 5 |
| 7 | census_acs1 | 2010,2011,2012,2013,2014 | 5 |
| 8 | census_acs1 | 2005,2006,2007,2008,2009 | 5 |

Review every stage before proceeding. Earlier metadata gaps, including ACS1
2010/2011 concept fields, remain gates; do not relax validation or copy another
year's metadata.

## Exact ACS5 2023 handoff status

Independently inspected GitHub metadata confirms successful main dispatch run
`38598860460`, commit `55182ef227f1c66f1da91ab102999a1b93187c0b`, artifact
`11912706827` / `acs5-2023-provider-evidence`, advertised ZIP SHA-256:

```text
6d96f15258f4e718b3612d1afea635195dde71dc39770831189bd1c26de22b1d
```

The user reports independent verification of 225 bodies, 438 available metric
observations, eight no-content observations and four exact 204 receipts. **This
change does not claim a new independent verification of those bytes.** The
download connector returned a file-service reference, but direct GitHub ZIP
access and archive transfer into this workspace are blocked by the configured
proxy. No substitute evidence file was fabricated or adopted. Supporting metadata
and unresolved checks are in `docs/audit/acs5_2023_evidence_handoff.json`.

When original archive bytes become available through an allowed transport:

```bash
python -m jobs.monthly_refresh.acs_evidence_review \
  --archive /tmp/acs5-2023-11912706827.zip \
  --output /tmp/acs5-2023-reviewed
```

The offline CLI is bound to the exact advertised digest. It checks ZIP CRC,
strict member/size allowlists, all 225 raw lengths/hashes/decoded identities,
evidence/manifest hashes, current config/membership, and snapshot/receipts rebuilt
through the existing `historical.preflight` and `validate_evidence`. It performs
no network or production writes. Success emits unchanged original map bytes as
`provider-acs5-2023.json` plus local `verification.json` marked review-candidate
only. Failure emits no candidate. Fixture verification passed; the live archive
could not be replayed here.

## Evidence adoption and acquisition-plan boundary

The accepted evidence input is the year-keyed map; do not wrap provenance inside
the historical provider contract. After actual verification, externally review
unchanged evidence at `config/acs_backfill_reviews/provider-acs5-2023.json` with
its source/archive/config verification record in documentation. The existing
backfill runbook explicitly requires separately reviewed input on main. Main
review/merge is the adoption boundary; self-consistent hashes are not approval.
This PR does not adopt inaccessible evidence or approve scope/gaps.

Historical governance registry is absent at inspected main
`55182ef227f1c66f1da91ab102999a1b93187c0b`. Existing read-only backfill planning
permits a missing-year plan without scope/gap approval. Approved scope/gaps are
materialization requirements. Planning still needs exact evidence and a fresh
scoped catalog reconciliation. No broad catalog investigation was repeated.

After the verified evidence is separately reviewed onto main:

```bash
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=preflight -f source_id=census_acs5 -f vintages=2023 -f batch_size=1 \
  -f evidence_file=config/acs_backfill_reviews/provider-acs5-2023.json
```

This command is documented, **not dispatched**. It produces the exact hosted plan
ID under the final reviewed code/config/runtime identity. No exact plan ID is
reported now: archive bytes are inaccessible, reviewed evidence is absent on main
and this PR changes code identity. Do not reuse the earlier unready B1 plan ID.
Plan authorization/execution, scope/gap approval, physical publication, historical
bootstrap and serving promotion remain separate prohibited actions in this task.

## Offline validation

```bash
PYTHONPATH=. pytest -q tests/test_acs_provider_evidence.py \
  tests/test_acs_backfill.py tests/test_acs_boundary_hardening.py \
  tests/test_acs_historical_materialization.py tests/test_acs_c_physical_integration.py
```
