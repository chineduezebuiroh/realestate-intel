# ACS5 2023 hosted provider-evidence pilot

This manual job captures official provider evidence for exactly `census_acs5`,
ordinary `acs/acs5`, vintage `2023`. It calls the existing
`sources.census_acs.historical.preflight` and `validate_evidence`, using the
complete governed `geography_plan()` (223 members and ten frozen exclusions).
There are no product/year/member overrides or latest-vintage fallback.

The existing backfill workflow consumes reviewed evidence; it does not read
provider observations in its read-only planning job. This pilot supplies that
missing hosted transport/output adapter. It uses the existing ACS convention of
adding `CENSUS_API_KEY` solely to observation request transport parameters.
Discovery and variable metadata are read without credentials. Provider validation
and omission classification remain entirely in the existing historical contract.

## Hosted invocation

After this change is separately reviewed and merged, an authorized operator can
run the following. Implementation review does **not** dispatch or merge it.

```bash
gh workflow run acs5-2023-provider-evidence.yml \
  --repo chineduezebuiroh/realestate-intel --ref main
```

The workflow is `workflow_dispatch` only, restricted to this repository's main
branch, and checks out main with full ancestry and no persisted Git credentials.
Its only repository permission is `contents: read`. The Census secret enters
only the capture step; no repository token is passed to the evidence program.
It has no production publisher, catalog, pin, governance, state, acceptance or
serving writer. Uploading an Actions artifact and step summary are its outputs.

Repository secret `CENSUS_API_KEY` must be available to the capture job and valid
for official Census observation queries. Hosted outbound HTTPS to
`api.census.gov` must work; local Cloud CONNECT 403 does not establish hosted
availability. GitHub-hosted secret/network readiness has not been verified by
offline tests. Operator workflow-dispatch permission and a reviewed main merge
are also prerequisites. No historical scope/gap approval or physical acquisition
authorization is granted by running this evidence-only job.

## Artifact and validation

Download the run's `acs5-2023-provider-evidence` artifact (30-day retention):

- `provider-evidence.json`: only on full success, map `{"2023": <evidence>}` in
  the existing `acs_historical_provider_preflight_v1` contract. This is a review
  input compatible with the existing backfill `--evidence` interface.
- `responses/000.body` through `224.body`: successful capture contains discovery,
  variables and all 223 observation response bodies. These are HTTP content
  bytes after Requests content decoding, not compressed wire bytes.
- `response-manifest.json`: credential-free exact request URLs, HTTP statuses,
  body sizes/SHA-256 and existing canonical decoded-JSON hashes. An empty 204
  has zero body bytes and the canonical JSON null hash used by omission receipts.
  A rejected HTTP/schema response can have a raw hash but no decoded hash.
- `summary.json`: validation result, governed configuration hashes, accounting,
  omission receipt IDs, evidence/manifest hashes and no-production-write flag.
  Logs and the Actions step summary contain only bounded status/count information.

Exact ordinary product/year discovery must resolve once with the existing opaque
identifier/accessURL checks. Both `B01003_001E` and `B19013_001E` require label,
concept and numeric predicate metadata. Every requested member and variable must
be accounted for exactly once; response headers, state/county/us/metro codes and
safe request URLs must match. Existing null/three-known-sentinel and actual empty
204 observations require exact `acs_historical_omission_evidence_v1` receipts.
Unknown sentinels, errors, missing geographies/headers, conflicting metadata,
nonempty 204 bodies and whole-vintage absence fail closed. No code crosswalk,
synthetic value, threshold inference or expanded exclusion is introduced.

The pilot does not assume current county/CBSA identities existed in 2023.
Observed incompatible responses remain in the diagnostic artifact when safe;
they are not turned into no-content receipts. Successful no-content receipts
record observations, not eligibility claims or historical governance approvals.

The transport makes at most 225 GETs without redirects or retries, uses existing
60-second request timeouts, caps metadata bodies at 32 MiB and observation bodies
at 1 MiB, and enforces a 15-minute collection budget inside a 20-minute job.
Response URLs, headers and transport exception details are never persisted.
Responses containing the credential or its URL/JSON escaped form are rejected
before any body is written. Never enable HTTP debug logging for this job.

On failure the process exits nonzero and the workflow attempts artifact upload
with `if: always()`. Partial safe captures, the response manifest and a redacted
failure summary remain available; no `provider-evidence.json` is emitted.
`last_request` is a progress diagnostic, not necessarily the incompatible member:
the existing preflight performs full member/omission validation after collection.
If setup or a hard job timeout prevents capture/upload, the artifact may be
absent; that remains unknown evidence, not provider nonpublication.

## Review and handoff

Independently check artifact hashes, official provenance, every member's
accounting, omissions and current configuration. Run `validate_evidence` again
against the reviewed governed plan. Only a successful validated evidence map is
eligible for a separately reviewed file under `config/acs_backfill_reviews/` on
main. The pilot does not commit that file. Subsequently use the existing
one-vintage backfill preflight to generate a reviewed plan, then seek separate
exact-plan authorization before physical acquisition. Preserve raw response
identity; provider drift requires new review rather than synthetic replacements.

Offline validation:

```bash
PYTHONPATH=. pytest -q tests/test_acs_provider_evidence.py \
  tests/test_acs_backfill.py tests/test_acs_boundary_hardening.py \
  tests/test_acs_historical_materialization.py tests/test_acs_c_physical_integration.py
```
