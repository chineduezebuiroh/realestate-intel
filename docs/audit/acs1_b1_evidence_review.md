# ACS1 B1 provider evidence adoption review

Status: **EXACT UPLOADED CANDIDATES VALIDATED — PROPOSED FOR MAIN REVIEW**.
Evidence remains `REVIEW_CANDIDATE_ONLY`; acquisition is not authorized.

GitHub metadata independently confirms run `38760112145`, artifact
`11985004807` (`acs-provider-evidence`), main commit
`33ccf0f4aa15bed052b6f748050e6847fffb3972`, 7,253,530 compressed bytes,
and advertised ZIP SHA-256
`02f8ec3e230a70c8e6e7e9be006884ceb4b31571ad47179285c56a9caea91f1f`.
The connector downloaded to a file-service reference, but workspace transfer
received proxy CONNECT 403. The digest has **not** been independently computed
from live archive bytes here. The user subsequently ran the PR #287 offline
verifier on the original ZIP on their Mac and uploaded all seven generated files.
This change preserves the five per-year candidates, aggregate map and verification
report byte-for-byte. Uploaded hashes, provenance, current configuration, snapshots,
metadata and full geography/omission accounting were independently checked here
with `validate_evidence`. No raw-response replay was repeated in this workspace.
See `acs1_b1_evidence_verification.json` for the unchanged local-verifier record
and `acs1_b1_evidence_handoff.json` for this session's checks and exact hashes.

The fixed pilot verifier accepts only a root-level ACS5 2023 archive with 225
responses and a hard-coded pilot digest. It cannot accept the hosted batch's
five year directories, batch summary, aggregate map, ACS1 source or 1,125 bodies.
The offline adapter in `scripts/verify_acs_provider_evidence_batch.py` handles
that bounded wrapper and uses the existing `historical.preflight`,
`checked_response` and `validate_evidence` for all provider interpretation.
It neither fetches observations nor changes a workflow or production provider.
It requires an independently supplied exact digest, product, years, run,
artifact and capture commit. Those inputs bind identity; they confer no approval.

Archive verification checks exact member/size allowlists, CRC, duplicate/path/
symlink/encrypted entries, raw and decoded hashes/lengths/URLs, exact ordinary
dataset/year and metadata, all 223 governed members and ten existing exclusions,
current configuration, snapshots and explicit omission receipts. Every year and
the aggregate must agree; failed/partial batches fail closed without candidates.
Successful local output preserves all original evidence bytes and is marked
`REVIEW_CANDIDATE_ONLY`. Credentials are neither requested nor logged.

## Validation in this adoption change

All uploaded candidate and report bytes match their preserved repository copies.
Existing `validate_evidence` passed for every year against current main configuration.
Relevant provider-evidence, backfill, boundary-hardening, historical-materialization
and physical-integration regression suites: **294 passed**. Acquisition code,
workflows, bound configuration and dependencies are unchanged.

## ACS1 B1 verification provenance and next preflight

Steps 1–2 document the successful user-run archive verification. Step 3 is now
proposed in this PR, based on the uploaded exact outputs; steps 4–5 remain future
review and execution boundaries. The original ZIP is unavailable here.

1. Transfer the original ZIP, without re-zipping. If available locally, download:

   ```sh
   gh api repos/chineduezebuiroh/realestate-intel/actions/artifacts/11985004807/zip > /tmp/acs1-b1-11985004807.zip
   ```

2. With the reviewed offline adapter available and current governed config,
   verify entirely offline (module invocation from repository root):

   ```sh
   python -m scripts.verify_acs_provider_evidence_batch \
     --archive /tmp/acs1-b1-11985004807.zip --output /tmp/acs1-b1-reviewed \
     --expected-sha256 02f8ec3e230a70c8e6e7e9be006884ceb4b31571ad47179285c56a9caea91f1f \
     --source-id census_acs1 --vintages 2018,2019,2021,2022,2023 \
     --run-id 38760112145 --artifact-id 11985004807 \
     --capture-commit 33ccf0f4aa15bed052b6f748050e6847fffb3972
   ```

3. Only after success, review and adopt unchanged
   `provider-acs1-2018.json`, `provider-acs1-2019.json`,
   `provider-acs1-2021.json`, `provider-acs1-2022.json`,
   `provider-acs1-2023.json` under `config/acs_backfill_reviews/`, with
   `verification.json` recorded as `docs/audit/acs1_b1_evidence_verification.json`.
   Also adopt the original, byte-preserved aggregate
   `provider-acs1-2018-2019-2021-2022-2023.json`: the existing hosted preflight
   takes one evidence map. This is the same five year records, not a new capture.
   All six evidence files and the unchanged verification report are included
   in this PR. Every file hash matches the uploaded report; each per-year map
   equals its corresponding aggregate entry. A reviewer must independently assess
   provenance, exclusions and provider omissions; successful validation is not
   acquisition authorization or scope/gap approval.
4. After that evidence PR merges, the exact read-only B1 invocation is:

   ```sh
   gh workflow run acs-historical-backfill.yml \
     --repo chineduezebuiroh/realestate-intel --ref main \
     -f action=preflight -f source_id=census_acs1 \
     -f vintages=2018,2019,2021,2022,2023 -f batch_size=5 \
     -f evidence_file=config/acs_backfill_reviews/provider-acs1-2018-2019-2021-2022-2023.json
   ```

   Expected artifact: `acs-backfill-preflight`, containing `plan.json`.
   Stop for any conflict, verification requirement, drift, unexpected gap or
   geographic accounting failure. Review exact plan identity and all five entries.
5. Adopt the exact resulting plan under a safe main review path. **Separate
   authorization** must bind its full plan ID; **separate execution** must bind
   that ID and successful authorization run. Evidence adoption authorizes neither.

## Adopt existing ACS5 2023 plan unchanged

Run `38755879949`, artifact `11983242565` (`acs-backfill-preflight`), main
`33ccf0f4aa15bed052b6f748050e6847fffb3972`; advertised archive SHA-256:
`a38d7be34f0c89c5398742404149b131a077c12e12fdefece4c4ca06c5e7a6b5`.
Expected exact plan ID:
`acs_historical_execution_plan_v1__d7962c19f6f02771be6964c5ef707e2b8594f6f44bfa9e20060a420828c9e77a`.

1. Transfer the exact original archive; verify digest, ZIP CRC and safe members,
   then retain `plan.json` bytes unchanged. This review has inspected metadata,
   not independently downloaded those bytes into the workspace.
2. Verify semantic plan ID with `acs_backfill.verify_identity`, then
   `acs_backfill.verify_plan(plan, plan['repository_identity'])` for structural
   self-consistency. This latter check alone does **not** establish current
   execution readiness. Validate embedded ACS5 2023 provider evidence with
   `validate_evidence` and compare it to the adopted main evidence. Confirm 438
   available cells, eight no-content cells, four receipts, 223 members, ten
   exclusions, missing_acquisition, no parents and no verification requirements.
3. Review bound main/code authority, four governed config hashes plus absent
   governance registry, and the full `serialization_runtime`: Python version and
   executable SHA, pandas/NumPy/PyArrow versions and installed RECORD hashes.
   Compare to the actual runtime selected by the existing hosted acquisition job.
   Never replace that runtime object with a local machine's identity, edit the
   plan or regenerate bytes to manufacture the old plan ID.
4. Adopt the exact `plan.json` bytes as
   `config/acs_backfill_reviews/plan-acs5-2023.json`, with archive/plan provenance
   in documentation, through independent external review and merge. This task
   does not create or adopt the inaccessible file.
5. File-only evidence, plan, documentation, tests and this standalone script do
   not enter the existing acquisition code fingerprint. Confirmed baseline code
   SHA: `6b9d56daab7f272cab0f72887bb046a7e80ba51d8bb392b4127081e72d78aae9`.
   No files under `core/`, `jobs/`, `sources/`, `requirements.txt`, or the
   acquisition workflow change here. Preserve main ancestry and governed config.
   Changes to those bound inputs require a newly reviewed plan.
6. **After separately granted authorization only**, the existing authorize
   dispatch would use:

   ```sh
   gh workflow run acs-historical-backfill.yml \
     --repo chineduezebuiroh/realestate-intel --ref main \
     -f action=authorize \
     -f plan_file=config/acs_backfill_reviews/plan-acs5-2023.json \
     -f reviewed_plan_id=acs_historical_execution_plan_v1__d7962c19f6f02771be6964c5ef707e2b8594f6f44bfa9e20060a420828c9e77a
   ```

   It must recheck live scoped inventory, main authority and actual runtime and
   emit `acs-backfill-authorize`. This command is a future boundary, not a request
   to dispatch now. If actual runtime differs, the old plan is blocked even if
   its JSON is unchanged: restore the exact reviewed environment, or separately
   review a fresh preflight plan under the available runtime. The workflow uses
   moving `ubuntu-latest` / Python 3.11 setup and unconstrained dependencies; a
   successful earlier preflight does not guarantee later runtime equality.
7. A separately authorized execute dispatch requires action `execute`, the same
   full `reviewed_plan_id` and the successful `authorization_run_id`. Only this
   phase publishes physical acquisitions/catalog entries. No execute command was
   run or acquisition authorized in this review.

Absent historical governance is allowed by physical-only planning and does not
approve materialization. Scope/gap decisions, 2024 selection, historical
activation, serving and Macro remain outside this change.
