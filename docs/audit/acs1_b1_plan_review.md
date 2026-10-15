# ACS1 B1 exact execution-plan adoption review

This change proposes the original `plan.json` bytes from hosted preflight run
`38786268710`, artifact `11998451491`, for external review and merge. It grants
no acquisition authorization and dispatches no workflow.

The uploaded original ZIP is 83,369 bytes; SHA-256:
`de4a4759de0b23199ceb59519ad6b0e23e506e41e180e92e4941a77acc2e0da8`.
Independent checks verified exact member allowlist (`plan.json` only), ZIP CRC,
size, absence of symlink/encrypted entries, and the original plan byte hash:
`cb281aa6f848f56ed02dacc755cbe2670222ec7ddf7fa17eb58c4aca76050157`
(873,689 bytes). No plan regeneration, reformatting or substitution occurred.

Adoption path:
`config/acs_backfill_reviews/plan-acs1-b1-2018-2019-2021-2022-2023.json`.

Exact identity verified by existing `verify_identity` and `verify_plan`:
`acs_historical_execution_plan_v1__5245626e78a20a39fc51bf499273d8595f796c0186e928d110a046ae1c97a28a`.

## Entry and authority checks

Source `census_acs1`, vintages `[2018,2019,2021,2022,2023]`, batch size 5.
Each entry is `missing_acquisition`, with zero parents, empty verification
requirements, null verification requirement and no approved gap. Every embedded
provider-evidence object equals the corresponding adopted main evidence and
passes `validate_evidence` for exact ordinary dataset/year, both variables,
snapshot and explicit omission accounting. All 223 governed members and ten
existing exclusions match current configuration. Totals: 1,548 available cells,
682 provider-no-content cells and 341 omission receipts. No exclusions change.

Main authority and code commit are
`0802e4552c84ddfd8d29f3393afa599b96bfd883`. Repository identity, baseline ancestry,
code SHA and governed configuration hashes match inspected main; historical
registry remains bound as `absent`. This is allowed for physical-only acquisition,
not authority to materialize history. This PR changes only the plan and audit
records, so the existing acquisition code/configuration/workflow/dependency
bindings remain intact. Authorization must freshly reconcile the scoped catalog;
this review does not replace that required check.

## Runtime readiness: conditional, not an unconditional execution certification

The exact bound runtime is preserved in the plan and verification record:

- CPython `3.11.17 (main, Oct 1 2026, 14:32:31) [GCC 13.3.0]`, with the exact
  original whitespace-bearing version string retained in the JSON.
- Executable SHA `d601e62487858384d3000725c3b2e16e86ff11a6ad0b0810b76281e45426ea62`.
- pandas 2.2.2, NumPy 2.4.6, PyArrow 26.0.0, each binding its full installed
  RECORD hash. Existing runtime verification also checks declared installed bytes.

Read-only inspection of successful preflight job `116598965032` confirms
Ubuntu 24.04 image `20261012.334`, CPython 3.11.17 at
`/opt/hostedtoolcache/Python/3.11.17/x64`, and those exact package versions.
The job generated the bound hashes in the original plan. The authorization and
execution jobs use the same existing setup (`ubuntu-latest`, setup-python 3.11,
`pip install -r requirements.txt`). Thus the requested environment has been
realized by the hosted preflight; future identical-byte compatibility has **not**
been independently established and is not guaranteed by moving selections or
pip download cache hits. Local review runtime differs and was never substituted
into the plan. Structural self-consistency checks using the plan's own identity
are not a test of actual local or future hosted runtime compatibility.

At eventual separately authorized dispatch, `verify_plan` must compare the entire
actual repository/runtime identity; execution must match the same immutable
identity again. A mismatch must stop before authorization/publication. Never edit
runtime fields or bypass those checks. Restore the exact reviewed environment
through an independently reviewed procedure if available, or separately review a
fresh plan under a deliberately established runtime. No runtime restoration,
workflow modification, new preflight or authorization is performed here.

## Future authorization boundary

After external review and merge, **only when separately authorized**, the exact
existing authorization command is:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=authorize \
  -f plan_file=config/acs_backfill_reviews/plan-acs1-b1-2018-2019-2021-2022-2023.json \
  -f reviewed_plan_id=acs_historical_execution_plan_v1__5245626e78a20a39fc51bf499273d8595f796c0186e928d110a046ae1c97a28a
```

This command is documented, not dispatched. Expected successful output:
`acs-backfill-authorize`, with unchanged plan and separately identified
`acquire_physical_only` authorization. Runtime, current scoped catalog, main
ancestry and code/configuration checks may fail closed. Subsequent execution
requires a separate authorized dispatch binding that exact plan and successful
authorization run; adopting this plan approves neither step. No governance,
2024 selection, serving or Macro change is included.

Full machine-readable verification: `docs/audit/acs1_b1_plan_verification.json`.

## Read-only regression results

231 tests passed across existing backfill, boundary-hardening, historical
materialization and physical-integration suites, including runtime drift, installed
byte verification, stale authority and altered plan rejection. Final staged plan
bytes match the ZIP member exactly; diff checks passed. No hosted workflow ran.
