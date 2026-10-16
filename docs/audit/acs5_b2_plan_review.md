# ACS5 B2 exact hosted plan adoption review

Proposes the original `plan.json` from preflight run `38948457315`, artifact
`12066892828`, unchanged at:
`config/acs_backfill_reviews/plan-acs5-b2-2018-2019-2020-2021-2022.json`.
This is `REVIEW_CANDIDATE_ONLY`: no acquisition authorization or execution.

Independently verified original uploaded archive:

- ZIP SHA-256: `445d808aa5bce940aa6145374c3094530df2754df7e1b7533e5eaa393d74ee33`.
- ZIP bytes: 65,591; CRC passed; exactly one bounded regular `plan.json` member,
  with no duplicate, traversal, symlink or encrypted entries.
- Original plan SHA-256: `09ed5e185317386ae3c0bfe53116a6f9300c9323fde20e42d38707a09ceb46e1`.
- Original plan bytes: 678,054. No reformatting or regeneration occurred.
- Exact semantic plan ID, checked with existing `verify_identity`/`verify_plan`:
  `acs_historical_execution_plan_v1__ba06f0384549ae1b501c62e957bf496854c46e4be6938b695d48651232349563`.

## Entry and authority verification

Exact source `census_acs5`, vintages `[2018,2019,2020,2021,2022]`, batch size 5.
All five entries are `missing_acquisition`, with no parents, approved gaps or
unresolved verification requirements. Current main's scoped catalog independently
matches those classifications. Each embedded provider evidence object exactly
matches the adopted year-keyed map and passes the unchanged `validate_evidence`.
Both configured variables, ordinary dataset/year, snapshots, all 223 requested
geographies and ten configured exclusions match. Each year has 446 available
metric observations and zero omissions; total 2,230. 2021 matches the successful
retry provenance, not the failed original capture. All capture origins and
snapshot hashes are retained in `acs5_b2_plan_verification.json`.

Repository/code authority is main commit
`65c6dfac36793536184f2062bfab34d40515b221`; baseline ancestry, repository identity,
code SHA and governed configuration match inspected main. Historical governance
remains bound as `absent`; this is not scope/gap or materialization approval.
Only this plan and two audit records change. Acquisition implementation, workflow
definitions, dependencies, governed configuration and production pointers do not.
Authorization must recheck current scoped inventory and authority after merge.

## Immutable runtime boundary

The exact original runtime object remains in the plan and audit JSON, including
CPython `3.11.17 (main, Oct  1 2026, 14:32:31) [GCC 13.3.0]`, executable SHA
`d601e62487858384d3000725c3b2e16e86ff11a6ad0b0810b76281e45426ea62`,
pandas 2.2.2, NumPy 2.4.6 and PyArrow 26.0.0 with their full installed RECORD
hashes. Existing runtime verification checks declared installed bytes too.

Successful preflight job `117133473576` logs independently confirm Ubuntu 24.04
image `20261012.334`, CPython 3.11.17 and those package versions. Future hosted
compatibility is conditional: moving runner/Python/package selections do not
attest identical future executable/wheel hashes. Authorization and execution
must compare the entire actual runtime identity and fail closed on any drift.
Local review runtime differs and was never substituted into the plan. Structural
self-consistency against the plan's identity is not actual-runtime certification.
No dependency or workflow changes, compatibility bypasses or plan rewrites are
introduced. Any mismatch requires an independently reviewed resolution.

## Future authorization command — not dispatched

After external review/merge and separate authorization approval:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=authorize \
  -f plan_file=config/acs_backfill_reviews/plan-acs5-b2-2018-2019-2020-2021-2022.json \
  -f reviewed_plan_id=acs_historical_execution_plan_v1__ba06f0384549ae1b501c62e957bf496854c46e4be6938b695d48651232349563
```

Expected successful output: `acs-backfill-authorize`, containing the unchanged
plan and distinct `acquire_physical_only` authorization. Execution requires its
own separately authorized dispatch, exact plan ID and successful authorization
run. No merge, authorization, execution, governance activation, 2024 selection,
serving or historical-materialization change is performed by this PR.

## Read-only validation

231 existing backfill, boundary-hardening, historical-materialization and
physical-integration tests passed, including runtime drift and installed-byte
verification. Final plan/ZIP byte equality, staged diff checks and unchanged
execution-code/workflow/dependency checks passed. No hosted workflow was run.
