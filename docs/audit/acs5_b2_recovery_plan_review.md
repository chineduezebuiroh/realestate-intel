# ACS5 B2 transport-recovery plan adoption

**Decision: propose the exact original plan for external adoption review only.**
No acquisition authorization, workflow dispatch or production write is performed.

Source: successful main preflight run `38968320918`, artifact `12075737849`
(`acs-backfill-preflight`). The uploaded original ZIP is 65,590 bytes; independently
verified SHA-256:
`194dada8195a219c4cfa26994f26ab438c61e6c868227ace0fb3a8e07f1a5b1b`.
CRC and the exact member/size/type allowlist passed: one regular `plan.json`,
no duplicates, traversal, encrypted members or symlinks.

Original plan SHA-256:
`8754236d9c9d97c19234afc62a7d18d19bf28bab11dccd74879a811daa8017e7`
(678,054 bytes). It is copied **byte for byte**, without normalization,
reserialization or regeneration, to
`config/acs_backfill_reviews/plan-acs5-b2-transport-recovery.json`.
Existing identity/schema validators confirm exact plan ID:
`acs_historical_execution_plan_v1__e66ca5e3b72241d7347da8313421620846d597a0d9f0f3262eda4f8190bf1d9a`.

## Independent comparison and governance checks

Exact source `census_acs5`, vintages `[2018,2019,2020,2021,2022]`, batch size 5.
The previous adopted B2 plan remains untouched, with original SHA-256
`09ed5e185317386ae3c0bfe53116a6f9300c9323fde20e42d38707a09ceb46e1`.
A recursive comparison found **only** these four changed semantic paths:

- `/plan_id`
- `/code_commit`
- `/authority/commit`
- `/repository_identity/code_sha256`

Both commit fields equal `6749060f32903901718614a8d70ae73f98c5723c`, independently
confirmed as merged PR #291. Code fingerprint is
`a17c199f37f8df8646d270c16b47225652a3e58fbbcf49d1cf10f4b21948ffaa`,
matching inspected main. Baseline ancestry and repository/configuration bindings
pass existing identity checks. Configuration hashes and the entire serialization
runtime object, including executable and installed RECORD hashes, are unchanged
from the previous plan. Local runtime differs and was not substituted; future
hosted authorization/execution must still match the immutable runtime exactly.

All five provider-evidence objects are identical to the previous plan and adopted
B2 evidence map (including 2021's successful retry), and pass `validate_evidence`.
Each retains its exact snapshot hash, 223-member accounting, ten configured
exclusions, both variables, 446 available observations and zero omissions.
Total available observations: 2,230. No evidence was regenerated or modified.

Each vintage remains `missing_acquisition`, with no competing parents, approved
gaps or verification requirements. Current main's scoped catalog agrees; no pin
or cycle result exists for the new plan's cycles. The full catalog and all accepted
August pointers equal the previous B2 preflight authority snapshot. Historical
governance remains `absent`; adoption grants no scope/gap or materialization approval.

Full hashes, five snapshot identities, runtime fingerprints and comparison results
are in `docs/audit/acs5_b2_recovery_plan_verification.json`.

## Separate next authorization boundary

After external review and merge, and only after separate authorization approval:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=authorize \
  -f plan_file=config/acs_backfill_reviews/plan-acs5-b2-transport-recovery.json \
  -f reviewed_plan_id=acs_historical_execution_plan_v1__e66ca5e3b72241d7347da8313421620846d597a0d9f0f3262eda4f8190bf1d9a
```

This command is documented, not dispatched. Existing authority, inventory,
runtime, pin, snapshot and publication gates remain unchanged. The old plan and
authorization run `38956749622` do not authorize the transport-recovery code.
No physical acquisition/publication, serving promotion or governance action is
included in this plan-adoption change.

## Validation

335 relevant existing regression tests passed: historical backfill, provider
evidence, boundary hardening, historical materialization and physical integration.
Final original-member byte equality and staged diff checks passed. Changes are
limited to the exact new plan and these audit records; the previous plan, provider
evidence, dependencies, workflow definitions and acquisition implementation are
unchanged. No production workflow was dispatched.
