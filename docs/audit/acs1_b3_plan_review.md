# ACS1 B3 immutable execution plan adoption

Decision: **PROPOSED FOR EXTERNAL REVIEW**, `REVIEW_CANDIDATE_ONLY`.
This adoption does not authorize or execute acquisition.

Successful hosted preflight run `39026170162`, artifact `12098612587`
(`acs-backfill-preflight`), main/code authority
`e94ccf7ddd444453a63c5aa6da576a22a0aa02b5`.
The original uploaded ZIP (84,161 bytes) independently hashes to
`de8c6e7c39b8fc8975541e5c61eb6c2d44cdcc685c66c818170aef0f0be44db6`.
ZIP CRC and exact single-member, size, duplicate/path, encryption and symlink
checks passed. The original `plan.json` is 876,713 bytes, SHA-256
`94ee58e03873cf0ecc2183333b25657648596efac2d0572edef72b2f5244015d`.
It is adopted byte for byte, without regeneration or reserialization, at
`config/acs_backfill_reviews/plan-acs1-b3-2013-2014-2015-2016-2017.json`.

Existing identity/schema and provider-evidence validation passed for exact ID:
`acs_historical_execution_plan_v1__4a27745b32f45035cbff5e0278b66f64359dd67afeb95dfa76b89d3573f59dbc`.
Source is `census_acs1`, vintages 2013–2017, batch size five. Every entry is
`missing_acquisition` with zero competing parents, no approved gaps and no
unresolved verification requirements. Main catalog has no matching physical
parents for these five product/vintages.

All five embedded provider-evidence objects exactly equal the adopted B3 map.
Each year preserves its snapshot identity, 223 governed members, ten exclusions,
308 available metric observations, 138 no-content observations and 69 omission
receipts. Geography and configuration hashes match current main. No provider
evidence or omission interpretation is changed.

Bound acquisition code SHA-256 remains
`a17c199f37f8df8646d270c16b47225652a3e58fbbcf49d1cf10f4b21948ffaa`.
Repository/baseline identity, code and all configuration hashes match current main.
Historical governance remains `absent`; this grants no scope/gap or materialization
approval. The full immutable serialization-runtime object matches the previous
hosted B2 transport-recovery plan, including Python executable and installed
pandas/NumPy/PyArrow RECORD hashes. The local review runtime differs and was
not substituted. Future hosted authorization/execution must independently match
the exact bound runtime; moving runner/dependency availability is not guaranteed.

Main catalog's entire accepted pointer map matches the prior accepted August
baseline: logical ACS `src__acs__2024-12__r1__366cf814efe424ef`, Source Set
`source_set__2026-08__v2__b429053765fb9fed`, canonical market
`market__2026-08__r2__7793c3e80612bf80`, serving market
`serving__market__2026-08__r2__7793c3e80612bf80__58610cddf4015d11`.
Detailed hashes, runtime, evidence accounting and checks are preserved in
`acs1_b3_plan_verification.json`.

## Validation and next boundary

All **335 existing regression tests passed** across backfill, provider evidence,
boundary hardening, historical materialization and physical integration. Original
member byte equality and final staged diff checks passed. Changes
are limited to this original plan and two audit files. Provider evidence,
acquisition code, governance, configuration, dependencies, workflows and
accepted pointers are unchanged.

After external review and merge, the next separately approved operation is:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=authorize \
  -f plan_file=config/acs_backfill_reviews/plan-acs1-b3-2013-2014-2015-2016-2017.json \
  -f reviewed_plan_id=acs_historical_execution_plan_v1__4a27745b32f45035cbff5e0278b66f64359dd67afeb95dfa76b89d3573f59dbc
```

This command was not dispatched. Authorization must recheck authority, scoped
inventory and bound runtime. Production acquisition requires its own subsequent
execute operation bound to the same plan and successful authorization run.
