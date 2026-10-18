# Coordinated B4–B7 immutable plan adoption

Decision: **PROPOSED FOR EXTERNAL REVIEW**, `REVIEW_CANDIDATE_ONLY`.
Four independent plans are adopted; no combined executable plan, authorization
record, dispatch, physical acquisition or production write is created.

## Original hosted provenance and integrity

All four hosted preflights completed successfully on main at
`8b59276965ff06c9853b2307a915b6ea746f1ed2`, using
`.github/workflows/acs-historical-backfill.yml`. Original uploaded ZIP hashes
match independently read GitHub artifact digests. ZIP CRC and exact one-member
`plan.json` path/size/type/duplicate/encryption/symlink allowlists passed.

| Batch | Run | Artifact | Original ZIP SHA-256 |
|---|---|---|---|
| B4 | 39126284037 | 12136585621 | `eec541314a067a6461a929b45d1f88aa1e6b5729208ea3feeca971d28feeee51` |
| B5 | 39126533579 | 12135869708 | `2a5e4df1a80869d815f9e838893f8f2553f0dd61001fcf6a01df37913464d3fe` |
| B6 | 39126647582 | 12136556362 | `7c2756b287cd28235d7aea66063cca18d8d683b3fb43f3e7e3d52b6cfc2a1fb9` |
| B7 | 39127085656 | 12136242783 | `f3b00a53fcfb4e0da618a83d1282763b121f6c7d21a9be8931eaeb6f62980b02` |

Original `plan.json` bytes were copied unchanged without regeneration,
normalization or reserialization. Existing `verify_plan` validates exact content
identities, schemas and each embedded historical evidence object.

| Batch | Adopted path under `config/acs_backfill_reviews/` | Bytes | Original plan SHA-256 |
|---|---|---:|---|
| B4 | `plan-acs5-b4-2013-2014-2015-2016-2017.json` | 677839 | `25c3c4b5dc4b99b19a80b184b052591df87e5e9ebbd4eef59cdbe4ea2ac4e03f` |
| B5 | `plan-acs1-b5-2008-2009-2010-2011-2012.json` | 885170 | `905b4bd02915003120954568af949582ed9375823cdcd61803b713e187d8a353` |
| B6 | `plan-acs5-b6-2009-2010-2011-2012.json` | 556025 | `366c892f83b6c9e3bff45a4fca958dcacc33aeb586d9fbf99648526cb52fe0e7` |
| B7 | `plan-acs1-b7-2005-2006-2007.json` | 542238 | `238f753cbf6858038d0322ea0fac11111ab45311ac0cfda088fc0875d3af7e29` |

## Four independent reviewed identities

- B4: `acs_historical_execution_plan_v1__76efc520b09873f0d76bb4f69f779c87a68da64f4939f7b3aba4b980308926e4`
- B5: `acs_historical_execution_plan_v1__08aea10545f809a91650d2ca89ca0ec33bcb2227b0a79faf04b395b25df35190`
- B6: `acs_historical_execution_plan_v1__da4aff269f0525e48f28de5a44f32fdff9e26689e664181ae30182e26f8ce129`
- B7: `acs_historical_execution_plan_v1__6b4e66c224bda7ebff2ba45972fc408c96c16f2e78206dbe2af0ab47f0603280`

## Governance, evidence and inventory verification

| Batch | Product | Vintages | Batch size | Available | No-content | Classification |
|---|---|---|---:|---:|---:|---|
| B4 | census_acs5 | 2013,2014,2015,2016,2017 | 5 | 2230 | 0 | All missing_acquisition |
| B5 | census_acs1 | 2008,2009,2010,2011,2012 | 5 | 1512 | 718 | All missing_acquisition |
| B6 | census_acs5 | 2009,2010,2011,2012 | 4 | 1752 | 32 | All missing_acquisition |
| B7 | census_acs1 | 2005,2006,2007 | 3 | 896 | 442 | All missing_acquisition |

All 17 entries have zero parents, no approved gaps and no unresolved verification
requirements. Independent reconciliation against current main’s catalog confirms
no matching physical acquisitions. Every annual evidence object exactly equals
the corresponding annual and aggregate evidence adopted through PR #296. B5’s
mixed-run origins remain in its unchanged adopted verification record; plan
adoption does not relabel captures or replace missing concept metadata.

Each plan exactly matches the current 223-member request plan and ten exclusions.
Every year validates both configured variables, exact source/year metadata,
446 metric classifications, snapshot hashes and all observed omission receipts.
No observations, omissions, geography membership or provider evidence change.

Code/authority commit, repository/baseline identity and all governed configuration
hashes match current main. Bound acquisition code SHA-256 remains
`bdba8743ee94ca703ebdd5f9e6b9f17b3816b4f3a4836a4db2527f38356a288b`.
Historical governance remains `absent`; no scope/gap or materialization approval
is implied. All four full serialization-runtime fingerprints are identical and
match the prior hosted recovery plan: Python 3.11.17 executable hash and
pandas 2.2.2 / NumPy 2.4.6 / PyArrow 26.0.0 installed RECORD hashes are retained
in the verification record. The local review runtime differs and is not
substituted. Future hosted authorization and execution must independently match
the exact bound fingerprints; moving runner/dependency availability is not assumed.

Current main’s entire accepted pointer map matches the accepted August baseline:
logical ACS `src__acs__2024-12__r1__366cf814efe424ef`, Source Set
`source_set__2026-08__v2__b429053765fb9fed`, canonical market
`market__2026-08__r2__7793c3e80612bf80`, serving market
`serving__market__2026-08__r2__7793c3e80612bf80__58610cddf4015d11`.
Catalog, source pins/results, releases and serving state are not written.

Detailed per-vintage evidence hashes, accounting, receipt counts, authority,
runtime and configuration fingerprints are in `acs_b4_b7_plan_verification.json`.

## Separate next B4 authorization boundary

After independent external review and merge of this plan-only PR, and only for
the separately approved authorization operation:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=authorize \
  -f plan_file=config/acs_backfill_reviews/plan-acs5-b4-2013-2014-2015-2016-2017.json \
  -f reviewed_plan_id=acs_historical_execution_plan_v1__76efc520b09873f0d76bb4f69f779c87a68da64f4939f7b3aba4b980308926e4
```

This command is documented, not dispatched. Review the resulting authorization
before a separate execute step. B5–B7 must each use its own exact adopted plan
and separate authorization; B4 approval cannot authorize any other batch.

## Regression results

Targeted historical backfill, provider evidence, authorization/execution boundaries,
provenance, historical materialization and physical integration: **365 passed**.
Full repository `python -m pytest -q tests`: **624 passed**, 138 warnings,
no failures. Final original-member byte equality, strict plan validation, exact
six-addition inventory and `git diff --check` passed. Remote main remained at
the inspected authority commit when rechecked before adoption.

## Exact changed-file inventory

```text
config/acs_backfill_reviews/plan-acs1-b5-2008-2009-2010-2011-2012.json
config/acs_backfill_reviews/plan-acs1-b7-2005-2006-2007.json
config/acs_backfill_reviews/plan-acs5-b4-2013-2014-2015-2016-2017.json
config/acs_backfill_reviews/plan-acs5-b6-2009-2010-2011-2012.json
docs/audit/acs_b4_b7_plan_review.md
docs/audit/acs_b4_b7_plan_verification.json
```
