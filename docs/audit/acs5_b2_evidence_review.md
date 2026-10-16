# ACS5 B2 verified provider evidence adoption

Status: **PROPOSED FOR MAIN REVIEW**, `REVIEW_CANDIDATE_ONLY`.
No acquisition, scope/gap approval, workflow dispatch or production write.

Both original uploaded ZIPs were independently hashed and passed ZIP CRC,
member/size/path/symlink/encryption allowlists. Capture commit for both:
`ac591182c9b03ccb453315fe328fba32b86bfe8b`.

| Original run | Artifact | ZIP SHA-256 | Accepted vintages |
|---|---|---|---|
| 38932974550 | 12059648541 | dd627948708dc38ba74300139bb9ba21c0ed0d20eb7690d97f5d807a3025a6d3 | 2018,2019,2020,2022 |
| 38934844908 | 12061240264 | c803250bc6a7365ce54558877b918f742b0f73f651f557c6bd73c5db1c90ad01 | 2021 |

The original batch remains explicitly **failed** for 2021. Its 177 captured raw
bodies, manifest and decoded identities were checked for audit integrity only;
no original 2021 evidence, partial observations or synthetic omissions are adopted.
The original attempted 1,078 HTTP requests, capturing 1,077 responses; its final
uncaptured request remains a failure. Only the complete retry supplies 2021.

All 1,125 successful raw responses across the two runs were independently replayed
through existing `historical.preflight`, `checked_response`, and `validate_evidence`.
Exact ordinary ACS5 dataset/year, B01003_001E/B19013_001E metadata, response
lengths/hashes/request identities, snapshots, year isolation, accounting and current
configuration match. Each year has 223 requested members and ten unchanged
exclusions; all 446 metric observations are available, with zero omission receipts
for each successful year. Across five vintages: 2,230 available metric observations.
No exclusions were expanded and no observations were reacquired or reconstructed.

## Review files and combined preflight input

Five `config/acs_backfill_reviews/provider-acs5-<year>.json` files for
2018,2019,2020,2021,2022 preserve each successful original per-year file **byte for
byte**. `docs/audit/acs5_b2_evidence_verification.json` records original archive,
batch, capture and response-manifest identities, individual file and snapshot
hashes, failed-capture audit status, and each accepted year's source run.

`config/acs_backfill_reviews/provider-acs5-2018-2019-2020-2021-2022.json` is a
new serialized review map containing precisely those five unchanged year objects.
Its SHA-256 is
`c10be7b42488f7c39a6d775d0c1207664c892aa0fd92745bd56a4161d42a83b0`.
It is **not** represented as an original single-run aggregate. Per-year provenance
remains in the audit record. Existing preflight consumes a year-keyed evidence map;
`make_plan` validates each product/year independently and imposes no same-capture-run
requirement. The combined map therefore uses the existing contract without an
acquisition-code change. Overlapping years are rejected rather than automatically
replaced; the failed 2021 entry is excluded before merging.

The narrow offline-review extension adds explicit `failed_vintages` selection to
`verify_batch_archive`. Default behavior still rejects partial batches. Opt-in
checks exact capture failure inventory, bounded failed members and raw integrity,
preserves the original false capture validation, and emits only successfully
replayed year candidates. `combine_reviewed_evidence` rejects duplicate, missing,
unexpected, wrong-product or configuration-drifted candidates and preserves each
origin. This changes scripts/tests only; acquisition code, workflows, governed
configuration, dependencies and historical materialization are untouched.

For independent offline repetition, use the existing batch-review CLI with exact
original source/run/artifact/hash/capture commit and all five requested years,
adding `--failed-vintages 2021` only for the original. Verify the retry with
`--vintages 2021` and no failed-year option. Combine only returned successful
candidates using `combine_reviewed_evidence`; never edit a failed capture summary
or rename the retry to impersonate its original run.

## Exact future read-only preflight

After separate external review and merge of this PR:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=preflight -f source_id=census_acs5 \
  -f vintages=2018,2019,2020,2021,2022 -f batch_size=5 \
  -f evidence_file=config/acs_backfill_reviews/provider-acs5-2018-2019-2020-2021-2022.json
```

Expected output: `acs-backfill-preflight` / `plan.json`. This command was not
run. Review exact new plan identity, fresh scoped reconciliation, all verification
requirements and runtime bindings. Plan adoption, authorization and execution
remain separate boundaries. No 2024 selection, historical bootstrap or serving
change is implied by evidence adoption.

## Regression verification

297 relevant offline tests passed across provider evidence, historical backfill,
boundary hardening, historical materialization and physical integration. Three
focused adoption checks also passed after strengthening separate-run provenance
and testing the combined map through existing `make_plan`. The tests prohibit
network access. Bound acquisition code SHA remains
`6b9d56daab7f272cab0f72887bb046a7e80ba51d8bb392b4127081e72d78aae9`.
All candidate file hashes and combined-origin identities were checked again
before commit; diff and Python compilation checks passed.
