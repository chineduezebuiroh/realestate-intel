# Coordinated B4–B7 provider evidence adoption

Status: **PROPOSED FOR MAIN REVIEW**, `REVIEW_CANDIDATE_ONLY`.
Evidence adoption grants no acquisition, scope/gap, materialization or serving authority.

## Independent archive verification

All five original uploaded ZIPs passed the existing offline batch verifier: exact
SHA-256, ZIP CRC/member/path/size/duplicate/symlink/encryption checks; raw response
hashes/lengths and decoded identities; exact source/year metadata; snapshot hashes,
omission receipts and current governed configuration. No provider was contacted.

| Capture | Run | Artifact | Capture commit | Original ZIP SHA-256 |
|---|---|---|---|---|
| b4 | 39118371228 | 12133407540 | `31a3c5210f25d94e9d526d7a01fd5216d4c9d68f` | `065ad944953904e37c946c2fab421d898bd1c58186a09ce72b2f0e0d68552f87` |
| b5_original | 39119739109 | 12132894861 | `31a3c5210f25d94e9d526d7a01fd5216d4c9d68f` | `0a6f2e5424e72866b2341e051caa361fbd26833f32d8392de52b523b35f486c4` |
| b5_recovery | 39122524334 | 12135062001 | `4887a91d6a82428d14862089098286db5b70df52` | `706e9b66c8c5e9e5a2723397f25f66b7fe0a3da11db66e88a43623695cbfabb5` |
| b6 | 39123671869 | 12135336250 | `4887a91d6a82428d14862089098286db5b70df52` | `e5a0169f5a50bfc7d534a7ba0f14ce7c06f83abb16795930624f6a657ba15094` |
| b7 | 39124183021 | 12135144523 | `4887a91d6a82428d14862089098286db5b70df52` | `90fc7613c9749b1acf5f6c2dd7e412c441ed45534942ad9215c90bcd24610ca5` |

The original archives and response manifests remain unchanged. Verification
records bind every original manifest by hash and retain each capture, evidence,
snapshot and omission receipt identity. Raw responses/manifests remain in their
original immutable hosted artifacts and local transferred ZIPs; they are not
duplicated as production inputs in this PR.

## Complete 17-vintage inventory

Each adopted year has 225 verified responses, 223 requested members and ten
unchanged exclusions; both configured metrics have explicit classifications.

| Batch | Product | Vintage | Available metrics | Provider no-content metrics | Omission receipts |
|---|---|---|---:|---:|---:|
| B4 | census_acs5 | 2013 | 446 | 0 | 0 |
| B4 | census_acs5 | 2014 | 446 | 0 | 0 |
| B4 | census_acs5 | 2015 | 446 | 0 | 0 |
| B4 | census_acs5 | 2016 | 446 | 0 | 0 |
| B4 | census_acs5 | 2017 | 446 | 0 | 0 |
| B5 | census_acs1 | 2008 | 300 | 146 | 73 |
| B5 | census_acs1 | 2009 | 300 | 146 | 73 |
| B5 | census_acs1 | 2010 | 304 | 142 | 71 |
| B5 | census_acs1 | 2011 | 304 | 142 | 71 |
| B5 | census_acs1 | 2012 | 304 | 142 | 71 |
| B6 | census_acs5 | 2009 | 438 | 8 | 4 |
| B6 | census_acs5 | 2010 | 438 | 8 | 4 |
| B6 | census_acs5 | 2011 | 438 | 8 | 4 |
| B6 | census_acs5 | 2012 | 438 | 8 | 4 |
| B7 | census_acs1 | 2005 | 296 | 150 | 75 |
| B7 | census_acs1 | 2006 | 300 | 146 | 73 |
| B7 | census_acs1 | 2007 | 300 | 146 | 73 |

Totals: 3,825 successful raw responses replayed, 6,390 available metric
observations, 1,192 provider-no-content observations and 596 omission receipts.
The four metadata responses from the two failed original B5 years were separately
hash/decoded-identity verified for audit only. Every observed omission remains
unchanged; no value, Census concept, geography or exclusion was synthesized.

## B5 partial capture and mixed-code provenance

Original B5 remains `capture_validated=false`, with failed 2010 and 2011 excluded.
Only its unchanged 2008, 2009 and 2012 annual evidence is adopted. Recovery run
39122524334 supplies unchanged 2010 and 2011 annual evidence. Missing concept
properties remain absent under the merged PR #295 compatibility contract.

The existing `combine_reviewed_evidence` validates exact five-year coverage and
rejects duplicate/overlapping successful years. It combines separately verified
year maps with matching governed configuration; it does not require a shared
capture commit. Each origin retains its original run/artifact/ZIP hash/capture
commit/batch ID/capture ID/manifest/evidence/snapshot/receipt identities. The new
combined B5 JSON is explicitly a serialized review map, not an original single-run
aggregate. No common run or capture commit is assigned to B5. Its batch-specific
verification record contains both original verifier reports and combined provenance.

## Review files and aggregate hashes

The 17 annual files are byte-for-byte original successful hosted JSON members.
B4/B6/B7 aggregate files also preserve original hosted member bytes. Only B5’s
combined aggregate is newly serialized by the existing combiner.

| Preflight input under `config/acs_backfill_reviews/` | SHA-256 |
|---|---|
| `provider-acs1-2005-2006-2007.json` | `e552c14d1032fa756682b4c6b17bdb6247769634bee168671de2b0a1262b7ca0` |
| `provider-acs1-2008-2009-2010-2011-2012.json` | `a31a33408e84ab0a03df8d3ae07333fe09960ac572ab93b0ac4bf0b5e62cf5b0` |
| `provider-acs5-2009-2010-2011-2012.json` | `164b26c1b9a4be8565324a4aa2a1618e8f89d0951893f6d66002199fa284fc0f` |
| `provider-acs5-2013-2014-2015-2016-2017.json` | `800e704d8d402e4071d51d7464900c44c8bf142abc938de9c5d78f5f887d5df7` |

Four batch-specific verification records:
`docs/audit/acs_b4_evidence_verification.json`,
`docs/audit/acs_b5_evidence_verification.json`,
`docs/audit/acs_b6_evidence_verification.json`,
`docs/audit/acs_b7_evidence_verification.json`.

## Validation and independent next steps

Baseline main: `4887a91d6a82428d14862089098286db5b70df52` (merged PR #295).
Acquisition code SHA-256 remains
`bdba8743ee94ca703ebdd5f9e6b9f17b3816b4f3a4836a4db2527f38356a288b`.
No acquisition code, workflows, dependencies, configuration, existing evidence,
catalog, pins, cycle results or production artifacts are changed. The entire
accepted August pointer map equals the prior accepted baseline; no serving change
is performed. No production writer or workflow is invoked.

After external evidence review and merge, run independent read-only preflights
in B4 → B5 → B6 → B7 order; wait for each before dispatching the next. Retain
each original plan artifact independently. No plan generation is performed here.

B4:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=preflight -f source_id=census_acs5 \
  -f vintages=2013,2014,2015,2016,2017 -f batch_size=5 \
  -f evidence_file=config/acs_backfill_reviews/provider-acs5-2013-2014-2015-2016-2017.json
```

B5:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=preflight -f source_id=census_acs1 \
  -f vintages=2008,2009,2010,2011,2012 -f batch_size=5 \
  -f evidence_file=config/acs_backfill_reviews/provider-acs1-2008-2009-2010-2011-2012.json
```

B6:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=preflight -f source_id=census_acs5 \
  -f vintages=2009,2010,2011,2012 -f batch_size=4 \
  -f evidence_file=config/acs_backfill_reviews/provider-acs5-2009-2010-2011-2012.json
```

B7:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=preflight -f source_id=census_acs1 \
  -f vintages=2005,2006,2007 -f batch_size=3 \
  -f evidence_file=config/acs_backfill_reviews/provider-acs1-2005-2006-2007.json
```

Subsequent plans must bind the current post-PR #295 acquisition implementation,
actual hosted serialization runtime and fresh scoped inventory. Plan adoption,
authorization and execution remain separate per-batch boundaries.

## Test results

Targeted evidence/backfill/boundary/materialization/physical-integration suites:
**365 passed**. Full repository `python -m pytest -q tests`: **624 passed**,
138 warnings, no failures. Final original-member byte comparisons, B5 object
comparisons, exact 26-addition inventory and `git diff --check` passed.

## Exact changed-file inventory

```text
config/acs_backfill_reviews/provider-acs1-2005-2006-2007.json
config/acs_backfill_reviews/provider-acs1-2005.json
config/acs_backfill_reviews/provider-acs1-2006.json
config/acs_backfill_reviews/provider-acs1-2007.json
config/acs_backfill_reviews/provider-acs1-2008-2009-2010-2011-2012.json
config/acs_backfill_reviews/provider-acs1-2008.json
config/acs_backfill_reviews/provider-acs1-2009.json
config/acs_backfill_reviews/provider-acs1-2010.json
config/acs_backfill_reviews/provider-acs1-2011.json
config/acs_backfill_reviews/provider-acs1-2012.json
config/acs_backfill_reviews/provider-acs5-2009-2010-2011-2012.json
config/acs_backfill_reviews/provider-acs5-2009.json
config/acs_backfill_reviews/provider-acs5-2010.json
config/acs_backfill_reviews/provider-acs5-2011.json
config/acs_backfill_reviews/provider-acs5-2012.json
config/acs_backfill_reviews/provider-acs5-2013-2014-2015-2016-2017.json
config/acs_backfill_reviews/provider-acs5-2013.json
config/acs_backfill_reviews/provider-acs5-2014.json
config/acs_backfill_reviews/provider-acs5-2015.json
config/acs_backfill_reviews/provider-acs5-2016.json
config/acs_backfill_reviews/provider-acs5-2017.json
docs/audit/acs_b4_b7_evidence_review.md
docs/audit/acs_b4_evidence_verification.json
docs/audit/acs_b5_evidence_verification.json
docs/audit/acs_b6_evidence_verification.json
docs/audit/acs_b7_evidence_verification.json
```
