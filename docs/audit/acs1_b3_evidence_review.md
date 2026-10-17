# ACS1 B3 provider evidence adoption review

Status: **PROPOSED FOR MAIN REVIEW**, `REVIEW_CANDIDATE_ONLY`.
Evidence adoption does not authorize acquisition, scope/gaps or serving changes.

Original hosted run: `39023640589`; artifact: `12097403133`
(`acs-provider-evidence`); capture/main commit:
`4ba7c1f689b6ec83217674e25810fe20ff71bca5`.
The original ZIP was transferred by user upload after proxy CONNECT 403 blocked
the connector download. Its 6,699,392 bytes independently hash to
`d747ec083b868279bad6fa3790a4d8ae7312bd154da2150daeb7981f20d07363`,
matching the hosted artifact metadata and supplied identity.

The existing offline batch verifier passed ZIP CRC and exact member/path/size,
duplicate, symlink and encryption checks. All 1,125 raw response bodies passed
hash, length, request identity and decoded-content checks and were replayed through
existing historical preflight, response interpretation and `validate_evidence`.
Exact ordinary ACS1 datasets/years, both variable metadata records, year isolation,
snapshots, omission receipts and current governed configuration passed.
Each of 2013–2017 accounts for 223 requested members and ten unchanged exclusions,
225 responses, 308 available and 138 provider-no-content metric observations,
and 69 explicit omission receipts. Totals: 1,540 available, 690 no-content and
345 omission receipts. No missing values were imputed or exclusions expanded.

All five annual files and the aggregate are byte-for-byte original captured
evidence, emitted unchanged by the verifier. The unchanged verifier report is
`acs1_b3_evidence_verification.json`, including all manifest hashes, capture IDs,
configuration bindings, snapshot hashes and omission receipt identities.

| Vintage | Annual evidence SHA-256 | Provider snapshot SHA-256 |
|---|---|---|
| 2013 | `006f2a11d1cdfaf3abad761951e78e258cf263482d96447a3bf1d0c6d2b81207` | `e619298edd36161a2d5108a2540a478c933454c15f2eda56b7130aa8532e7136` |
| 2014 | `74e23049d61465fe77e1781247036e2a23c843026ab82569e88e6e4cc46b0f77` | `84cc2ed66ebab5df28d22e980274dbee7ad560c5142bc98a8f3a3431b012e35e` |
| 2015 | `a89cb660e215b17dc1576d6179ce93b4457a151df86209744f2e824d6e57091d` | `4a0f0b9e0fd3b39fd377b3cb0cc3a719e33d91f8b9cf5562b0bb7c47297a1b7d` |
| 2016 | `f57735d9daa60fd74d89af321de5b23800c4ea9f4d8113438766bb99a64ab51a` | `f9d926c93c2dcc186fbf4b2a13c7b8e71c342e055fc40dc2881e471d36369fe1` |
| 2017 | `0f3e5445b10add0df3b837ba40fc132872c669558d63b6a66f709636abd8a488` | `d3612d6da43821f72dffc101f6db108f7bd1431a5d68952535e429c8aa29a0ad` |

Aggregate preflight input:
`config/acs_backfill_reviews/provider-acs1-2013-2014-2015-2016-2017.json`.
SHA-256: `ce60e51eecb91ae386e53f77e5dad575edff94d68812f97be8cda82e1ff98dd1`.

## Validation and next step

The relevant existing provider-evidence, backfill, boundary-hardening,
historical-materialization and physical-integration suites passed: **335 tests**. Final byte comparison and staged diff
checks also passed. Only six new review JSON files and these two audit records change;
source registry, geography, acquisition implementation, workflows, dependencies,
existing evidence and accepted August pointers are untouched.

After independent review and merge, the next read-only hosted preflight is:

```sh
gh workflow run acs-historical-backfill.yml \
  --repo chineduezebuiroh/realestate-intel --ref main \
  -f action=preflight -f source_id=census_acs1 \
  -f vintages=2013,2014,2015,2016,2017 -f batch_size=5 \
  -f evidence_file=config/acs_backfill_reviews/provider-acs1-2013-2014-2015-2016-2017.json
```

Expected artifact: `acs-backfill-preflight`, containing `plan.json`. Review fresh
inventory, classifications, verification requirements and immutable runtime/code
bindings before separate plan adoption, authorization and acquisition. No workflow
was dispatched and no production state was written during evidence adoption.
