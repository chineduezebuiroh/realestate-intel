# NRC-C governed physical candidate integration audit v0.1

**Status:** NRC-C — **COMPLETE / LIVE-PROVEN**

## Decision and boundary

NRC-C implements the independent `census_nrc` physical candidate lifecycle. It does not
change accepted pointers, the monthly Source Set, canonical-market or serving-market
promotion, and it creates no logical family resolver. Shared-cohort participation is
reserved for NRC-D. The manual workflow remains outside routine scheduling.

## Production implementation

The implementation merged in PR #250 at authoritative merge commit `be2aff49`
(`21f3eb5c` implementation, followed by whitespace-only commit `d444bd00`).

`jobs/monthly_refresh/nrc_monthly.py` owns first-party Census acquisition, exact-byte
pin construction/recovery, and candidate construction. `jobs/monthly_refresh/nrc_hosted.py`
composes those operations with the common durable pin, GitHub Release publication, and
cycle-result stores. `sources/census_nrc/parser.py` is the single production-safe parser;
the NRC-B verifier imports it so verification and production cannot drift.

## Exact input and identity contract

One complete input pin has exactly two members, `starts` and `completions`. Each member
records its authoritative URL, retrieval provenance, exact byte length, SHA-256, and
base64-encoded workbook bytes. The embedding is deliberate: the Census URLs are mutable,
so a URL/hash-only record could not satisfy replay. Recovery decodes and verifies both
length and hash before parsing. The provider release identity is a digest of the ordered
member names and hashes; it does not invent a Census release identifier.

Candidate execution parses both complete workbooks and publishes all available governed
history. `(NA)` is omitted as provider unavailable; no periods or values are synthesized.
The source request identity is the ordered two-workbook content digest. Candidate identity
is consequently stable for identical normalized inputs and excludes retrieval time.
Lineage inventories both member URL/hash/length identities. Historical changes are accepted
as revisionary current provider truth and are not compared against the legacy FRED database.

## Parser and dependency reproducibility

The parser retains NRC-B's openpyxl workbook/sheet, title, SAAR, native-thousands, typed-date,
Total-column, contiguous-block, and fail-closed footer contracts. Only exact `(NA)` (and an
empty workbook cell) means unavailable; arbitrary parenthesized tokens fail. Values use a
scale factor of 1. Because the repository does not have a general resolved-runtime dependency
identity facility, `requirements.txt` pins the live-proven `openpyxl==3.1.5`. The parser
contract identifier names that version and the parser file hash participates in artifact
identity.

## Invocation semantics

* **Normal:** downloads and validates both workbooks, durably writes the complete pin, then
  executes and publishes the candidate and durable result.
* **Resume:** consumes an existing pin and reconstructs both workbooks from embedded bytes;
  discovery is not called.
* **Replay:** follows the same pinned-byte-only reconstruction and cannot contact Census.

## Evidence

Deterministic tests cover canonical identities and native scale, month-end normalization,
exact unavailable handling, governed geography mapping, both-member completeness and
lineage, byte recovery/hash rejection, content-addressed stability, revision acceptance,
and normal/resume/replay ordering. NRC-B parser tests remain the parser regression suite.

The final local deterministic/regression gate passed 60 tests across
`tests/test_nrc_b_verify.py`, `tests/test_nrc_c_physical_integration.py`,
`tests/test_bea_c_physical_integration.py`, `tests/test_acs_c_physical_integration.py`,
`tests/test_bea_d_cohort_integration.py`, and `tests/test_acs_g_cohort_integration.py`.
The BPS production integration smoke passed, the monthly production contract smoke passed
172 tests, `compileall` passed, and `git diff --check` passed after the whitespace-only fix.

### Local real-Census proof

The successful local provider proof used cycle
`monthly_cycle__nrc_c_local_live_2026_09_19`. It produced:

* candidate artifact `src__census_nrc__2026-08__r1__94a8e9dc77b9063b`;
* pin `source_input__census_nrc__5bfd0c848836c4fb484f`;
* provider release
  `nrc-workbooks:84f66e94466844bc93d40ffee06571e2026f09e9bbea9250eee19f89dfbb8676`;
* 7,052 rows spanning `1959-01-31` through `2026-08-31`; and
* a replay artifact ID identical to normal, with `replay_identity_match: true`.

The exact workbook members were:

| Member | Authoritative URL | Bytes | SHA-256 |
|---|---|---:|---|
| `starts` | `https://www.census.gov/construction/nrc/xls/starts_cust.xlsx` | 193,398 | `02c1926ba520e5ab4f7eadf93a10293fe8c9b95f4343ee9285856321fc2b6da4` |
| `completions` | `https://www.census.gov/construction/nrc/xls/comps_cust.xlsx` | 171,446 | `62ce743e1714eacecbf8592d43a3128b101eb4aa366c419a3f59d54f9fb83986` |

The candidate contains 10 governed series. Starts includes nation plus four regions, each
with 812 observations from `1959-01-31` through `2026-08-31`. Completions includes 704
nation observations from `1968-01-31` through `2026-08-31`, and 572 observations for each
of four regions from `1979-01-31` through `2026-08-31`.

### Durable hosted-path proof

Because the workflow did not yet exist on the default branch before merge, the hosted-path
proof was executed locally against the real GitHub durable backend. The proof used cycle
`monthly_cycle__nrc_c_hosted_live_2026_09_19`.

The normal result succeeded with validation `passed` and publication state
`published_verified`. It recorded candidate artifact
`src__census_nrc__2026-08__r1__94a8e9dc77b9063b`, artifact content hash
`94a8e9dc77b9063befc592de1f64678ac143bb4609307861ab075c37ff815982`, package SHA-256
`4ece305a13c801a9741df9262e64a0b8e5d333e83c55299cbbf403f7ee961780`, and provider
release `nrc-workbooks:84f66e94466844bc93d40ffee06571e2026f09e9bbea9250eee19f89dfbb8676`.
It reported `source_change_detected: true`, `prior_artifact_id: null`, and
`accepted_pointer_changed: false`.

Resume and replay each succeeded from a fresh local workspace against durable GitHub
state. Both reproduced the normal result's candidate artifact ID, artifact content hash,
package SHA-256, and provider release ID; both passed validation, reached
`published_verified`, and reported `accepted_pointer_changed: false`.

Normal, resume, and replay therefore reproduced one deterministic governed candidate
identity, while accepted state did not move. NRC remains one physical governed source,
`census_nrc`; no logical NRC family resolver was introduced. No Source Set,
canonical-market, serving-market, or accepted-source promotion occurred. NRC-D remains
the shared cohort integration phase.

During workbook loading, openpyxl emitted `Cannot parse header or footer so it will be
ignored`. This is known benign workbook presentation metadata and is intentionally not
suppressed. It did not affect governed worksheet observations, row counts, ranges, parity
evidence, or deterministic artifact identity.

A future repeat of the local live proof should use a fresh isolated workspace and cycle.
The manual hosted workflow remains outside routine scheduling.

### Reproduction commands

Local first proof (requires Census network access):

```bash
PYTHONPATH=. python scripts/nrc_c_live_proof.py \
  --cycle-id monthly_cycle__nrc_c_local_live_YYYY_MM_DD \
  --workspace /tmp/nrc-c-live-proof
```

Inspect `/tmp/nrc-c-live-proof/summary.json`, the durable pin beneath `authority/`, and
both candidate directories. Invoke `.github/workflows/nrc-monthly-source.yml` in `normal`,
`resume`, and `replay` modes with one fresh isolated cycle ID for a repeat durable proof.
All three modes must report the same candidate artifact identity; only normal may contact
Census.

The earlier Codex-environment HTTP 403 proxy block applied only before the later local
execution. It is superseded by the successful real-provider and durable-backend proofs
recorded above.
