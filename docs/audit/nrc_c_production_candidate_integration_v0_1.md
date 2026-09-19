# NRC-C governed physical candidate integration audit v0.1

## Decision and boundary

NRC-C implements the independent `census_nrc` physical candidate lifecycle. It does not
change accepted pointers, the monthly Source Set, canonical-market or serving-market
promotion, and it creates no logical family resolver. Shared-cohort participation is
reserved for NRC-D. The manual workflow remains outside routine scheduling.

## Production implementation

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

A local live proof should use a fresh isolated workspace and cycle. The manual hosted
workflow is available only for subsequent durable GitHub proof; it is not the first proof
and is not scheduled. Record the live candidate row counts/ranges, both pin hashes, artifact
identity, resume/replay identity, and durable result before NRC-D consideration.

### Reproduction commands

Local first proof (requires Census network access):

```bash
PYTHONPATH=. python scripts/nrc_c_live_proof.py \
  --cycle-id monthly_cycle__nrc_c_local_live_YYYY_MM_DD \
  --workspace /tmp/nrc-c-live-proof
```

Inspect `/tmp/nrc-c-live-proof/summary.json`, the durable pin beneath `authority/`, and
both candidate directories. Then invoke `.github/workflows/nrc-monthly-source.yml` in
`normal`, `resume`, and `replay` modes with one fresh isolated cycle ID to prove GitHub
Release publication and durable cycle-result recording. All three runs must report the
same candidate artifact identity; only normal may contact Census.

The current Codex environment could not complete the live command because its outbound
proxy rejected the Census HTTPS tunnel with HTTP 403. No live claim is made here.
