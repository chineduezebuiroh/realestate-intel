# LAUS annual-publication collector v0.1

**Status:** governed raw-capture boundary implemented; semantic completion parser
blocked. `READY_FOR_ANNUAL_DEEP` is not automatically trustworthy and LAUS-C2 is
not authorized.

## Reconnaissance performed

On 2026-08-31 the implementation environment attempted direct access to these
official candidates:

* `https://www.bls.gov/lau/notices/2025.htm`
* `https://www.bls.gov/lau/notices/2026.htm`
* `https://www.bls.gov/lau/notices/`
* `https://www.bls.gov/lau/`
* `https://www.bls.gov/schedule/news_release/laus.htm`
* `https://www.bls.gov/feed/laus.rss`
* `https://download.bls.gov/pub/time.series/la/`

The web facility returned HTTP 401 before retrieving provider content. Direct
HTTPS attempts were rejected at the environment proxy CONNECT boundary with 403.
Therefore no BLS response bytes, final provider URL, content type, redirect chain,
document structure, annual-year expression, or class-completion wording was
observed. Search snippets were not used.

The candidates have plausible roles but are **not selected production endpoints**:
the LAUS notices collection may document revisions, the LAUS landing page may
announce important information, the release schedule/RSS may establish expectation,
and the time-series directory publishes data. Without provider bytes, none was
proven to establish class-specific annual completion. In particular, schedule,
RSS, and data availability cannot be promoted into completion evidence by
assumption.

## Stop-condition decision

No exact official historical bytes existed in the repository, and none could be
retrieved. Inventing HTML fixtures would invent both provider structure and
completion semantics. The task explicitly forbids weakening READY, so C1b stops
before a semantic parser. It adds no 2025/2026 “official archived fixture” and
does not relabel synthetic test bytes as a provider capture.

Consequently the requested historical state-first/substate-later results cannot
be honestly proven by C1b. C1 Smoke 187 continues to prove the state-machine
contract using normalized policy fixtures, but those fixtures are not evidence
that an official page can produce them.

## Implemented raw-capture boundary

`jobs/monthly_refresh/laus_annual_publication.py` implements the portion justified
without semantic assumptions:

* HTTPS-only `bls.gov`/`www.bls.gov` source and redirect-target allowlisting;
* redirects disabled in the HTTP client and followed explicitly for validation;
* explicit user agent, timeout, three bounded attempts, and five redirects;
* retry of connection/timeout, 408, 429, and 5xx failures;
* terminal treatment of unexpected 4xx and malformed/excess redirects;
* SHA-256 over exact response bytes without HTML normalization;
* immutable content-addressed storage under
  `artifacts/laus_annual_processing/raw/<sha>.bin`;
* receipt storage under
  `artifacts/laus_annual_processing/receipts/<sha>__<receipt-id>.json` with source URL, final
  URL, status, content type, redirect chain, byte count, SHA, user agent, and
  retrieval timestamp;
* offline `inspect` verification of raw bytes against receipt and provenance.

Retrieval time is receipt-only. Content SHA names the evidence and changed bytes
create a different immutable raw object. Same bytes are idempotent; a conflicting
receipt fails. Because semantics cannot yet be parsed, changed-byte semantic
comparison, provider contradiction detection, provider-release identity, annual
year, and C1 evaluation deliberately remain unavailable.

The `normalized_evidence_from_capture` boundary raises
`PublicationParserContractUnavailable`; it never returns NOT_EXPECTED or READY.
The collector summary explicitly reports
`parser_contract_status=blocked_unreviewed_provider_structure`, null normalized
evidence, `detector_evaluated=false`, and `annual_deep_authorized=false`.

## Fixture labels

Smoke 188 uses **synthetic transport fixtures only**. They prove trusted/untrusted
provenance, trusted/untrusted redirects, retry/terminal behavior, exact hashing,
archive idempotency, archive verification, and fail-closed absence of a parser.
They contain no BLS prose and are not official archived fixtures or live captures.

There is no 2025 fixture result, 2026 fixture result, live normalized evidence,
provider release ID, or detector result in this change. Claiming any of those
would violate the raw-byte authority invariant.

## Operator-local capture command

From a network environment able to reach BLS, capture each candidate separately:

```bash
PYTHONPATH=. python -m jobs.monthly_refresh.laus_annual_publication collect \
  --url https://www.bls.gov/lau/notices/ \
  --output-root artifacts/laus_annual_processing \
  --output artifacts/laus_annual_processing/notices_collection_summary.json
```

Repeat for the other `www.bls.gov` candidate URLs above. Preserve the entire runtime evidence
directory outside Git and provide the raw/receipt pairs for governed review. No
`BLS_API_KEY` is required and this command does not invoke the observations API.

## Required continuation before C2

Review actual captures to select stable endpoint(s) and establish, separately for
`model_based_state` and `substate_nonmodeled`, the structural cues, authoritative
year derivation, completion/underway language, publication metadata, and drift
rules. Then add minimized, provenance-labelled official fixtures and a parser
whose release identity binds source, year, class, status, and raw SHA. Tests must
cover harmless prose variation, structural/semantic drift, revised bytes,
contradictions, 2025 staging, and 2026 delay before the parser may feed C1.

Until that reviewed continuation passes, automatic READY is not trustworthy and
LAUS-C2 is unsafe.
