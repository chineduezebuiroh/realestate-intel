# ADR-006: Review and Decision Platform Closeout

## Status

Accepted

## Context

The review runtime already provides typed bundles, tables, plots, manifests,
decisions, geography selections, artifact writing, and deterministic bundle
orchestration. Review campaigns compare named production or experimental roles;
the current manifest directly identifies the source and optional challenger run,
while campaign metadata can identify a distinct baseline or incumbent where a
campaign needs those roles.

The closeout needs an enforceable package boundary without designing the
unfinished 8b-B or 8b-C functionality or declaring new diagnostic outputs.

## Decision

- Continue to use `ReviewBundle`, `ReviewManifest`, `DecisionSummary`, and
  `ReviewGeographySelection` as the typed contracts.
- Keep orchestration deterministic: tables are written in bundle order and
  manifest outputs are discovered in sorted path order.
- Use the existing deterministic geography policy. Mandatory and targeted
  geographies are selected by policy; explicit manual overrides are recorded in
  the rationale. Context geographies remain separate.
- Represent baseline, incumbent, and challenger using existing run identifiers
  and manifest metadata. No second campaign schema is introduced.
- Keep production execution artifacts separate from review exports. A review
  package references source runs rather than modifying them.
- Export each review package as a directory rooted by `manifest.json`; optionally
  bundle the same files as a deterministic ZIP.
- Validate directory and ZIP packages with the executable runtime contract. The
  validator checks metadata, declared members, duplicate paths, ZIP integrity,
  and every emitted SHA-256 hash.
- Exclude generated review and execution artifacts through `.gitignore`; source,
  contracts, tests, and documentation remain tracked.

## Consequences

Packages are portable and can be checked by producers, consumers, and smoke
tests using one implementation. The manifest remains the authority for variable
package contents. A declared plot must exist before a package can pass, although
plots and decision summaries remain optional unless emitted or declared.

ZIP generation uses stable ordering, timestamps, permissions, and compression
settings. Generated outputs can be regenerated or retained outside Git.

## Rejected alternatives

- A second review metadata schema was rejected because it would diverge from
  `ReviewManifest`.
- A fixed list of future dashboard, scorecard, recommendation, chronology,
  comparison, or diagnostic files was rejected because the runtime does not
  universally produce them.
- Embedding review output into immutable production-run directories was rejected
  because it couples execution evidence to a later review lifecycle.
- Tracking generated packages in Git was rejected because hashes and external
  retention provide reproducibility without repository bloat.

This decision records existing Section 8b behavior plus package validation and
ZIP closeout only. It makes no claim that unfinished 8b-B or 8b-C features exist.
