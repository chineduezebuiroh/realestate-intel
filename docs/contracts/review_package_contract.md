# Review Package Contract

## Scope and authority

This contract describes packages emitted by `ReviewArtifactWriter` and
`write_review_bundle`. `manifest.json` is the canonical inventory; the executable
rules live in `regime.review.package_contract`. The contract version is the
manifest's `schema_version`, not the documentation revision.

## Components

| Path or component | Status | Producer | Consumer | Validation rule |
| --- | --- | --- | --- | --- |
| `manifest.json` | Required | `ReviewManifest` through the orchestrator/writer | validators and downstream review tooling | Readable JSON object that constructs a valid `ReviewManifest`; required non-empty fields are `schema_version`, `campaign_id`, `run_id`, `created_at`, `framework_version`, and `source_run_id`. |
| `manifest.json.outputs[]` | Required inventory; may be empty | orchestrator and artifact writer | validators and package consumers | Every item has one unique, non-empty, relative canonical `path`; each path must exist in the package. |
| `tables/<name>.csv` or a typed table's configured subdirectory | Required when listed | `ReviewArtifactWriter.write_table` | reviewers and diagnostics | Member exists; when the manifest supplies `sha256`, its bytes match that digest. |
| `tables/selected_review_geographies.csv` | Optional; emitted when geography selection is supplied | orchestrator | reviewers | If emitted, it is listed and hash-validated like any table. |
| `tables/review_geography_rationale.csv` | Optional; emitted with geography selection | orchestrator | reviewers | If emitted, it is listed and hash-validated like any table. |
| `decision_summary.json` | Optional | `DecisionSummary` through the orchestrator | human or automated decision workflow | Must be present when produced; it is not currently included in `outputs` because the orchestrator writes the manifest first. |
| Plot paths | Optional; required when declared in `outputs` | an upstream diagnostic; orchestrator records the reference | reviewers | The referenced relative member must exist. A plot without a hash receives existence validation only. |

Campaign metadata uses the existing manifest fields and its extensible `metadata`
object. `campaign_id` is mandatory. `source_run_id` identifies the reviewed source,
`challenger_run_id` identifies a challenger when present, and campaigns may record
baseline/incumbent identifiers in `metadata`. No additional metadata schema is
imposed by package validation.

## ZIP behavior

`ReviewArtifactWriter.write_zip` archives all regular files below the package
root using relative POSIX member names, sorted member order, a fixed timestamp,
fixed file permissions, and deterministic compression settings. The ZIP has the
same required members as its directory. Validation rejects unreadable archives,
CRC failures, duplicate file members, missing manifest members, unsafe manifest
paths, invalid metadata, and hash mismatches.

## Manifest and hashes

The writer records `path`, `size_bytes`, and SHA-256 for discovered files before
writing the manifest. Package validation recomputes every hash that is present.
Entries such as externally created plots may currently contain only `path` and
`artifact_type`; those entries receive member-existence validation rather than an
invented digest requirement. The manifest itself does not hash itself.

## Versioning and extension

Backward-compatible additions belong in `metadata` or as additional manifest
outputs. A breaking change to required metadata or output semantics requires a
new `schema_version` and compatible validator handling. Future calibration
campaigns may add tables, plots, or metadata by declaring them in the existing
manifest. They must not silently make campaign-specific files globally required;
a broadly required component needs a contract/version change and runtime support.
