# Generated Artifact Policy

## Tracked material

The repository tracks source code, tests, configuration, schemas, typed
contracts, ADRs and other documentation. Small hand-authored fixtures may be
tracked when they are justified, stable, and necessary for deterministic tests.

## Generated and ignored material

The following are generated outputs and must remain ignored: regime runs, review
exports, comparison outputs, generated CSV and Parquet files, plots, ZIP bundles,
temporary diagnostics, caches, and logs. `.gitignore` is the enforcement
mechanism; contributors must still inspect the staged file list before commits.

Generated outputs may be regenerated or retained in external artifact storage
when preservation is needed. Adding any generated artifact to Git requires
explicit justification and review rather than `git add -f` as a convenience.

Deleting a generated artifact from the current Git tree does not remove its bytes
from prior Git history. Large generated output therefore must never be committed
temporarily with a plan to delete it in a later commit. If sensitive or oversized
content is committed accidentally, repository-history remediation is a separate
maintainer operation.
