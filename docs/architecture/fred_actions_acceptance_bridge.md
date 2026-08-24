# FRED Actions acceptance bridge

The FRED acceptance workflow temporarily uses GitHub Actions artifact storage to
carry the governed artifact between clean hosted runners. It selects the newest
successful prior run of the same workflow on `monthly-refresh-orchestration`,
requires exactly one non-expired `fred-governed-artifact-*` package, downloads
it with the workflow token, verifies the evidence-package structure, and runs
the governed source-artifact validator before reconciliation.

An explicitly supplied `prior_artifact_path` takes precedence and must itself
validate. If no qualifying prior successful run exists, resolution is a
bootstrap and no prior argument is supplied. Once a prior run is discovered,
missing, ambiguous, malformed, expired, or invalid evidence fails closed.

This is only an acceptance/evidence bridge with 30-day retention. It is not the
durable governed source-artifact registry or a production storage contract. The
production architecture must eventually resolve prior state from a durable,
immutable artifact registry.

## Phase 2B migration precedence

Phase 2B retains this bridge only for controlled migration. Resolution reads
`config/artifact_catalog.json` first. If `accepted.source.fred_macro` exists,
the exact record and numeric Release/asset must download, hash, extract, and
validate; failure is corruption and fails closed without trying Actions. Only
absence of the accepted pointer permits the Actions bridge. Retire the bridge
after first publication, explicit activation, and a second fresh-run durable
idempotency proof.
