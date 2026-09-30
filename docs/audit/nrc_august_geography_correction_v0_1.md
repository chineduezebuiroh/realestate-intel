# August NRC governed-geography correction v0.1

## Corrected root cause

The published August NRC r1
`src__census_nrc__2026-08__r1__e02eda146c92a3d3` is immutable historical
evidence, but it is not eligible for canonical assembly.  The migration made two
distinct mistakes: it treated legacy `us_*` identities as canonical, and it
promoted the Census provider's complete five-geography shape into canonical
candidate applicability.

`config/geo_scope_macro.csv` is derived from the project's focus-state/Redfin
hierarchy.  Its governed states are CA, DC, MD, NJ, and VA, whose parent Census
regions are Northeast, South, and West.  Midwest is therefore correctly absent
from `config/geo_manifest.generated.csv`; this correction does not expand the
governed geography universe.

## Provider and candidate boundaries

The provider contract remains exact and fail closed.  Both Census workbooks must
publish `Total` for United States, Northeast, Midwest, South, and West.  All five
are normalized behind the NRC adapter:

| Provider | Normalized identity | Candidate disposition |
|---|---|---|
| United States / `US` | `united_states__nation` | included |
| Northeast / `NE` | `northeast_region__region` | included |
| Midwest / `MW` | `midwest_region__region` | `OUT_OF_GOVERNANCE` |
| South / `S` | `south_region__region` | included |
| West / `W` | `west_region__region` | included |

Provider evidence therefore contains five geographies and ten metric/geography
pairs.  Canonical r2 contains the intersection with the generated manifest:
nation, Northeast, South, and West, or eight metric/geography pairs.  Midwest is
not silently dropped: evidence records
`classification=OUT_OF_GOVERNANCE` and
`disposition=EXCLUDED_FROM_CANONICAL_CANDIDATE`, both metrics present, and
excluded row counts by metric.

## Immutable correction

The correction is revision 2 and must declare the exact r1 above in
`supersedes_artifact_id`.  The r1 Release, catalog record, and package remain
untouched.  The durable August pin embeds the exact starts and completions XLSX
bytes, so r2 is rebuilt by verified pin recovery without Census discovery or
provider reacquisition.  Canonical row keys, membership, Parquet bytes,
data/content hashes, package hash, and artifact ID necessarily change.

`jobs/monthly_refresh/august_nrc_geography_correction.py` is the only authorized
cycle-authority exception.  Its dry run builds r2 from the durable pin and emits
a plan-bound authorization token.  A later explicit live invocation may publish
and catalog r2, replace the exact August NRC cycle result, and rebuild the exact
August logical cohort plan.  It fails closed unless provider evidence is five by
two, candidate evidence is four by two, Midwest has the exact exclusion, the old
selection is the known r1, r2 supersedes that r1, and r2 lineage matches every
pinned member.

The correction does not move any accepted source, Source Set, canonical-market,
or serving pointer; does not consume Redfin readiness; and does not create a
Source Set or market.  Those transitions remain reserved for authorized cohort
promotion.

## Operational commands (not executed by this change)

Dry run:

```bash
python -m jobs.monthly_refresh.august_nrc_geography_correction \
  --repository OWNER/REPO --branch monthly-refresh-orchestration \
  --workspace /tmp/august-nrc-r2 --git-sha COMMIT \
  --output /tmp/august-nrc-r2-plan.json
```

After reviewing the dry-run output, live execution uses the exact emitted token:

```bash
python -m jobs.monthly_refresh.august_nrc_geography_correction \
  --repository OWNER/REPO --branch monthly-refresh-orchestration \
  --workspace /tmp/august-nrc-r2-live --git-sha COMMIT \
  --output /tmp/august-nrc-r2-live.json --live \
  --authorize 'AUTHORIZE_AUGUST_NRC_GEOGRAPHY_CORRECTION__<plan-sha256>'
```

Mandatory governed-geography validation for every other production source
publisher remains post-cutover hardening debt.  This correction keeps the
opt-in shared validator and applies it only to NRC.

## Gate 4 safety

The failed non-live Gate 4 attempt stopped during assembly before Source Set or
canonical-market publication and before prepared-promotion persistence.  Its
workspace files are disposable and require no durable cleanup.  Gate 4 may be
retried after r2 publication and exact cycle-result/logical-plan reconciliation.
