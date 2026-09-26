# August 2026 FRED unemployment forward-correction contract v1

## Decision and safety boundary

August is repaired by a **forward correction**, not rollback and not a replay of
ordinary Gate 5.  The accepted nine-source promotion
`cohort_promotion__1ea61a82ff16b1e492ba0f6f` is immutable historical evidence.
Its Source Set `source_set__2026-08__v2__fde2413d93948bee`, canonical
`market__2026-08__r1__cfe00e7ec9a9be8e`, source artifacts, cycle results, pins,
and readiness record are never modified or deleted.

This implementation is preparation and offline proof only.  It has no FRED
client, no hosted publication adapter, and no command that writes production
authority.  It does not fabricate the future production `fred_unemp` identity.
Serving remains a separate Gate 6 authorization after correction acceptance.

## Immutable plan schema

`accepted_cohort_forward_correction_v1` is deliberately distinct from both
historical promotion v1 and normal ten-source promotion v2.  Its content-hash
identity binds:

- the exact August cycle, old Source Set, old canonical, and original promotion;
- all ten expected-old and target source pointer values (the old nine are
  identical and only `fred_unemp` moves from absent to an exact artifact);
- the corrected Source Set and canonical IDs and hashes, with both old IDs as
  explicit supersession parents;
- exact BPS and ACS family-resolution lineage;
- governed policy/config hashes;
- the exact consumed Redfin readiness ID, full-record hash, and consuming
  promotion ID;
- deterministic operations and the expected-old-or-target recovery contract;
- an authorization scope and plan hash.

The create-once store accepts an exact retry and rejects a different plan.
Preparation exposes the human-readable token
`AUTHORIZE_ACCEPTED_COHORT_FORWARD_CORRECTION__<plan-sha256>`.  Recovery cannot
run without an exact token; a changed plan necessarily produces a different
token.  Preflight hashes and rechecks its inputs and performs zero mutation.

## Corrected immutable artifacts

The Source Set builder starts from the accepted nine-member v2 manifest, copies
its nine entries and family-resolution map exactly, and adds only the supplied
`fred_unemp` entry.  Its logical inventory is exactly:

1. `fred_macro`
2. `fred_unemp`
3. `ces`
4. `laus`
5. `redfin`
6. `bps`
7. `acs`
8. `bea_gdp_qtr`
9. `bea_gdp_ann`
10. `census_nrc`

BPS/ACS parents remain lineage-only.  The corrected Source Set catalog record
must name the accepted Source Set in `metadata.supersedes_artifact_id`.

Canonical artifact schema v2 adds an explicit `supersedes_artifact_id` and
requires correction revision 2 or later.  The intended August identity is
therefore `market__2026-08__r2__<semantic-hash>`, never reused r1.  The offline
database comparator proves every accepted fact remains byte/value-identical,
the only added rows are owned by `fred_unemp` and
`fred_unemployment_rate_sa`, all six governed national/state geographies are
present, and canonical keys remain unique.  LAUS rows are retained separately;
there is no coalescing or overwrite.  Normal canonical assembly validation is
still required before publication.  The corrected canonical catalog record
must name r1 in `metadata.supersedes_artifact_id`.

## Redfin invariant

Consumed August Redfin readiness is evidence, not an operation.  Preflight
requires exactly one record whose cycle is the August cycle, whose artifact is
the unchanged accepted August Redfin artifact, whose `consumed` value is true,
and whose complete canonical JSON hash matches the plan.  It also validates the
historical promotion v1 record and proves that promotion is complete, binding
the consumed state to that promotion.

Neither preflight nor recovery copies a modified readiness value into the
result.  Recovery returns an unchanged deep copy.  There is no reset, reopen,
eligibility lookup, or consume transition.  Unconsumed, wrong-cycle,
wrong-artifact, or otherwise changed readiness fails closed.

## Transition and interruption recovery

After separate publication and authorization, the only accepted-state CAS
order is:

1. old Source Set → corrected Source Set;
2. old canonical → corrected r2 canonical;
3. absent `accepted.source.fred_unemp` → exact new artifact.

Before every step, all nine unchanged accepted source pointers, null serving
pointer, immutable old and target objects, catalog supersession metadata, and
consumed readiness evidence are revalidated.  A value equal to its target is an
already-completed operation.  A value equal to expected-old may advance.  Any
third value is a contradiction and fails closed.  Consequently a retry after
any operation continues forward, an exact completed retry is a no-op, and no
path resets a pointer backward.  A hosted production wrapper must apply at most
one returned operation per catalog blob CAS, reread both authorities, and
repeat this recovery evaluation.

## Precise next production sequence (not executed here)

1. On production authority, run the governed `fred_unemp` hosted source job for
   the exact August cycle.  Acquire all six FRED series once, persist the real
   immutable input pin, reread and verify it, then build the candidate from the
   pin only.
2. Publish/read back the immutable source candidate and write the exact August
   source cycle result.  Do not change any accepted pointer or readiness.
3. Resolve the real candidate, accepted nine-source Source Set, original
   promotion, catalog, consumed readiness, BPS/ACS lineage, and governed hashes
   from durable authority.  Do not use locally invented IDs.
4. Build the corrected ten-member Source Set offline; prove the nine IDs are
   exact; assemble the isolated canonical; run normal validation and the exact
   factual-delta proof; create the r2 manifest with r1 supersession.
5. Publish/read back the two immutable candidate objects without activating
   them.  Persist the create-once correction plan.
6. Run the **non-live correction preflight** against freshly reread production
   authority and export its exact authorization token for human review.  Stop.
7. In a later separately approved live task, supply that exact token to a
   one-operation-per-CAS hosted recovery loop.  Reread and verify completion.
8. Rerun Gate 6 under its own serving authorization.  The correction token must
   never authorize serving.

## Residual production risks

The hosted durable CAS wrapper is intentionally not introduced in this offline
task, and real source/Source Set/canonical identities do not yet exist.  The
production task must ensure publication metadata carries both supersession
parents and must retain the accepted-state/readiness snapshot between each CAS
retry.  Provider truth can change before acquisition; the durable pin and
candidate validation, rather than this correction plan, govern that boundary.
