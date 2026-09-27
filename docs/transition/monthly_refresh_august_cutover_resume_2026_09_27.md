# August monthly refresh production cutover: resume handoff

**Handoff date:** 2026-09-27

**Migration implementation branch:** `monthly-refresh-orchestration`

**Expected migration HEAD:** `97e12e51` (full SHA
`97e12e510a0ea1b245b601bdf730a0ac904f2d70`)

**Paused at:** the default-branch workflow-dispatch/control-plane blocker,
before any correction preflight or live correction.

> **Completion bias — read this first.** The objective from this point forward
> is not to expand or improve the monthly architecture. It is to safely complete
> the August production refresh using the already-governed architecture. Do not
> turn non-blocking hardening opportunities into cutover prerequisites. Only
> introduce new implementation work when required to clear a demonstrated
> production blocker. Keep unrelated hardening debt explicitly deferred.

This is a self-contained operational handoff. It records repository contracts,
the last observed durable production state, implementation history, and the
remaining authorization stops. A new operator must nevertheless reread current
repository and remote `main` state immediately before issuing any production
command: `main`, not this document or a local checkout, is durable production
authority.

## 1. Locked monthly architecture

The established production flow is locked for this cutover and **must not be
redesigned**:

```text
Redfin local manual acquisition boundary
  -> durable Redfin candidate/readiness
  -> master monthly production cohort
  -> parallel automated sources
  -> common barrier
  -> Source Set v2
  -> canonical market
  -> serving market
```

The master supports `normal`, `resume`, and `replay`. `main` is the durable
production authority/control plane; `monthly-refresh-orchestration` is the
migration implementation/execution branch. A migration-branch checkout may
supply executable code, but authority reads and writes remain explicitly
against `main`.

Source jobs publish immutable candidates and durable results. They do not move
accepted pointers during source execution. Acceptance/promotion—and this
exceptional correction—requires separate authorization. Serving is a later,
separate transaction and authorization; it is not reconciliation and is never
implied by source or canonical acceptance.

The relevant architecture contracts are:

* `docs/contracts/monthly_refresh_production_v1.md`
* `docs/contracts/source_set_v2.md`
* `docs/contracts/redfin_monthly_source_v1.md`
* `docs/contracts/fred_unemp_governed_source_v1.md`
* `config/monthly_refresh_policy.json`
* `config/monthly_source_execution_registry.json`

## 2. Corrected cohort inventory

The authoritative barrier has **12 physical execution members**, in governed
order:

1. `redfin`
2. `fred_macro`
3. `fred_unemp`
4. `ces`
5. `laus`
6. `census_bps`
7. `census_bps_provisional`
8. `census_acs1`
9. `census_acs5`
10. `bea_gdp_qtr`
11. `bea_gdp_ann`
12. `census_nrc`

The authoritative Source Set has **10 logical/direct members**:

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

The two physical BPS executions resolve to one logical `bps` artifact. The two
physical ACS executions resolve to one logical `acs` artifact. `fred_unemp` is
a direct source, distinct from both `fred_macro` and LAUS. Historical records
and documents using 11 physical / 9 logical members remain valid only as
immutable historical or superseded evidence; they must not define a new
promotion.

## 3. Last observed August production state

Cycle:
`monthly_cycle__2026-08__a9e022a980d29cd7`.

The accepted cohort has these existing **nine** logical artifact identities,
which correction must preserve byte-for-byte as IDs:

| Logical source | Accepted artifact ID |
|---|---|
| `fred_macro` | `src__fred_macro__2026-09__r1__29a8ac35a657cfce` |
| `ces` | `src__ces__2026-08__r1__c39c12b32234bd93` |
| `laus` | `src__laus__2026-07__r1__a1b60a2a16d2b99f` |
| `redfin` | `src__redfin__2026-08__r1__f2ca39c3c36a9c2b` |
| `bps` | `src__bps__2026-08__r1__a25be34c54e12251` |
| `acs` | `src__acs__2024-12__r1__366cf814efe424ef` |
| `bea_gdp_qtr` | `src__bea_gdp_qtr__2026-03__r1__9290d93e61b8e5dd` |
| `bea_gdp_ann` | `src__bea_gdp_ann__2024-12__r1__1c51a5b7a95bdc27` |
| `census_nrc` | `src__census_nrc__2026-08__r2__569f6385f5eb6d72` |

Other accepted identities and state:

* Source Set: `source_set__2026-08__v2__fde2413d93948bee`.
* Canonical: `market__2026-08__r1__cfe00e7ec9a9be8e`.
* Original promotion:
  `cohort_promotion__1ea61a82ff16b1e492ba0f6f`.
* `accepted.source.fred_unemp` is absent before correction.
* `accepted.serving_market` is `null`.
* The exact August Redfin readiness was consumed by the original promotion. It
  must **never** be reset, reopened, made eligible, or consumed again during
  correction.

Gate 5 completed. Gate 6 failed because the accepted canonical lacked the
governed `fred_unemp` metric. Gate 7 has **not** been authorized.

These are last-observed remote production facts and must be verified afresh
against durable `main`. Do not substitute the repository checkout's local
`config/artifact_catalog.json` or `config/monthly_refresh_readiness.json`: those
checked-in snapshots are stale relative to the reported production state (for
example, the checkout's August readiness says `consumed=false`).

## 4. `fred_unemp` root cause and governed contract

`fred_unemp` was accidentally omitted from the migrated lifecycle. It was **not
intentionally retired**, folded into `fred_macro`, or transferred to LAUS.

It exclusively owns metric `fred_unemployment_rate_sa`, with exactly these six
governed geography/series pairs:

| Geography | FRED series |
|---|---|
| United States | `UNRATE` |
| California | `CAUR` |
| District of Columbia | `DCUR` |
| Maryland | `MDUR` |
| New Jersey | `NJUR` |
| Virginia | `VAUR` |

It uses ordinary FRED complete-history **current-truth** semantics, not an
ALFRED vintage. It remains semantically distinct from LAUS's non-seasonally
adjusted unemployment (`laus_unemployment_rate_nsa`); neither source overwrites
or coalesces the other.

The lifecycle is durable pin -> immutable candidate -> durable cycle result.
Normal mode may contact FRED only if durable authority has neither a valid
successful result nor an input pin. It acquires all six histories, normalizes
and embeds the exact deterministic bytes in a
`monthly_source_input_pin_v1`, persists and reads back that pin, then constructs
the candidate from the pin. Resume reuses a successful result or reconstructs
from the existing pin; replay requires the exact pin. After pinning, neither
mode rediscovers provider data. Publication/result recording must not move any
accepted pointer, readiness, or serving state.

Authoritative implementation and tests:

* Contract: `docs/contracts/fred_unemp_governed_source_v1.md`.
* Inventory/config: `config/geo_manifest.generated.csv`,
  `config/monthly_source_execution_registry.json`, and
  `config/monthly_refresh_policy.json`.
* Lifecycle: `jobs/monthly_refresh/fred_unemp.py` and
  `jobs/monthly_refresh/fred_unemp_hosted.py`.
* Reusable workflow: `.github/workflows/fred-unemp-monthly-source.yml`.
* Tests: `tests/test_fred_unemp_governed_source.py`,
  `tests/test_phase2_full_cohort.py`, and
  `tests/test_serving_fred_unemp_contract.py`.

## 5. August forward-correction contract

The governing plan schema is
`accepted_cohort_forward_correction_v1`. This is a forward correction, not a
rollback and not another ordinary Gate 5 promotion. The original promotion and
its artifacts remain immutable historical evidence.

The correction builds a corrected ten-member Source Set by copying the
existing nine entries and family-resolution lineage exactly and adding only the
real August `fred_unemp` artifact. It builds a canonical revision `r2` or later
whose manifest and catalog metadata explicitly supersede canonical r1. The
factual-delta check requires every old fact to remain identical and permits
only six-geography `fred_unemployment_rate_sa` additions owned by `fred_unemp`.

After target objects have been published and separately authorized, these are
the **only** accepted-state transitions, in this exact order:

1. `accepted.source_set`: old August Source Set -> corrected Source Set.
2. `accepted.canonical_market`: August canonical r1 -> corrected/superseding
   canonical.
3. `accepted.source.fred_unemp`: absent -> exact August `fred_unemp` artifact.

All nine existing `accepted.source.*` pointers remain unchanged.
`accepted.serving_market` remains `null`. Redfin readiness remains unchanged,
already consumed, and is evidence rather than an operation.

Every transition uses expected-old/target compare-and-swap semantics. If the
current value equals expected-old, it may advance; if it equals target, that
step is complete/idempotent; any third state fails closed. The hosted loop
applies at most one catalog transition per CAS, rereads authority, and
reevaluates. The exact plan-bound token
`AUTHORIZE_ACCEPTED_COHORT_FORWARD_CORRECTION__<plan-sha256>` authorizes only
these three transitions—not provider acquisition, readiness mutation, or
serving.

Authoritative correction files:

* Contract/audit: `docs/audit/august_fred_unemp_forward_correction_v1.md`.
* Pure plan, preflight, and recovery engine:
  `core/source_artifacts/forward_correction.py`.
* Offline preparation:
  `jobs/monthly_refresh/august_fred_unemp_forward_correction.py`.
* Durable hosted adapter:
  `jobs/monthly_refresh/august_fred_unemp_forward_correction_hosted.py`.
* Manual workflow:
  `.github/workflows/august-fred-unemp-forward-correction.yml`.
* Tests: `tests/test_august_fred_unemp_forward_correction.py` and
  `tests/test_cohort_production_authority.py`.

## 6. Implementation history and validation already completed

* PR **#266**, merged as `1e0fd45` (implementation commit `c9be57c`), added the
  offline August forward-correction machinery, schema/contract, canonical
  supersession support, and tests.
* PR **#267**, merged as migration HEAD `97e12e51` (implementation commit
  `38dc3da`), added the hosted durable correction adapter, the manual workflow,
  contract updates, and adapter/workflow tests.

Recorded validation history:

* The pre-PR merged branch passed **188 tests**.
* The PR #267 branch passed **192 tests** locally.
* Codex focused correction, source, authority, and relevant smoke suites passed.
* Codex's full suite was blocked only because that environment lacked required
  `openpyxl==3.1.5`.
* The local environment had `openpyxl==3.1.5` and passed the full PR suite,
  **192/192**.

This history is evidence, not permission to skip fresh focused validation after
any control-plane change.

## 7. Current exact blocker and repository contradictions

The correction workflow exists on `monthly-refresh-orchestration` but was not
registered on the default branch `main`:

```text
.github/workflows/august-fred-unemp-forward-correction.yml
```

Both `gh workflow run ... --ref monthly-refresh-orchestration` and a direct POST
to the workflow-dispatch API returned HTTP 404. GitHub requires a
`workflow_dispatch` workflow to be registered on the default branch. The first
direct-API attempt also had a shell-quoting problem; that was corrected, and
the subsequent request reached GitHub and returned the authoritative 404.

The default-branch workflow named **Governed cohort promotion (manual only)**
(`cohort-promotion-live.yml`) was inspected and is not a safe correction
launcher as observed there: it was hard-coded to the July cycle, invokes
`cohort_promotion_hosted`, and represents ordinary cohort promotion rather than
accepted-cohort forward correction. The next thread must determine the
smallest safe control-plane solution—likely a thin, `main`-registered manual
launcher that checks out/executes the governed migration implementation while
continuing to use `main` as durable authority. **Do not casually merge the
entire migration implementation into `main` merely to make
`workflow_dispatch` discoverable.**

### Contradictions/stale text that must not be silently reconciled

Repository truth exposes two important version distinctions:

1. At migration HEAD, `.github/workflows/cohort-promotion-live.yml` accepts a
   `cycle_id` input; it is not hard-coded to July. The hard-coded-July finding
   applies to the separately inspected default-branch `main` version, which is
   precisely why a fresh thread must inspect both remote refs before acting.
2. The opening section of
   `docs/audit/august_fred_unemp_forward_correction_v1.md` still says the
   implementation has “no hosted publication adapter,” while its later
   **Hosted production adapter** section and current code document the adapter
   merged by PR #267. Treat the opening sentence as stale pre-PR #267 wording;
   the adapter exists at `97e12e51`. Do not redesign or weaken boundaries to
   paper over this documentation lag.

The local checkout also has no configured Git remote, so this documentation
task could not independently reread current GitHub `main`. Production work must
restore/confirm remote context and verify GitHub state before commands.

## 8. Exact remaining execution sequence

The objective is to **complete the August refresh**, not continue expanding the
architecture.

### A. Clear only the control-plane blocker

Resolve default-branch workflow registration/dispatch with the smallest
governed change. Inspect current remote `main` and migration HEAD first. Keep
`main` as durable authority and preserve every authorization boundary.

### B. Run real **PREFLIGHT only**

Preflight may:

* contact FRED only if no valid durable pin/result exists;
* create the immutable `fred_unemp` pin, candidate, and result;
* create the corrected immutable Source Set;
* create the corrected immutable canonical artifact;
* create the immutable correction record; and
* emit the exact authorization token.

Preflight **must not**:

* move `accepted.source_set`;
* move `accepted.canonical_market`;
* move any of the nine existing accepted source pointers;
* set `accepted.source.fred_unemp`;
* reset, reopen, consume, or otherwise alter Redfin readiness; or
* alter serving state.

### C. Review real preflight evidence

Stop and review all of the following:

* exact durable `main` authority snapshot;
* FRED pin/result/artifact IDs and hashes;
* exact six-geography/series coverage;
* corrected ten-member Source Set and hash;
* proof that all nine existing artifact IDs are unchanged;
* superseding canonical ID/hash and r1 lineage;
* factual delta proving only governed `fred_unemp` additions;
* correction record ID/hash;
* unchanged consumed-readiness evidence and full-record hash;
* exact expected-old/target map in the required order; and
* exact plan-bound authorization token.

### D. Human authorization required

**Do not execute live correction merely because preflight passes.** Obtain
explicit human authorization for the reviewed token and exact plan.

### E. Execute the authorized correction

Execute exactly the three CAS transitions from section 5, in order. Do not add
another transition or reuse this authorization for serving.

### F. Verify correction state

Verify corrected Source Set, superseding canonical, and exact `fred_unemp`
pointer; verify all nine prior source pointers are unchanged, readiness remains
the same consumed record, and serving remains `null`.

### G. Rerun Gate 6 serving preflight

Run the separately governed serving preflight against the corrected accepted
canonical.

### H. Separate Gate 7 human authorization

Only if Gate 6 passes, stop and obtain a distinct human authorization for Gate
7. Neither the correction token nor its approval authorizes serving.

### I. Execute and verify serving promotion

Promote the exact reviewed serving target, then verify durable serving authority
and required downstream validation.

### J. Declare completion

Declare the August production refresh complete only after serving promotion and
verification succeed. Record unrelated hardening opportunities as deferred
debt, not retroactive cutover gates.

## 9. Fresh-thread bootstrap prompt

Copy/paste the following into a fresh ChatGPT/Codex thread:

```text
Resume the realestate-intel August production cutover. First read
docs/transition/monthly_refresh_august_cutover_resume_2026_09_27.md in full.
Treat current repository and remote durable main truth as authoritative, and
explicitly report contradictions rather than silently reconciling them.

Preserve the locked monthly architecture, main-as-authority boundary, immutable
candidate model, correction CAS order, consumed Redfin readiness, separate
serving transaction, and every human authorization stop. Before issuing any
production command, inspect current repository refs, remote main authority,
workflow registration, and migration HEAD (expected 97e12e51).

Resume specifically at the default-branch workflow-dispatch/control-plane
blocker. Find the smallest governed solution; do not casually merge the whole
migration branch into main. Optimize for safely finishing the August refresh,
not architecture expansion, and defer unrelated hardening. At each production
gate, give me only the next one or two actions—not a giant command block. Never
run live correction just because preflight passes, and require a separate human
authorization for Gate 7 serving promotion.
```
