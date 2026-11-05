# ACS historical backfill runtime compatibility v1

This contract applies only to ordinary ACS historical acquisition planning,
authorization and execution. It does not authorize acquisition, scope changes,
historical acceptance, publication outside the existing physical-only boundary,
or downstream processing. The reviewed main implementation/profile supplies
runtime policy; a caller cannot supply a different profile or grant an exception.

Initial profile semantic SHA-256: `d64df413d5541100d41cd3ed60e96bc4a557ba2da378704ba885842e6e49acc2`.

## Stable plan requirements

`jobs/monthly_refresh/acs_backfill_runtime_profile.json` is fingerprinted with
execution code. Its canonical semantic SHA-256 is included in
`repository_identity.serialization_runtime` under
`acs_backfill_runtime_compatibility_v1`. The complete repository identity remains
exactly equal across stages: repository, baseline, code/workflow/requirements
fingerprint, configuration/governance byte hashes, and runtime profile identity.
The plan's content-addressed hash binds that identity, provider evidence, exact
vintages, geography/exclusions, reconciliation, and original code/main references.
Plan-adoption commits may add review JSON without changing protected files; they
must retain reviewed code ancestry and pass fresh inventory reconciliation.

Supported host: **CPython 3.11.17 or 3.11.18, Linux, x86_64**. Other patches,
implementations, operating systems and architectures fail closed. The workflow
selects 3.11.18 explicitly to avoid a floating setup-python upgrade outside the
allowlist. Supporting another patch requires reviewed profile/code changes and a
new plan, not an ad hoc operator override. Standard manylinux wheel prerequisites
also apply; notably the selected NumPy/PyArrow builds require glibc 2.28 or newer.

Exact serialization distributions (official PyPI release metadata checked during
implementation; no wheel is checked into this repository):

| Distribution | Exact wheel | Archive SHA-256 |
| --- | --- | --- |
| pandas 2.2.2 | pandas-2.2.2-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl | 6d2123dc9ad6a814bcdea0f099885276b31b24f7edf40f6cdbc0912672e22eee |
| NumPy 2.4.6 | numpy-2.4.6-cp311-cp311-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl | 89cd468399cfd2504718f0ba50e410dca55a170b61a02ad92bb18c8a65186e93 |
| PyArrow 26.0.1 | pyarrow-26.0.1-cp311-cp311-manylinux_2_28_x86_64.whl | 39cb0f2418282c24bea6ceda77b03e5a8f0a5766b63a8bf194d4e2a2c423548c |

Sources: `https://pypi.org/pypi/<distribution>/<version>/json` for these three
exact releases. Versions alone are insufficient. Workflow preparation downloads
only binary wheels with pip `--require-hashes --no-deps`, then checks exact
filenames/hashes and force-installs those files without dependency resolution.
The existing general requirements installation remains unchanged; this bounded
step fixes the three serialization implementations afterward.

## Local verification and observations

Each job retains approved wheel files in `ACS_BACKFILL_WHEELHOUSE`. This variable
selects evidence bytes, not policy: missing files or any hash mismatch fail. Every
`repository_identity()` call rechecks the running host and installation:

1. Verify the complete wheel archive SHA-256 and installed distribution version.
2. Verify every hashed installed RECORD member against its actual file bytes.
   Missing integrity metadata and unsupported hash algorithms fail. Only RECORD
   itself and generated `__pycache__/*.pyc` may lack hashes.
3. Independently compare every wheel payload file to installed bytes, including
   native libraries and dist-info metadata. Only the rewritten dist-info RECORD
   and ZIP directory entries are excluded from that payload comparison. Unexpected
   wheel relocation layouts or duplicate members fail closed. The approved wheels
   contain no relocated `.data` payloads.

Pip-generated scripts and installation metadata are still locally checked against
RECORD, but are not required to be identical across installations. Rewriting
RECORD to bless modified package payloads cannot pass the independent wheel
comparison. Trust still assumes reviewed code, the Python process, OS/filesystem,
installer and GitHub main controls; this is not a sandbox against a hostile host.

`acs_backfill_runtime_observation_v1` retains full `sys.version`, executable
SHA-256, implementation/version/system/architecture, profile hash and each
installed distribution's version, approved wheel hash and exact installed RECORD
hash. Successful verification precedes its creation. Each workflow job uploads
this as `acs-backfill-runtime-<action>` before its plan/authorization/execution
command, so a later command failure does not discard the runtime observation.
It is a separate audit artifact: the independently verified authorization archive
continues to contain exactly `plan.json` and `authorization.json`. Runtime audit
files confer no approval and are not injectable production capabilities.

Python build strings/executable hashes and installed RECORD hashes may differ
between compatible runs. They are retained observations, not equality criteria.
The installed payload/host checks are still repeated at execution and write
fences, without relying on a once-issued observation or cached verification.
Failures report fixed rejected field names and never identity values, arbitrary
caller JSON keys, provider payloads, tokens or signed URLs.

## Lifecycle and preserved boundaries

New identities are `acs_historical_execution_plan_v2` and
`acs_historical_execution_authorization_v2`. Old v1 plans/authorizations are not
accepted or reinterpreted. In particular, PR #307's unchanged plan
`acs_historical_execution_plan_v1__60606dd220af6eca66a79ffcf5d56f214e7308d0afacc4b84be4802e5535e5d7`
remains historical evidence. It cannot authorize the changed code/runtime policy.

`verify_plan`, `authorize`, `execute_batch`, CLI current-main checks and
`PlanAuthority.current_commit` all use the same exact stable identity. The latter
recomputes runtime verification on every fenced read/write. Pin/result stores,
physical catalog writes and pre-publication callbacks retain those checks.
Independent successful main authorization-run/archive verification, exact archived
plan/authorization equality, source/vintage/cycle restrictions, provider snapshot
and HTTP 204 omission checks, immutable publication/reuse, collision rejection,
durable pin verification, scoped inventory reconciliation and non-force Git
advancement are unchanged. No compatibility profile waives one of these checks.

After external review/merge: run a new ACS5 2023 preflight, adopt the exact new v2
plan through review, obtain a distinct successful authorize run, then separately
authorize execution with that exact run/plan. Do not retry the old v1 plan under
the new code. No workflow dispatch or acquisition occurs in this implementation.

## Limits

The wider application requirements are not converted to a global dependency lock.
The three previously identity-bound serialization distributions are now exact
approved wheels; other dependencies retain existing installation policy. Artifact
and provider checks remain necessary regardless of environment compatibility.
Wheel availability, network/install failure, unsupported hosts and hash mismatches
stop execution. The wheel comparison adds local I/O at every authority check.
A verified profile does not promise identical serialization across arbitrary
runtime versions; immutable package hash/replay checks continue to reject changed
bytes rather than overwrite an existing acquisition. The full runtime observation
artifact should be retained with operator review evidence beyond its workflow
retention window when needed for long-term audit.

## Local validation for this implementation

The implementation is based on main `391728930475b2a0b60f33bc09ae5a5d7e4e42b5`,
including merged PR #308. All eight existing implementation files were
fingerprinted before the fast-forward and preserved byte-for-byte during it.
Only this validation section was subsequently updated with new results.

- The deterministic bootstrap-artifact regression now passes without any changes
  to the runtime implementation or additional governance/proposal edits.
- Focused runtime/backfill/write-readback regression: **357 passed**, both in the
  project environment and in a clean supported CPython 3.11.18 environment.
- Full project-environment suite: **1,161 passed**, 138 existing warnings.
- Full clean CPython 3.11.18 environment suite: **1,161 passed**, with the same
  138 existing warnings.
- A new virtual environment was created with actual official GitHub Actions
  CPython 3.11.18, then installed the repository requirements and ran the real
  `acs_backfill_runtime --prepare` hash-locked download/install entrypoint.
  `pip check` passed, followed by real `--observe`, `compatibility_identity` and
  `repository_identity` validation. No host/version checks were monkeypatched.
- The official interpreter archive was
  `https://github.com/actions/python-versions/releases/download/3.11.18-40260280589/python-3.11.18-linux-22.04-x64.tar.gz`,
  independently checked against its advertised SHA-256
  `2fd26e1f81ede41906d1e76d5d4de447a904e7fb42bd7635fc4dac0753f607b2`.
  It ran on Linux x86_64 under the profile, without installing to a system path.
- All three exact approved wheel archives and installed payloads passed integrity
  verification. The clean runtime observation file SHA-256 was
  `00ca881e242b38385fcfd9d13adce4d6ff301910e58de4d471e58806ff979664`.
  The observation is local test evidence, not governance approval or production
  verification. No interpreter archive or wheels are included in this change.
- Workflow YAML, embedded shell syntax, environment imports and whitespace checks
  passed. No workflow, provider acquisition, or production operation was run.
- PR #307's plan remains byte-for-byte unchanged (file SHA-256
  `cc7aa5a196b0490a65b902631448fd473d50f0b1f010c8ff55c06266e228dd46`).
  The actual clean runtime also rejected it under the v2 identity contract.
