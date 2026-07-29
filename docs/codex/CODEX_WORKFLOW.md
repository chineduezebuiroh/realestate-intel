# Codex Contribution Workflow

## Purpose

This document defines the standard workflow for Codex-assisted implementation in
the Real Estate Intel repository.

It supplements (but does not replace):

- `docs/project_memory.md`
- `docs/regime_engine_roadmap.md`
- architecture documents under `docs/architecture/`
- architectural decision records under `docs/adr/`
- relevant phase decision documents

`project_memory.md` explains the current state of the repository.

This document explains **how implementation work should be performed safely**.

---

# 1. Session Bootstrap

Before making implementation changes, always review:

1. `docs/project_memory.md`
2. `docs/regime_engine_roadmap.md`
3. Relevant phase decision document(s)
4. Relevant architecture document(s)
5. Relevant ADR(s)
6. Any implementation contract(s) governing the requested work

Then inspect:

- current Git branch
- `git status --short`
- existing modified files
- existing untracked files
- relevant production implementation
- relevant experiment implementation
- relevant registries
- relevant smoke tests

Before writing code, establish:

- the implementation objective;
- whether the work is production, experimental, diagnostic, documentation, or infrastructure;
- expected files to modify;
- expected production-policy impact;
- expected validation strategy.

Never begin implementation without first understanding the current repository state.

Implementation contracts are authoritative for implementation behavior.
If an implementation contract exists for the requested work, review it before writing code.

---

# 2. Core Engineering Principles

Preserve the following engineering contracts.

## Deterministic execution

- deterministic execution
- immutable production runs
- explicit batch identity
- explicit data-as-of identity
- reproducible artifacts

## Registry-driven behavior

Production behavior should be determined through registries and documented policy.

Avoid introducing hard-coded production behavior.

## Explicit lineage

Maintain complete lineage for:

- source observations
- transformed observations
- derived metrics
- engineered features
- production artifacts

Never introduce implicit source substitution.

## No silent fallbacks

If required inputs are unavailable, fail explicitly or document the intended behavior.

Never silently substitute alternative production behavior.

---

# 3. Production and Experiment Separation

Production and experimental code serve different purposes.

Experimental modules may:

- evaluate challenger policies;
- generate diagnostics;
- compare production candidates;
- produce review artifacts.

Production modules own accepted production behavior.

Production code should **not** depend upon computational implementations that live exclusively under:

```
regime/experiments/
```

When experimental behavior is promoted:

1. move or expose the shared computation through a production-safe module;
2. have both production and experiment implementations call the shared logic;
3. preserve deterministic contracts;
4. preserve lineage contracts;
5. update documentation where appropriate.

Avoid duplicating computational algorithms between production and experiment code.

---

# 4. Scope Discipline

Each implementation task should represent one coherent engineering objective.

Before editing, identify:

- files that must change;
- files that may change;
- files that should not change.

Avoid unrelated edits.

Avoid opportunistic refactoring unless explicitly requested.

Do not overwrite unrelated uncommitted work.

---

# 5. Testing Expectations

Validate the smallest relevant scope first.

Possible validation includes:

- unit tests;
- smoke tests;
- deterministic reruns;
- artifact validation;
- lineage validation;
- comparison diagnostics;
- review exports;
- production-versus-candidate comparisons.

Before considering work complete:

```bash
git diff --check
```

Report:

- tests executed;
- tests passed;
- tests not executed;
- remaining risks.

---

# 6. Artifact Discipline

Persisted production artifacts are authoritative.

Generated review exports should remain separate from production artifacts.

Do not commit generated artifacts unless explicitly requested.

Do not commit:

- `.DS_Store`
- temporary analysis output
- scratch files
- generated review exports
- generated production runs

unless the task specifically requires versioning them.

---

# 7. Documentation Expectations

Documentation should evolve with architecture.

Use:

| Document | Purpose |
|----------|---------|
| ADR | Durable architectural decisions |
| Phase decision document | Investigation outcomes |
| Architecture documents | Current system architecture |
| `project_memory.md` | Current repository state |
| `regime_engine_roadmap.md` | High-level project roadmap |

Avoid duplicating the same information across multiple documents.

---

# 8. Git Workflow

Before staging:

```bash
git status --short
git diff --check
```

Stage files explicitly.

Avoid:

```bash
git add .
```

when unrelated working-tree changes exist.

A commit should represent one coherent engineering objective.

Commit messages should describe the resulting implementation.

## Environment-specific Git behavior

When operating in a user-controlled local repository, do not:

- create branches;
- switch branches;
- merge branches;
- rebase;
- commit;
- push;

unless explicitly instructed.

When operating through an isolated hosted Codex task environment, the platform may create task branches, commits, or pull-request records automatically.

In that environment:

- do not perform additional branch manipulation;
- do not merge or rebase;
- do not directly push outside the platform-managed task workflow;
- do not create extra commits beyond platform-managed task completion unless
  explicitly requested.

Always report:

- the branch used;
- whether a commit was created;
- the commit SHA, when available;
- whether a pull-request record was created;
- whether any remote branch was modified.

---

# 9. Completion Report

At the end of each implementation task, provide:

1. Objective completed
2. Files modified
3. Behavioral changes
4. Tests executed
5. Generated artifacts
6. Production-policy impact
7. Remaining risks
8. Recommended commit scope
9. Git actions performed or platform-created

For hosted tasks, distinguish platform-created Git activity from Git actions explicitly initiated during implementation.

Explicitly identify anything left incomplete.

---

# 10. Standard Task Prompt

Typical Codex requests should begin with:

> Read `docs/codex/CODEX_WORKFLOW.md` and follow the documented workflow.
>
> Task:
> *(implementation objective)*
>
> Constraints:
> *(task-specific constraints)*
>
> Acceptance criteria:
> *(required validation and expected behavior)*
