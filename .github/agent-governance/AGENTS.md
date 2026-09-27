# AGENTS.md — Organization coding-agent contract

This is the organization-wide always-on execution contract. Repository-local `AGENTS.md`, accepted Issues, canonical product sources, and live repository/GitHub state provide repository and task-specific authority.

Core rule: implement only the accepted gap; read only what can change the next decision; stop when the requested lifecycle is complete.

## Authority

Use this order unless the task defines a stricter authority chain:

1. latest explicit user instruction;
2. accepted Issue / Implementation contract;
3. repository-local canonical code, schema, validators, workflows, tests, and documented architecture;
4. repository-local `AGENTS.md` and scoped overlays;
5. live repository and provider state for volatile facts.

Do not redesign, rediscover, or reinterpret a settled contract without concrete contradictory evidence.

## Scope and reads

- Never invent requirements or broaden scope for cleanup, refactors, documentation, compatibility layers, or future extensibility.
- Start from the files, symbols, tests, commands, and dependencies named by the contract.
- Expand only for a direct dependency, unresolved acceptance condition, safety/security concern, failing validation, or blocker.
- Use the narrowest sufficient read. Do not perform repository-wide scans merely for orientation.
- Do not reread unchanged evidence or rerun unchanged discovery commands for confidence.
- Once evidence is sufficient to implement or decide, stop researching.

## Implementation

- Prefer the first solution that satisfies the accepted contract without weakening correctness, security, compatibility, accessibility, or architecture.
- Reuse canonical repository primitives before adding new machinery.
- Do not duplicate canonical helpers, validators, authorities, or public semantics.
- Do not add speculative abstractions, fallbacks, migration paths, extension points, or tests for hypothetical future behavior.
- Guard denials are execution boundaries. Never weaken guards, validation, assertions, tests, or security controls merely to pass.

## Git and isolation

- Never create, modify, stage, commit, or delete implementation work directly on `main` or `master`.
- Use the supplied governed branch/worktree/session when present; otherwise create the repository-governed isolated execution context.
- Do not overwrite, reset, stash, force-push, or commit unrelated work.
- Epic/integration branches are not implementation leaves.
- Treat ownership, stale-base, collision, and guard failures as blockers; do not bypass them.

## Validation and review

- Acceptance criteria and required checks in the accepted contract are authoritative.
- Run focused validation while the write-set is changing; run required final verification after it stabilizes.
- Rerun checks only after a result-affecting change.
- Never call an unexecuted, pending, stale, unavailable, hung, or environment-blocked check passed.
- Remote CI and local verification are distinct evidence.
- Review the actual diff against the contract, surrounding architecture, tests, CI, regressions, error paths, and scope. Do not create new requirements during review.

## Lifecycle and authorization

- Complete exactly the requested lifecycle. Do not stop early when the next governed step was requested and available; do not continue beyond the requested phase.
- Design, analysis, audit, and review are read-only unless mutation is explicitly requested.
- Creating or repairing a PR does not imply merge authority.
- Never merge, close, force-push, bypass governance, or change review state unless explicitly authorized.
- Report newly discovered out-of-scope problems; do not implement them.

## Organization Skills and governed CLI

Purpose-specific procedures live in `.github/agent-governance/skills/`.

Use the applicable organization Skill for implementation, review, pull-request, Epic/orchestration, audit, and release work. Load only the Skill required by the current task.

Inari usage is temporarily suspended for agent work. Do not invoke `inari`, including `inari skill`, `inari pr`, `inari issue`, or `inari template`, and do not treat Inari-generated live guidance as an execution authority. Follow the accepted Issue / Implementation contract, repository-local canonical sources, organization Skills, and live GitHub state directly until this suspension is removed.

Runtime profiles change delegation only; they do not bypass scope, isolation, validation, governance, or authorization boundaries.

## Repository-local AGENTS.md

Each repository should keep its root `AGENTS.md` short and repository-specific. It should contain only information needed on every task, such as:

- repository purpose and responsibility;
- critical architecture/ownership boundaries;
- canonical validation entrypoints;
- canonical authority locations;
- routing to organization Skills or repository-specific skills.

Detailed product behavior belongs in canonical code/docs/schema/tests. Task procedures belong in Skills. Task-specific requirements belong in the accepted Issue.

## Communication and truthfulness

- Report material results, blockers, changed plans, irreversible risks, or information needed for a decision.
- State each fact once.
- Prefer exact technical nouns and short active sentences.
- Report actual state, not expected state.
- Never say `done`, `passed`, `reviewed`, `merged`, or equivalent without supporting evidence.
- If evidence disproves an assumption, correct the plan immediately.
