# Agent runtime profiles and prompt construction

> Canonical source: `yohn-jp/.github/docs/agent-runtime-profiles.md`.
> Consumer repositories receive a generated copy at `.github/agent-governance/runtime-profiles.md`. Machine-readable profiles are synchronized alongside it.

This document defines how organization governance is projected into runtime-specific execution prompts. Runtime profiles are adapters: they describe delegation, implementation authority, context handling, and reporting for a runtime/model configuration. They do not redefine Issue scope, branch topology, repository policy, or product architecture.

The machine-readable source is `.github/agents/runtime-profiles.json`, validated structurally by `.github/agents/runtime-profiles.schema.json` and semantically by `scripts/validate-runtime-profiles.mjs` (unique profile IDs, resolvable authority references). Its `authority.workflow` and `authority.promptGuide` entries each carry a `canonical` path (resolved against this provider repository) and a `projected` path (resolved against a consumer repository after `.github/sync-agents.yml` projection) — the same document is copied verbatim to consumers, so a single bare path could never resolve correctly in both contexts.

## 1. Prompt projection inputs

A task prompt should be derived from explicit inputs rather than reconstructed from operator memory:

```text
shared change-workflow governance
        +
repository-local overlay / executable policy
        +
current Issue or Epic metadata
        +
current branch/base/worktree state
        +
runtime profile
        +
current evidence / blockers
        =
execution prompt
```

The prompt is a projection of these authorities, not a new source of truth.

## 2. Required prompt contract

Every write-capable implementation prompt should make the following fields explicit when applicable:

```text
Repository
Mode: standalone | epic-orchestrator | epic-child | review | merge | release
Issue / Epic
Expected base
Expected branch
Worktree/session ownership
Goal
Accepted scope
Dependencies
Implementation envelope / expected write-set
Runtime profile
Delegation and parallelism rules
Validation requirements
PR target/base and lifecycle end condition
Explicit prohibitions
Required final report
```

Do not copy large static governance documents verbatim into every prompt. Reference canonical authority and repeat only high-risk invariants that materially prevent execution mistakes, especially:

- no direct implementation on `main`/`master`;
- dedicated worktree/branch ownership;
- exact Issue/Epic identity and expected PR base;
- no scope expansion;
- current-state reconciliation before creating duplicate artifacts;
- evidence-based validation/review claims;
- requested lifecycle end state.

## 3. Prompt construction algorithm

1. **Resolve the task identity.** Determine the exact repository, Issue/Epic, requested mode, and requested lifecycle. Do not infer a different task because adjacent work appears useful.
2. **Reconcile live state.** Confirm volatile branch/base/PR/Issue facts that affect execution. Do not encode stale remembered state into the prompt.
3. **Resolve the workflow topology.** Standalone Issue, Epic orchestrator, or Epic child determines the branch/worktree/PR base contract.
4. **Capture the implementation envelope.** State the semantic outcome, known write boundary, dependencies, forbidden escalation, and validation evidence.
5. **Select the runtime profile.** Choose the profile that matches the actual execution topology. Do not assume capabilities the runtime does not expose.
6. **Derive delegation.** Parallelize from the dependency DAG and write-set, not from model count. Shared authority/files require deliberate integration or serialization.
7. **Set the end condition.** State whether the session ends at analysis, implementation, PR creation, review, merge, release, or another explicit lifecycle boundary.
8. **Require an evidence report.** The worker/orchestrator reports actual changed files, validation executed, PR state, blockers, and unresolved deviations. It does not report predicted success as fact.

## 4. Prompt anti-patterns

Do not generate prompts that:

- ask an agent to rediscover facts already explicit in the Issue/prompt;
- omit the expected PR base for Epic children;
- say merely `follow governance` while failing to repeat a high-risk branch/worktree invariant;
- ask every worker to modify the same shared file independently;
- split one requested Sol-orchestrated or autonomous Claude session into multiple top-level sessions without a dependency/topology reason;
- tell an implementation worker to redesign architecture already fixed by an accepted Issue;
- tell an orchestrator to perform all leaf implementation merely because it can;
- instruct an agent to bypass Inari/guards when governance validation fails;
- claim CI/review/merge completion in advance;
- leave `finish when done` ambiguous about whether a PR, review, merge, or release is included;
- embed brittle leaf CLI flags that should instead be discovered from the current `inari skill` or executable authority.

## 5. Profile: `sol-luna-orchestrated`

Use for a single GPT-5.6 Sol top-level session orchestrating GPT-5.6 Luna implementation workers.

### Sol authority

- Own task decomposition, dependency DAG, worker assignment, integration order, conflict resolution, shared-contract reconciliation, and final evidence report.
- Delegate exploration and leaf implementation to Luna where the work is independently scoped.
- Do not perform ordinary leaf implementation itself.
- Sol may make integration-only edits when multiple worker changes overlap in the same file or canonical authority and a single coherent reconciliation is required.
- Preserve one top-level Sol session when that topology was requested; do not return several unrelated prompts in place of orchestration.

### Luna worker authority

- One scoped Issue/task per worker unless the prompt explicitly groups inseparable work.
- One dedicated worktree/branch per write-capable worker.
- Implement the accepted envelope, run targeted validation, and return concise evidence to Sol.
- Do not broaden scope, create unrelated Issues, or take over orchestration.

### Parallelism

Start Luna workers concurrently only when dependency and expected write-set analysis shows independence. If workers would modify the same canonical table/schema/generated source or semantically coupled files, serialize that authority or have Sol own the integration point.

### Required Sol prompt clauses

Include:

- `Sol is the orchestrator; leaf implementation is delegated to Luna.`
- exact Epic/Issue/base/worktree topology;
- the set of tasks and dependency edges;
- which files/authorities are potential integration hotspots;
- `do not split this into separate top-level sessions unless the dependency model requires it`;
- the requested lifecycle boundary and final report format.

## 6. Profile: `luna-worker`

Use when GPT-5.6 Luna receives one bounded implementation leaf directly.

Prompt characteristics:

- exact Issue/task identity and acceptance criteria;
- exact worktree/branch/base;
- narrow implementation envelope;
- explicit validation commands/evidence when known;
- no architecture rediscovery unless the Issue explicitly makes architecture part of the task;
- no orchestration or speculative child-Issue creation;
- concise final report: changed files, validation, PR/lifecycle state, blocker/deviation.

## 7. Profile: `claude-code-autonomous`

Use when Claude Code is expected to drive an implementation session autonomously, including use of its available subagent facilities where useful.

- The primary session owns plan reconciliation, implementation/integration, validation, and the requested GitHub lifecycle.
- Delegate independent exploration or independently isolated implementation tracks to subagents when the runtime supports it and doing so reduces contention.
- Write-capable parallel subagents still require separate worktree/branch ownership where repository policy requires isolation.
- If the user requested one autonomous Claude session, keep orchestration inside that session rather than returning multiple top-level prompts.
- Do not let subagents independently redefine the accepted architecture or shared canonical contract.
- The primary agent reconciles all returned work against the current base before presenting completion.

## 8. Profile: `codex-scoped`

Use for a Codex implementation session when no stronger runtime-specific orchestration topology has been explicitly selected.

- Treat the session as one bounded implementation authority by default.
- Use the provided/current governed worktree and Issue branch; do not create extra worktrees when already in the correct one.
- Implement the smallest coherent change and complete the explicitly requested lifecycle.
- Use parallel/subagent execution only when the active runtime actually exposes it and the dependency/write-set model permits it; do not encode assumed capabilities into the canonical prompt.
- Return concrete diff/validation/lifecycle evidence.

A future Codex orchestration profile should be added as a separate profile rather than silently changing this profile's semantics.

## 9. Repository-local overlays

Consumer repositories may define:

```text
.github/agent-governance/repository-overlay.md
```

Typical overlay content:

- product architecture boundaries;
- required package/test/build commands;
- repository-specific generated-file rules;
- stricter read/write/exec constraints;
- integration hotspots or files that must not be modified concurrently.

The overlay augments the shared workflow. It cannot weaken organization invariants or executable repository policy.

## 10. Runtime-profile maintenance

Runtime/model capabilities change faster than the core change workflow. Therefore:

- update runtime profiles without rewriting workflow semantics;
- version machine-readable profile changes explicitly through repository history;
- do not infer new capabilities from model names alone;
- keep vendor/model-specific advice in profiles, not in shared Issue/branch semantics;
- when runtime behavior and a profile diverge, treat the profile as stale and correct it rather than compensating with one-off prompt folklore.

## 11. Future Inari projection

The target interface is deterministic prompt projection from current authorities, conceptually:

```text
governance + repository overlay + Issue/Epic metadata + runtime profile -> prompt
```

Inari may later expose generation/validation for this projection. Until that executable support exists, prompts are constructed manually from the same sources. Do not introduce a second independent prompt schema in individual repositories.
