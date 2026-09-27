# AGENTS.md — Jinushi implementation contract

Jinushi is a Go-native local execution substrate. Preserve the product boundary documented in README.md, docs/ARCHITECTURE.md, and docs/RUNTIME_CONTRACT.md.

## Authority

For implementation work, use this order:

1. docs/RUNTIME_CONTRACT.md — public runtime semantics.
2. docs/ARCHITECTURE.md — responsibility and product boundaries.
3. docs/IMPLEMENTATION.md — selected implementation direction.
4. accepted task/Issue scope.

Do not silently change an architectural contract while implementing it.

## Core invariant

One accepted Run is owned by Jinushi until its physical execution reaches a proven terminal state or is explicitly classified uncertain.

A PID is never the public Run identity.

## Keep Jinushi process-native

Jinushi owns:

- Run identity and lifecycle;
- process-tree ownership;
- stdio/PTY transport and bounded retention;
- resource telemetry and per-Run limit enforcement;
- signal/cancel/termination sequencing;
- event journal;
- execution lifetime/leases;
- restart reconciliation;
- terminal physical execution receipts.

Jinushi does not own:

- agent/session semantics;
- prompts/models/LLM providers;
- task scheduling or workflow DAGs;
- Git/GitHub semantics;
- repository/worktree/session authorization;
- code review or task-success judgment.

Do not introduce agent names or product-specific orchestration rules into Core.

## Implementation constraints

- Go is the implementation language.
- Linux and Windows are first-class V1 targets.
- Prefer OS-native ownership primitives: cgroup v2/process groups on Linux, Job Objects on Windows, real PTY/ConPTY for interactive Runs.
- Never construct a shell command from argv. Shell use must be an explicit executable supplied by the caller.
- Do not control arbitrary caller-supplied PIDs. Control uses Jinushi Run identity plus revalidated backend ownership.
- Never log raw environment values or secrets.
- Unsupported OS capabilities are explicit; do not emulate a hard guarantee and report it as enforced.
- Unknown/reconciliation failure is not success and not clean exit.
- A successful signal API call is not proof of process termination.
- Client await cancellation is not Run cancellation.
- Do not auto-restart a missing process during reconciliation.

## Resource evidence

Resource telemetry is part of the product contract.

Preserve explicit distinctions among:

- measured value;
- unavailable observation;
- unsupported capability.

Never convert unavailable evidence to zero.

Sampling, compaction, output truncation, and journal gaps must remain machine-readable.

## Durability

Run acceptance and lifecycle-critical external effects require durable state ordering.

Supervisor restart reconciliation must use persisted ownership evidence and current OS observation. When ownership cannot be proven, return an explicit uncertain state instead of guessing.

## Scope discipline

Implement the smallest coherent vertical slice required by the current task.

Do not add distributed control, cloud APIs, containers, repository management, GitHub integration, semantic agent adapters, automatic retries, or workflow scheduling unless a later accepted architecture explicitly introduces them.

Initial development may proceed without CI. Do not weaken or replace required real OS-bound verification with mocks once such verification exists.
