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
- Linux is the primary supported development and verification target.
- The existing Windows backend is experimental and frozen during Wave 2. Do not add Windows parity work or expand its public guarantees unless a later architecture decision explicitly reactivates it.
- Prefer OS-native Linux ownership primitives: cgroup v2 when delegated, with process-session/subreaper fallback and a real PTY for interactive Runs.
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

Initial development may proceed without CI. Do not weaken or replace existing OS-bound verification with mocks. Wave 2 may be developed and reviewed from code/tests without a manual real-machine certification pass; environment-dependent proofs must remain explicit rather than being invented.


## Wave 2 authority

For the current hardening/evidence programme, `docs/WAVE2.md` is the implementation programme beneath the architecture/contract documents.

Wave 2 may strengthen physical execution evidence and control, but it must not introduce agent semantics or scheduling. In particular:

- caller retries must not create duplicate physical Runs;
- process/task-count terminology must match Linux kernel semantics;
- terminal receipts must describe the effective backend/capabilities actually used by that Run;
- lifecycle/control events must use one typed, versioned vocabulary;
- high-rate telemetry must not be treated as an unbounded lifecycle journal;
- host-wide safety envelopes may reject or constrain physical execution, but may not choose semantic work;
- retention/GC may discard bounded historical evidence only with explicit, machine-readable policy/evidence;
- Windows implementation work is out of scope unless required to keep the existing code compiling after shared-contract changes.
