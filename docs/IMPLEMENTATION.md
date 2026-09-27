# Jinushi Implementation Architecture

## 1. Language and packaging

Jinushi is implemented in Go.

Reasons:

- process ownership is the primary domain;
- Linux OS primitives are central to the supported target; the existing Windows implementation is retained but not a Wave 2 parity target;
- a single native binary simplifies distribution;
- Go provides direct access to platform process/resource APIs;
- the runtime should not depend on Node.js, npm, Python, or an agent runtime;
- upper-layer products can integrate through a local protocol rather than FFI.

The initial deliverable is one repository and one product. Internal Go packages are used to preserve boundaries; do not split components into separate repositories or services without evidence that independent deployment is required.

## 2. Initial executable topology

The product has two logical roles that may ship from one binary:

```text
jinushi <client command>
        |
        | local IPC
        v
jinushi supervisor
        |
        +-- Run guardian/backend ownership
        +-- Run registry
        +-- event/output retention
        +-- telemetry
```

The exact process layout may differ by OS.

A platform may require an internal per-Run guardian process to preserve execution ownership across supervisor restart. Such a guardian is an internal mechanism, not a public semantic actor.

## 3. Package boundaries

Proposed Go layout:

```text
cmd/jinushi/
  main.go

internal/protocol/
  versioning
  request/response envelopes
  event stream

internal/run/
  identity
  lifecycle
  lineage
  receipt
  lifetime/lease

internal/supervisor/
  service
  admission of run requests
  control operations
  startup reconciliation

internal/process/
  common process-tree contracts
  termination sequencing

internal/io/
  stdio
  spool
  backpressure
  attach/detach

internal/pty/
  common PTY contract
  platform adapters

internal/resource/
  telemetry model
  limits
  sampling
  aggregation

internal/store/
  durable Run metadata
  event journal
  receipts
  output metadata

internal/backend/
  capability model
  platform interface

internal/backend/linux/
  process groups/session
  cgroup v2
  PTY
  Linux telemetry

internal/backend/windows/
  Job Objects
  ConPTY
  Windows telemetry

internal/ipc/
  Unix-domain socket
  Windows named pipe

internal/cli/
  thin local client
  human rendering
```

Names are provisional; responsibility boundaries are not.

## 4. Backend interface

The platform-neutral supervisor must depend on a narrow backend contract.

Representative responsibilities:

```go
type Backend interface {
    Capabilities(ctx context.Context) Capabilities

    Start(ctx context.Context, spec StartSpec) (OwnedExecution, error)
    Observe(ctx context.Context, owned OwnedExecution) (Observation, error)
    Signal(ctx context.Context, owned OwnedExecution, signal Signal) error
    Terminate(ctx context.Context, owned OwnedExecution, policy TerminationPolicy) (TerminationResult, error)
    Reconcile(ctx context.Context, durable DurableOwnership) (ReconcileResult, error)
}
```

The exact API is implementation work. Core requirements:

- OS handles/tokens never leak into public protocol;
- the backend returns evidence, not semantic task state;
- unsupported capabilities are explicit;
- ownership is stronger than PID identity.

## 5. Linux implementation direction

Linux is a first-class V1 backend.

### Process ownership

Use one dedicated execution ownership boundary per Run.

Preferred order:

1. cgroup v2 for descendant membership/resource enforcement where available;
2. process group/session for signal delivery and fallback process-tree behavior;
3. explicit process identity evidence for reconciliation.

Jinushi must not assume cgroup availability on arbitrary Linux. Capability discovery determines which guarantees are supported.

### Resource telemetry and limits

Where cgroup v2 is available, prefer kernel-owned counters/limits for:

- memory;
- CPU;
- process count;
- process membership.

Do not reproduce kernel accounting by summing an incomplete process snapshot when stronger cgroup evidence is available.

### PTY

Use a real PTY implementation. Interactive process semantics must not be simulated by pipes.

### Restart reconciliation

Persist the OS ownership identity required to reconnect a Run to its execution boundary.

If process/cgroup identity cannot be proven current after restart, classify the Run as uncertain.

## 6. Windows implementation direction

The existing Windows backend is an experimental implementation retained for compatibility and future evaluation. It is frozen during Wave 2: shared contract changes may keep it compiling, but new Windows parity, certification, or capability work is not required.

### Process ownership

Use Job Objects as the canonical process-tree mechanism where feasible.

Required semantics:

- descendants are assigned to one owned Run boundary;
- process count and resource limits use Job Object capabilities where available;
- termination applies to the owned job rather than caller-supplied arbitrary PIDs;
- process lifecycle notifications are consumed when Windows exposes them.

### PTY

Use ConPTY for interactive workloads.

### Restart durability

Windows restart persistence must be designed explicitly.

If keeping a Run alive requires an internal guardian or service-held ownership handle, implement that mechanism rather than claiming that a recreated Job Object is equivalent.

Supervisor restart must never silently lose process ownership and still report the Run as normally managed.

## 7. Persistence model

The runtime needs crash-safe durable control state.

The data model must preserve:

- Run spec identity;
- lifecycle state and generation;
- lifetime/lease state;
- durable OS ownership reference;
- event sequence/watermarks;
- output-retention metadata;
- terminal receipt;
- reconciliation status.

The current implementation uses bbolt as the embedded transactional store for Run state, lifecycle/control events, and imported output metadata. A durable per-Run guardian owns the OS backend, maintains a bounded output ring and physical snapshots, and lets the supervisor reconcile after restart.

The architecture requires crash-consistent transactional state; bbolt is the current implementation authority. Changing the store is not a Wave 2 objective unless a concrete contract defect requires it.

## 8. State write ordering

External effects and durable state must use explicit ordering.

### Before spawn

Persist enough accepted/starting state that restart can distinguish an accepted request from an unknown process.

### After OS ownership establishment

Persist the exact durable ownership evidence before reporting `running` to the caller.

### During execution

Events/telemetry can be buffered within a bounded durability policy, but lifecycle-critical events must not be acknowledged as durable before persistence.

### Terminal

Persist the terminal receipt and final lifecycle transition atomically enough that restart cannot report one without the other.

## 9. Event journal

The event journal is append-oriented and sequence-based.

Requirements:

- monotonic sequence per Run;
- bounded record size;
- deterministic event schema;
- concurrent readers;
- retained-history watermark;
- compaction without pretending history is complete;
- critical lifecycle events preserved at least through terminal receipt construction.

The Wave 2 design stores high-rate telemetry separately with its own bounded
compaction policy. The current supervisor still writes `resource.sample` events
to this journal; that #6 migration remains incomplete as recorded in §24.

## 10. Output spool

stdout/stderr/PTY output is potentially unbounded.

Implementation requirements:

- never retain unbounded output in RAM;
- spool to disk under a bounded policy;
- retain total observed byte counters independently from retained content;
- apply one aggregate per-Run retention ceiling across stdout, stderr, and PTY;
- allow tail/range consumption by sequence/offset;
- expose compaction/truncation;
- apply backpressure or bounded drop/compaction policy without deadlocking the owned process;
- terminal receipt records whether complete history remains.

The runtime should favor evidence continuity metadata over pretending every byte can be retained forever.

## 11. Telemetry sampling

Telemetry combines:

- event-driven process lifecycle changes where available;
- periodic resource samples.

Sampling must be configurable within supervisor-owned bounds.

The runtime records the effective sampling interval/capability so later analysis can distinguish "no spike observed" from "sampling too coarse to prove no spike occurred."

Resource aggregation for the terminal receipt is derived from recorded observations plus stronger backend final counters where available.

## 12. Resource limits

The supervisor defines machine-wide maximum policy. A Run can request equal or narrower limits.

Example supervisor concerns:

```text
max Run memory request
max Run process count
max Run Linux task/PID count
max Run wall time
max output retention
max event retention
minimum telemetry sampling interval
termination grace ceiling
```

This is a safety ceiling, not a workload scheduler.

Linux maps accepted memory, CPU, and task/PID limits to cgroup v2 when the relevant controller is delegated. Linux does not advertise process-count enforcement from `pids.max`, because the controller counts threads as well as process leaders. A required hard limit that cannot be enforced is rejected explicitly. The experimental Windows backend may preserve its existing behavior but is not a Wave 2 parity target.

## 13. Termination implementation

Termination must be deterministic and observable.

Pseudo-flow:

```text
request termination
  -> persist reason/generation
  -> graceful tree signal
  -> observe
  -> bounded grace
  -> force tree termination if policy allows
  -> observe until empty/proven terminal
  -> receipt
```

Do not equate a successful signal syscall/API call with successful termination.

## 14. Lifetime and lease implementation

Detached Run ownership belongs to the supervisor/runtime, not to the initiating client connection.

Lease-bound execution needs durable lease generation and expiry.

Lease renewal must reject stale generations.

The supervisor must have one timer/reconciliation source of truth; multiple connected clients cannot each independently own a lease timer.

## 15. Local IPC

V1 is local only.

### Linux

Use a Unix-domain socket under the Jinushi runtime state/runtime directory.

### Windows

Use a named pipe with local-machine access control.

### Protocol

Prefer a small framed JSON protocol for request/response and a streaming frame/NDJSON-like surface for events/output.

Requirements:

- explicit protocol version negotiation;
- bounded frame size;
- stable error envelopes;
- no network bind;
- no public HTTP server in V1;
- reconnectable clients;
- concurrent readers;
- cancellation of client wait does not imply cancellation of Run.

## 16. CLI

The CLI is intentionally thin.

It should:

- parse user arguments;
- connect to the supervisor;
- project the same machine protocol;
- render bounded human output when requested;
- propagate meaningful process/command exit semantics without inventing task semantics.

It should not:

- implement a second lifecycle state machine;
- inspect OS processes directly;
- own persistence;
- reconstruct Run state when the supervisor is unavailable.

## 17. Supervisor lifecycle

The resident supervisor owns:

- protocol endpoint;
- store;
- backend capability detection;
- all Run control decisions;
- reconciliation;
- retention/compaction;
- shutdown behavior.

Startup sequence:

```text
open state
  -> acquire single-supervisor ownership
  -> initialize backend
  -> reconcile all non-terminal Runs
  -> expose IPC ready
```

A second supervisor against the same state root must fail closed.

Graceful shutdown does not automatically terminate detached Runs. The chosen OS ownership mechanism must preserve or explicitly classify their fate.

## 18. Security rules

Implementation must preserve:

- `execve`/direct process semantics; no hidden shell;
- argument boundaries exactly;
- no logging of raw environment values;
- no arbitrary PID control;
- no path-to-process identity assumptions without OS revalidation;
- local IPC access control;
- bounded protocol payloads;
- bounded spool/journal storage;
- fail-closed stale generation handling;
- zero semantic interpretation of untrusted child output.

The runtime is not a sandbox. Do not market process ownership as filesystem/network isolation.

## 19. Opaque metadata

Correlation metadata is useful for AI-native composition but dangerous if unbounded.

V1 should support a small bounded label map, for example:

- limited number of entries;
- bounded key/value bytes;
- no nested arbitrary JSON;
- never interpreted for authorization;
- optionally excluded/redacted from default human output.

This is enough to correlate a Run with an external agent/tool/task identity without importing that product's schema.

## 20. AI-workload design consequences

The following are deliberate consequences, not extra responsibilities.

### Agent session as Run

An orchestrator can launch one Codex/Pi/OpenCode process per Jinushi Run and obtain physical session evidence.

### Tool process as child evidence

When an agent directly spawns `git`, `pnpm`, `tsc`, etc., the OS process tree can expose that physical work under the parent Run.

### Externalized tool execution

A harness can choose to invoke tools as independent child Runs. This gives each tool call its own budget, receipt, and telemetry.

Jinushi does not mandate or implement that harness policy.

### Cross-agent resource analysis

Because all Runs use comparable telemetry, another product can later identify concurrent heavy workloads, repeated tests, or host-pressure patterns.

Jinushi emits facts only.

## 21. Initial implementation milestones

The repository should implement vertically, not by creating every abstraction first.

### M0 — single owned Run

- supervisor;
- local IPC;
- direct non-interactive process start;
- Run ID/lifecycle;
- inspect/await;
- bounded stdout/stderr;
- terminal receipt;
- normal/nonzero/startup-failure tests.

### M1 — process-tree ownership

- descendant ownership;
- signal/cancel;
- graceful/forced termination;
- no surviving descendant after proven terminal cancellation;
- Linux and Windows backend foundations.

### M2 — telemetry and budgets

- time-series CPU/memory/process telemetry;
- process-count/memory/wall-time limits;
- limit events;
- resource receipt aggregation.

### M3 — interactive workloads

- PTY/ConPTY;
- attach/detach;
- stdin/resize;
- bounded interactive spool.

### M4 — durability

- transactional store;
- client-disconnect detached Runs;
- supervisor restart reconciliation;
- uncertain-state handling;
- lease-bound lifetime.

### M5 — AI workload dogfood

Without adding agent semantics:

- launch a real coding-agent CLI as a Run;
- observe its descendant tool processes;
- exercise telemetry under test/build workload;
- demonstrate memory/process limits;
- correlate a caller-supplied opaque tool/task reference;
- prove terminal cleanup.

## 22. Verification strategy

Initial development can proceed without CI, but the implementation must build deterministic local verification from the start.

Required test layers:

- pure lifecycle/protocol tests;
- backend unit tests;
- real child/grandchild process integration;
- output flood/backpressure tests;
- timeout/termination races;
- Linux cgroup/PTY capability tests where available;
- existing Windows tests must continue to compile where shared contracts change; Windows real-machine certification is outside Wave 2;
- supervisor crash/restart reconciliation;
- process identity/PID reuse defensive cases;
- limit enforcement;
- event gap/compaction;
- multi-client attach/await/cancel concurrency.

Environment-unavailable OS proofs are BLOCKED/UNSUPPORTED, never silently passed through mocks.

## 23. What not to implement in V1

Do not add:

- HTTP/cloud control;
- distributed supervisors;
- Kubernetes/container scheduler integration;
- repository discovery;
- worktree management;
- GitHub integration;
- LLM provider SDKs;
- agent-specific parsing;
- test-output parsing;
- automatic retries/restarts;
- workflow DAGs;
- semantic idle/stuck classification;
- persistent arbitrary environment snapshots.

These belong to higher layers or require a separate architecture decision.


## 24. Current implementation baseline

At the documentation review HEAD `0d154cac`, the initial architecture has been implemented beyond the original package sketch. The current runtime topology is:

```text
CLI / local client
      |
      v
Supervisor
  |-- bbolt Run/event/output metadata
  |-- local IPC
  |-- reconciliation
  |-- host-envelope admission/status
  |-- bounded event/output subscriptions
  |
  +-- per-Run Guardian
        |-- durable descriptor/status
        |-- bounded output ring
        |-- bounded physical telemetry snapshots
        |-- deadline/lease enforcement
        |
        +-- Linux backend
              |-- cgroup v2 when delegated
              |-- session/subreaper fallback
              |-- pidfd-validated signalling
              +-- PTY
```

Wave 2 evolves this implementation rather than recreating the original package proposal. The initial audit findings were recorded against `main@06d34423b1a466c828ad69eab5794248555527ea`; they are historical baseline findings, not a list of current open defects. Track status at this review HEAD is:

| Issue / track | Status | Current implementation and remaining acceptance work |
| --- | --- | --- |
| #4 / A — canonical contract | Complete | Typed/versioned event vocabulary, process/task distinction, frozen per-Run effective capabilities, aggregate stdout/stderr/PTY retention, fast-terminal receipt preservation, deadline ownership, and Linux state/shutdown hardening are in the shared/runtime code. |
| #5 / B — idempotent control | Complete | Run submissions bind caller identity to accepted-spec digest; control mutations use request identity and current generation where retry duplication matters. |
| #6 / C — physical evidence | Partial | Linux Guardian/model and bounded store primitives carry process identity/evidence and time-series telemetry. The supervisor still writes high-rate `resource.sample` events into the lifecycle journal, and no end-to-end supervisor protocol query/client telemetry acceptance path is complete. |
| #7 / D — host safety envelope | Complete | Linux workload envelope configuration, admission checks, capability/status projection, and active-Run/memory/task ceilings are implemented. Actual cgroup delegation and kernel pressure availability remain host-dependent. |
| #8 / E — subscriptions and input writer | Partial | Bounded event/output follow and all-Run watch, reconnectable cursors, and writer leases are implemented. The CLI attach path still polls output/terminal state and subscription acceptance is not complete. |
| #9 / F — operational lifetime controls | Pending | The runtime contract in [WAVE2.md](WAVE2.md) remains the requirement. The issue is still open; no end-to-end completion is claimed for terminal GC/status usage, capability-gated pause/resume and mutable controls, or Guardian-loss recovery. |

Windows remains experimental and frozen; shared type changes are compile maintenance only. No manual real-machine certification was performed for this documentation pass. Automated tests do not establish cgroup delegation, PSI visibility, or other kernel-specific capability on a deployment host; those must remain unsupported/blocked unless actual runtime discovery and the target environment prove them.

## 25. Wave 2 implementation programme

Wave 2 requirements are defined in [WAVE2.md](WAVE2.md). It is a Linux-first hardening and evidence programme with six tracks:

1. canonical alignment and correctness closure;
2. durable/idempotent control;
3. process and resource evidence;
4. host safety envelope;
5. event-driven observation and interactive control;
6. lifecycle operations and bounded retention.

The tracks may be implemented in parallel after shared protocol/model contracts are stabilized. Linux behavior is authoritative. Windows work is limited to keeping shared-code compilation coherent.
