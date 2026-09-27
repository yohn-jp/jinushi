# Jinushi Architecture

## 1. Purpose

Jinushi is the physical execution-ownership layer for local workloads.

Its unit of responsibility is a **Run**: one explicitly requested execution plus every process that belongs to that execution. Jinushi creates a stable Run identity, starts the workload, owns and observes its process tree, enforces declared resource limits, retains bounded execution evidence, and drives the Run to a truthful terminal state.

The architecture is intentionally process-centric rather than agent-centric. Agent runtimes are important initial consumers, but no agent-specific concept is part of Jinushi Core.

## 2. Architectural position

The intended yohn-jp composition is:

```text
Mottainai
  task semantics, readiness, orchestration, dispatch
        |
        | requests governed execution
        v
Nawabari
  repository/worktree/session/resource authority
        |
        | yields an allowed execution context
        v
Jinushi
  process execution ownership, resources, I/O, lifecycle
        |
        | starts and owns
        v
Codex / Pi / OpenCode / compiler / tests / any executable
```

These products remain independently usable.

### Mottainai

Mottainai answers semantic orchestration questions:

- what work is ready;
- which workload should run;
- when capacity should be filled;
- which result needs human/model judgment;
- how Run events relate to task/agent state.

Jinushi never schedules work.

### Nawabari

Nawabari answers repository and execution-authority questions:

- which repository/worktree/session is owned;
- what repository resources may be accessed or mutated;
- whether a protected execution is admitted;
- which filesystem/Git authority is effective.

Jinushi never creates or infers that authority. It executes in the explicit environment/cwd provided by its caller.

### Agent adapters

Agent-specific adapters may understand Codex/Pi/OpenCode session IDs, tool events, model events, prompts, or semantic phases. They can attach bounded correlation identities to Jinushi Runs.

Jinushi treats that metadata as opaque correlation data.

## 3. Core architectural rule: Execution Ownership

Jinushi must provide one invariant:

> Once a Run is accepted, Jinushi remains the physical owner of its execution until the Run reaches a truthful terminal outcome or is explicitly classified as uncertain.

Ownership includes:

- process-tree identity;
- execution lifecycle;
- process termination;
- I/O transport and bounded retention;
- resource observation;
- resource-limit enforcement;
- client attach/detach;
- event ordering;
- crash/restart reconciliation;
- terminal evidence.

A PID is never the public Run identity.

## 4. Component topology

```text
+-----------------------------------------------------------+
| Clients                                                   |
| CLI | Mottainai | Suzukuri | agent adapters | other apps |
+-----------------------------+-----------------------------+
                              |
                        local protocol
                              |
                              v
+-----------------------------------------------------------+
| Jinushi Supervisor                                        |
|                                                           |
|  Control API        Run Registry       Event Journal       |
|  ----------         ------------       -------------       |
|  create/run         durable Run state  ordered events      |
|  inspect/list       generation/state   replay watermark    |
|  await/events       lifetime policy    bounded retention   |
|  signal/cancel      final receipt      subscriptions       |
|                                                           |
|  I/O Manager        Resource Engine    Reconciler          |
|  -----------        ---------------    ----------          |
|  stdio/PTY          telemetry          startup recovery    |
|  spool              budgets            OS re-observation  |
|  backpressure       limit events       uncertain states    |
+------------------------------+----------------------------+
                               |
                         OS backend port
                 +-------------+-------------+
                 |                           |
                 v                           v
        +----------------+          +----------------+
        | Linux backend  |          | Windows backend|
        | process groups |          | Job Objects    |
        | cgroup v2      |          | ConPTY         |
        | PTY            |          | process metrics|
        +----------------+          +----------------+
```

The exact internal package structure may evolve, but these responsibility boundaries must remain distinct.

## 5. Run identity and lineage

Every accepted execution receives an opaque stable Run ID.

A Run may optionally reference another Run as its parent for causal lineage. This lineage describes execution origin only; it is not OS parentage and does not grant authority.

Example:

```text
agent Run A
  |
  +-- semantic adapter decides to externalize a tool call
        |
        +-- Jinushi Run B (parentRunId = A)
              |
              +-- test runner process tree
```

This allows later correlation of physical resource use without making Jinushi understand the tool's semantic meaning.

### Run identity requirements

A Run ID must:

- be collision-resistant;
- survive client disconnect;
- remain stable across supervisor restart/reconciliation;
- never be derived only from PID;
- never imply task, repository, or agent identity.

Optional caller correlation metadata must be bounded and opaque.

## 6. Lifecycle model

The logical lifecycle is:

```text
accepted
   |
starting
   |
running
   |\
   | \----> terminating
   |             |
   |             v
   +---------> terminal

restart / observation ambiguity
   |
   +---------> reconciling
                    |
              running | terminal | uncertain
```

Required states may be represented more precisely in implementation, but the following distinctions are mandatory:

- accepted but not yet spawned;
- startup failure;
- running;
- termination requested but not proven complete;
- normally exited;
- signaled/forced termination;
- resource-limit termination;
- timed out;
- cancelled;
- reconciliation in progress;
- uncertain ownership/outcome.

Unknown state must never be silently projected as exited or successful.

## 7. Lifetime policy

Caller lifetime and Run lifetime are independent.

A Run request declares an explicit lifetime policy.

Initial policies:

### Detached

The Run survives client disconnect and continues until its process tree terminates or another authorized client cancels it.

This is the normal policy for long-running agent work.

### Lease-bound

The caller or orchestrator must renew a bounded execution lease. Lease expiry causes the declared termination procedure to begin.

Lease semantics are physical execution policy only. They do not represent task ownership or authorization.

The runtime must record:

- lease generation;
- expiry;
- renewal;
- expiry-triggered termination;
- final cleanup evidence.

No implicit "caller disconnected, therefore kill" behavior is allowed unless that policy was explicitly selected.

## 8. Process-tree ownership

Jinushi owns the entire process tree belonging to a Run, not only the initial PID.

The backend must provide the strongest available OS-native ownership primitive.

### Linux

Preferred mechanisms:

- a dedicated cgroup v2 scope for membership, telemetry, and enforcement where available;
- process group/session ownership for signal delivery;
- PTY ownership where interactive;
- explicit descendant reconciliation.

### Windows

Preferred mechanisms:

- Job Objects for process membership, limits, and termination;
- ConPTY for interactive terminal workloads;
- Windows-native process accounting.

Implementation must not claim equal enforcement on a platform when the underlying OS capability is unavailable.

## 9. Durability and restart reconciliation

There are two separate durability guarantees.

### Client disconnect durability

Mandatory.

Once accepted under a detached lifetime policy, a Run continues when the initiating CLI/client exits.

### Supervisor restart durability

A product-level target.

Durable state must be persisted before external effects are reported as established. On supervisor restart, Jinushi re-observes the OS and reconciles each non-terminal Run.

The implementation may use different backend mechanisms per OS, including an internal per-Run guardian where required. The public contract is platform-neutral:

- a safely rediscovered execution resumes as `running`;
- a proven completed execution becomes terminal;
- an execution whose ownership/outcome cannot be proven becomes `uncertain`;
- Jinushi never recreates or restarts a workload merely because state is missing.

"Persistence" means persistence of execution ownership/evidence, not semantic agent-session resume.

If Codex/Pi/OpenCode itself exits, Jinushi records that outcome. It does not restart or resume that agent unless an upper layer submits a new Run.

## 10. I/O and PTY

Jinushi owns physical process I/O.

V1 supports:

- non-interactive stdin/stdout/stderr;
- interactive PTY/ConPTY;
- attach/detach;
- explicit stdin write;
- window-size update where PTY supports it;
- bounded persistent output spool;
- ordered output events;
- observed byte counts even when retention truncates old content;
- explicit truncation/watermark metadata.

Backpressure is mandatory. An unbounded producer must not exhaust supervisor memory.

Output retention is evidence transport, not semantic logging. Jinushi does not parse compiler, test, or agent output.

## 11. Resource telemetry

Resource telemetry is a first-class execution surface, not an optional diagnostic.

At minimum, where supported, Jinushi records time-series observations for:

- wall-clock elapsed time;
- process count;
- current and peak memory/RSS;
- CPU consumption/delta;
- output bytes;
- process-tree membership changes;
- physical activity timestamps.

Additional platform metrics may be exposed when their meaning is stable.

The runtime must distinguish:

- supported measured value;
- unsupported metric;
- temporarily unavailable observation.

Zero is never used as a substitute for unavailable evidence.

### Activity evidence

Jinushi may expose physical activity such as:

- recent CPU change;
- recent output;
- process spawn/exit;
- PTY attachment;
- recent stdin write.

It must not label an agent as "thinking", "waiting for input", "stuck", or "finished task" unless that meaning is supplied by an external semantic adapter.

## 12. Resource budgets and enforcement

A Run may declare enforceable budgets.

Initial budget classes:

- memory;
- CPU quota/rate where supported;
- process count;
- wall-clock timeout;
- retained/output byte bounds.

Budget policy is caller-selected. Enforcement is Jinushi-owned once the Run is accepted.

When a limit is reached:

1. emit a typed limit event;
2. execute the configured bounded termination policy;
3. continue observing until process-tree termination is proven or classified uncertain;
4. record the limit in the terminal receipt.

A host-wide adaptive scheduler is out of scope. Jinushi enforces per-Run limits; Mottainai may make higher-level capacity decisions using Jinushi telemetry.

## 13. Event model

Jinushi exposes an ordered event stream per Run.

Representative event families:

- `run.accepted`;
- `run.starting`;
- `run.started`;
- `process.observed`;
- `process.exited`;
- `io.stdout`;
- `io.stderr`;
- `pty.attached` / `pty.detached`;
- `resource.sample`;
- `limit.reached`;
- `signal.sent`;
- `termination.requested`;
- `run.reconciling`;
- `run.terminal`;
- `run.uncertain`.

Each event has a monotonic per-Run sequence number. Wall-clock timestamps are observations, never ordering authority by themselves.

The event journal is bounded. If history is compacted, the API exposes the retained sequence watermark so consumers can detect the gap.

## 14. Terminal receipt

Every terminal Run has one final immutable execution receipt containing enough physical evidence to answer:

- what executable/argv identity was accepted;
- when execution started and ended;
- how it terminated;
- whether termination was requested/forced/limit-triggered;
- aggregate resource usage;
- process-tree cleanup result;
- stdout/stderr observed and retained byte counts;
- event range;
- platform/backend capability summary;
- whether any evidence is incomplete.

The receipt does not state semantic task success.

## 15. Local protocol

Jinushi is local-first.

V1 does not expose a network service. Preferred transports:

- Unix domain socket on Linux;
- named pipe on Windows.

The protocol must support:

- request/response machine JSON;
- streaming ordered events;
- bounded payloads;
- protocol version/capability discovery;
- stable typed failures;
- concurrent clients.

The CLI and future language clients use the same protocol.

Machine output and diagnostics remain separable; human presentation is a projection.

## 16. Security and trust boundary

Jinushi is not a sandbox or authorization engine by default.

It must nevertheless enforce its own execution-safety invariants:

- direct argv execution; no implicit shell interpolation;
- explicit cwd;
- explicit environment policy;
- no environment values echoed in normal observations;
- bounded input and output payloads;
- local-only control transport;
- caller identity/permission appropriate to the local OS transport;
- no PID-only destructive action;
- signal/cancel targets a Jinushi Run identity and revalidated owned process tree;
- stale/reused OS identities fail closed;
- uncertain ownership prevents destructive claims of success.

Filesystem isolation, Git/repository authorization, network sandboxing, and agent capability policy remain outside Core unless explicitly introduced as separate future architecture.

## 17. Explicit non-goals

Jinushi must not become:

- an agent harness;
- an LLM framework;
- a task scheduler;
- a workflow/DAG engine;
- a repository/worktree manager;
- a Git/GitHub abstraction;
- a test runner;
- a semantic telemetry parser;
- a code-review system;
- a generic container/orchestration platform.

These exclusions are architectural boundaries, not merely V1 omissions.

## 18. AI-native extension seam

The runtime supports AI workloads through generic correlation, lineage, event, and telemetry primitives.

A separate harness may:

- create one Run for each agent session;
- externalize selected tool executions into child Runs;
- attach semantic tool-call IDs to opaque correlation fields;
- correlate resource spikes with tool events;
- detect repeated physical executions across agents;
- use terminal receipts as execution evidence.

Those higher-level interpretations remain outside Jinushi.

This seam is intentional: the runtime should make semantic/physical correlation possible without embedding agent semantics into the execution substrate.
