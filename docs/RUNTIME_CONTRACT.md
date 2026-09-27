# Jinushi Runtime Contract

This document freezes the initial external semantics of the Jinushi runtime. Exact CLI spelling and internal Go APIs may evolve while preserving these meanings.

## 1. Run request

A Run request describes one physical execution. It does not contain task semantics.

Illustrative machine shape:

```json
{
  "protocolVersion": 1,
  "argv": ["codex", "exec", "..."],
  "cwd": "/absolute/worktree",
  "environment": {
    "mode": "inherit-supervisor",
    "set": {},
    "unset": []
  },
  "interactive": true,
  "lifetime": {
    "mode": "detached"
  },
  "limits": {
    "memoryBytes": 8589934592,
    "processCount": 64,
    "wallTimeMs": 14400000
  },
  "parentRunId": null,
  "correlation": {
    "owner": "opaque-caller-reference"
  }
}
```

The serialized shape above is illustrative until implementation freezes the schema. The semantic rules below are normative.

### argv

- argv is an ordered non-empty string array.
- Jinushi executes argv directly.
- Jinushi never joins argv into a shell command.
- A caller that intentionally wants a shell must explicitly execute that shell as argv.

### cwd

- cwd is explicit.
- Jinushi does not discover a repository or worktree.
- Jinushi does not infer authority from cwd.
- Failure to enter cwd is a startup failure and no successful Run start is reported.

### environment

Environment behavior is explicit.

Initial modes may include:

- inherit supervisor environment, then apply bounded set/unset operations;
- replace with an explicitly supplied environment.

Normal Run observation never returns environment values. If environment identity is needed for evidence, it must use a secret-safe derived identity rather than raw values.

### interactive

Interactive Runs request PTY/ConPTY semantics. Non-interactive Runs use ordinary stdio pipes.

### lifetime

A lifetime policy is mandatory or has one documented deterministic default. It is never inferred from whether the initiating CLI stays connected.

Initial modes:

- `detached`: Run survives client disconnect.
- `lease-bound`: Run survives while a valid lease is maintained and begins termination when the lease expires.

### limits

Limits are optional individually, but when accepted as supported they become runtime enforcement obligations.

A backend must not accept a limit it cannot enforce while reporting it as enforced.

## 2. Run identity

On acceptance, Jinushi returns an opaque Run ID.

Example:

```json
{
  "runId": "run_01...",
  "state": "accepted"
}
```

A Run ID:

- is not a PID;
- is stable across client reconnect;
- is stable across supervisor reconciliation;
- has no embedded task/repository semantics;
- is the only public destructive-control target.

PIDs may be returned as observations but cannot be used in place of a Run ID for control operations.

## 3. Run lifecycle

The minimum lifecycle vocabulary is:

| State | Meaning |
| --- | --- |
| `accepted` | Request durably accepted; external process establishment not yet proven |
| `starting` | Runtime is attempting OS execution establishment |
| `running` | Owned process execution is proven live |
| `terminating` | Termination requested; tree termination not yet proven |
| `reconciling` | Runtime restart or observation gap is being reconciled |
| `terminal` | Physical execution outcome is final |
| `uncertain` | Runtime cannot prove live or terminal ownership/outcome |

A richer internal state machine may distinguish startup failure, timeout, cancellation, and different terminal causes. Those causes must remain machine-readable.

## 4. Terminal outcome

Terminal outcome is separate from lifecycle state.

Initial outcome classes should include:

- `exited`;
- `signaled`;
- `startup-failed`;
- `timed-out`;
- `cancelled`;
- `resource-limit`;
- `forced-termination`;
- `ownership-lost` or equivalent explicit uncertainty classification when a final physical outcome cannot be proven.

Exit code zero is only a physical process fact. It does not mean semantic task success.

## 5. Observation

`inspect(runId)` returns one bounded current physical snapshot.

Illustrative result:

```json
{
  "runId": "run_01...",
  "state": "running",
  "generation": 4,
  "startedAt": "2026-09-27T03:00:00Z",
  "processes": {
    "observed": 7
  },
  "activity": {
    "lastOutputAt": "2026-09-27T03:04:11Z",
    "lastCpuActivityAt": "2026-09-27T03:04:12Z",
    "lastProcessChangeAt": "2026-09-27T03:03:59Z"
  },
  "resources": {
    "memoryBytes": 612368384,
    "peakMemoryBytes": 1308622848,
    "cpuTimeNs": 81930112000,
    "supported": ["memory", "cpu", "process-count"]
  }
}
```

Unsupported or unavailable observations are explicit. They are not represented as zero.

## 6. Event stream

Each Run owns an ordered event stream.

Each event contains at least:

- protocol/event schema version;
- Run ID;
- monotonically increasing sequence;
- event kind;
- observed timestamp;
- bounded event body.

Illustrative event:

```json
{
  "version": 1,
  "runId": "run_01...",
  "seq": 42,
  "kind": "resource.sample",
  "observedAt": "2026-09-27T03:04:12Z",
  "resource": {
    "memoryBytes": 612368384,
    "processCount": 7
  }
}
```

Sequence, not wall-clock time, is the per-Run ordering authority.

Consumers can request events after a sequence. If requested history has already been compacted, Jinushi returns an explicit retained-from watermark/gap rather than pretending continuity.

## 7. Output contract

Output is both streamable and boundedly retained.

For stdout/stderr Jinushi records:

- total observed bytes;
- retained bytes;
- truncation/compaction state;
- sequence/window information needed to detect lost historical output.

A slow reader must not force Jinushi to retain unbounded memory.

The implementation may spool to disk and stream from the spool. Retention policy is explicit and bounded.

Jinushi does not parse output.

## 8. PTY contract

Interactive Runs expose:

- attach;
- detach;
- stdin/input write;
- terminal resize;
- PTY output stream;
- attachment state.

Detach does not imply termination.

A client can reconnect to an existing interactive Run while it remains live.

PTY attachment is physical evidence only. Jinushi does not infer whether an agent is awaiting human input.

## 9. Resource telemetry

Telemetry is retained as bounded time-series execution evidence.

Sampling may combine periodic sampling and OS-native event notification. The contract must preserve:

- sampling/observation timestamp;
- supported metric set;
- values;
- gaps/unavailable periods;
- aggregation used in the final receipt.

Initial metrics:

- current memory;
- peak memory;
- CPU time/delta;
- process count;
- output byte counts;
- process membership change;
- wall time.

Where meaningful and stable, platform backends may add I/O and other resource metrics behind capability discovery.

## 10. Resource limits

Limits are accepted only when the selected backend can implement their declared enforcement semantics.

Initial limits:

### Memory

Run/process-tree memory ceiling where the OS can enforce it.

### CPU

CPU quota/rate where the OS can enforce it. CPU time telemetry can still be available when a hard CPU quota is unsupported.

### Process count

Maximum owned process count.

### Wall time

Maximum Run lifetime before timeout termination begins.

### Output retention

Maximum retained output. This controls evidence retention/backpressure, not necessarily process output production.

When a hard limit is exceeded, Jinushi emits a typed `limit.reached` event and begins the configured termination sequence.

## 11. Termination contract

Termination is a stateful operation, not a one-shot `kill(pid)`.

The normal sequence is:

1. record termination request and reason;
2. prevent conflicting lifecycle transitions;
3. deliver graceful termination to the owned tree where configured;
4. wait a bounded grace interval;
5. escalate if policy allows;
6. re-observe the owned tree;
7. declare terminal only after termination is proven;
8. otherwise classify uncertainty truthfully.

A Run control operation never targets a caller-supplied arbitrary PID.

## 12. Signals

Signal vocabulary is normalized at the public contract and mapped by the OS backend.

Platform-specific unsupported signals fail explicitly.

Signal delivery never implies successful process-tree termination.

## 13. Cancellation

Cancellation is a caller intent that invokes the Run's termination policy.

Cancellation is not semantic task cancellation. Jinushi only terminates the physical execution.

Repeated cancellation is idempotent with respect to one Run generation: it cannot spawn replacement work or duplicate destructive cleanup.

## 14. Await

`await(runId)` blocks/streams until one of:

- terminal outcome;
- uncertainty requiring caller attention;
- explicit timeout/cancellation of the await operation.

Await timeout does not terminate the Run unless a separate Run lifetime/termination policy says so.

This distinction lets orchestrators wait without owning Run lifetime.

## 15. Lease

A lease-bound Run has:

- lease identity/generation;
- expiry;
- renewal operation;
- bounded grace/termination policy.

Renewal is compare-and-advance/idempotent against the expected lease generation so stale renewers cannot silently extend a newer execution policy.

Detached Runs have no implicit lease.

## 16. Lineage and correlation

`parentRunId` expresses causal execution lineage.

Opaque bounded correlation labels may let callers connect a Run to an external semantic identity.

Jinushi:

- stores them;
- returns them;
- does not interpret them;
- does not use them as authorization.

This is the primary seam for later tool-event/resource correlation.

## 17. Durability and reconciliation

Before returning an accepted Run identity, enough durable state must exist to distinguish:

- request accepted but process not started;
- process established;
- final outcome.

On runtime restart, every non-terminal Run is reconciled against the OS backend.

The only legal results are supported evidence-based transitions such as:

- still running;
- terminal with proven outcome;
- uncertain.

Jinushi never auto-relaunches a missing process as recovery.

## 18. Terminal receipt

A terminal receipt is immutable execution evidence.

Illustrative shape:

```json
{
  "version": 1,
  "runId": "run_01...",
  "outcome": "exited",
  "exitCode": 0,
  "signal": null,
  "startedAt": "2026-09-27T03:00:00Z",
  "finishedAt": "2026-09-27T03:10:13Z",
  "resource": {
    "peakMemoryBytes": 1308622848,
    "cpuTimeNs": 81930112000,
    "peakProcessCount": 12
  },
  "output": {
    "stdoutObservedBytes": 188416,
    "stderrObservedBytes": 2048,
    "historyComplete": true
  },
  "termination": {
    "requested": false,
    "forced": false
  },
  "cleanup": {
    "processTree": "complete"
  }
}
```

The final schema must carry explicit incompleteness/unsupported states where applicable.

## 19. Capability discovery

Clients must be able to discover runtime/backend capabilities before creating a Run.

Examples:

- PTY;
- memory enforcement;
- CPU quota enforcement;
- process-count enforcement;
- time-series memory/CPU;
- restart reconciliation strength;
- supported signal set.

A client can therefore choose whether an unavailable capability is acceptable. Jinushi never silently weakens an accepted requirement.

## 20. Failure model

Stable failure categories should distinguish at least:

- invalid request;
- unsupported capability;
- supervisor unavailable;
- startup failure;
- cwd/environment preparation failure;
- stale Run generation;
- Run not found;
- Run already terminal;
- ownership uncertainty;
- I/O retention/stream gap;
- backend failure;
- storage/durability failure.

Failures are bounded and must not include raw environment secrets.

## 21. CLI projection

The CLI is a thin client over the local runtime protocol.

Machine JSON is the authoritative automation surface. Human-readable output is a projection.

Expected domains:

```text
jinushi run
jinushi list
jinushi inspect
jinushi await
jinushi events
jinushi attach
jinushi signal
jinushi cancel
jinushi capabilities
jinushi status
```

A resident supervisor command/service entry point is also required, but its exact lifecycle UX is implementation detail until the first executable milestone.

## 22. Configuration

Runtime configuration belongs to the resident supervisor, not to individual semantic products.

Initial configuration concerns:

- state/storage root;
- local IPC endpoint;
- event/output retention bounds;
- telemetry sampling bounds;
- termination grace defaults;
- backend capability policy;
- maximum runtime-wide safety ceilings.

Per-Run requests can narrow allowed limits but must not exceed supervisor-wide maxima.

Configuration contains no repository, Issue, PR, model, or agent policy.
