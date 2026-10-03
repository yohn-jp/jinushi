# Jinushi

**Native execution ownership for durable, observable, resource-bounded process trees.**

Jinushi is a local execution substrate. It turns an external command into an owned **Run** and remains responsible for that Run's physical process tree, I/O, resource use, lifecycle, termination, and execution evidence until the outcome is known.

Jinushi is deliberately below agent semantics. It does not know whether a process is Codex, Pi, OpenCode, a compiler, or a test runner. It does not understand Issues, prompts, tools, repositories, or task success. To Jinushi, every workload is an executable plus an explicit execution request.

## Why Jinushi

Long-lived coding-agent workloads expose a gap between ordinary process spawning and orchestration:

- a caller can disappear while work should continue;
- children and grandchildren can outlive their launcher;
- timeouts can leave descendants behind;
- output can exhaust memory or disappear before diagnosis;
- CPU, memory, process-count, and activity spikes are difficult to attribute to one execution;
- an orchestrator should react to typed state changes instead of polling;
- a host should be protected by explicit execution budgets before one workload exhausts the machine.

Jinushi owns that physical execution boundary.

## Core contract

A caller submits an execution request. Jinushi creates a durable Run identity and owns the resulting process tree.

```text
client / orchestrator / CLI
            |
            | local protocol
            v
+--------------------------------------+
| Jinushi                              |
|                                      |
| Run identity + lifecycle             |
| process-tree ownership               |
| stdin / stdout / stderr / PTY        |
| bounded spool + backpressure         |
| resource telemetry + limits          |
| event journal                        |
| signals / cancel / termination       |
| restart reconciliation               |
| terminal receipt                     |
+-------------------+------------------+
                    |
                    v
                    OS
```

The CLI is a client of the same local runtime protocol; it is not the runtime's semantic authority.

## Run it locally

Build with Go 1.26 or newer, then start one resident supervisor in a separate terminal:

```sh
go build -o jinushi ./cmd/jinushi
./jinushi supervisor --state-dir ./jinushi-state
```

Submit a command from another terminal. The JSON response contains the stable `runId`; use it for observation and control:

```sh
./jinushi run --state-dir ./jinushi-state --submission-id run-unique-1 -- /bin/sh -c 'printf hello'
./jinushi await --state-dir ./jinushi-state <run-id>
./jinushi output --state-dir ./jinushi-state <run-id>
./jinushi events --state-dir ./jinushi-state <run-id>
./jinushi capabilities --state-dir ./jinushi-state
./jinushi status --state-dir ./jinushi-state
```

Use a fresh caller-generated `--submission-id` for each new Run and reuse that same ID when retrying an ambiguous submission. `run --wait` waits for the physical terminal receipt. Closing a client does not cancel a detached Run. Hard resource limits are accepted only when `capabilities` reports native enforcement on that host. Linux is the Wave 2 authority; the existing Windows backend is experimental and frozen, with no Wave 2 parity or certification claim.

The supervisor reads optional `config.json` from its state directory at startup. Omitted fields use built-in defaults; invalid or unknown fields prevent startup. For example:

```json
{
  "sampleIntervalMs": 250,
  "terminationGraceMs": 2000,
  "defaultOutputBytes": 1048576,
  "maxOutputBytes": 67108864,
  "eventRetentionCount": 4096,
  "eventRetentionBytes": 16777216,
  "hostMemoryBytes": 0,
  "hostTaskCount": 0,
  "maxActiveRuns": 0,
  "retentionIntervalMs": 60000,
  "retention": {
    "maxAgeMs": 2592000000,
    "maxTerminalRuns": 10000,
    "maxStateBytes": 536870912,
    "preserveTombstones": true,
    "maxTombstones": 100000,
    "maxTombstoneAgeMs": 2592000000,
    "compactMinFreeBytes": 67108864,
    "compactMinFreeRatio": 0.25
  }
}
```

The three optional host envelope settings default to zero, which disables that ceiling. `status` reports host envelope capability and current aggregate workload evidence along with store usage, retention counters, and bbolt compaction state. The `retention` policy applies age/count/logical-byte limits to terminal Run evidence and retained tombstones; `maxStateBytes` is not a physical database-file limit. The database and its logical contents are both reported by `status`. Other per-Run supervisor ceilings are `maxWallTimeMs`, `maxMemoryBytes`, `maxProcessCount`, and `maxTaskCount`; Run requests may narrow them. On Linux, `--task-count` maps to cgroup v2 `pids.max` when delegated; `--process-count` is rejected because that controller counts threads as well as processes. The state directory is local private runtime data, and environment values are not returned by normal Run observation.

The per-Run output-retention limit is one aggregate byte ceiling shared by stdout, stderr, and PTY output. Retained offsets and gaps show when old bytes were compacted.

Representative public operations:

```text
jinushi run --submission-id ID -- <executable> <args...>
jinushi list
jinushi inspect <run-id>
jinushi await <run-id>
jinushi events <run-id>
jinushi output <run-id>
jinushi watch [--cursor CURSOR] [--follow]
jinushi attach <run-id>
jinushi signal --request-id ID --expected-generation N <run-id> <signal>
jinushi cancel --request-id ID --expected-generation N <run-id>
jinushi pause --request-id ID --expected-generation N <run-id>
jinushi resume --request-id ID --expected-generation N <run-id>
jinushi memory-high --bytes N --request-id ID --expected-generation N <run-id>
jinushi cpu-quota --percent N --request-id ID --expected-generation N <run-id>
jinushi lease renew --generation N --lease-ms N <run-id>
jinushi input --request-id ID --expected-generation N <run-id> <text>
jinushi close-input --request-id ID --expected-generation N <run-id>
jinushi resize --rows N --cols N --request-id ID --expected-generation N <run-id>
jinushi capabilities
jinushi status
```

Run submission and non-idempotent physical mutations use caller-provided retry identities/current generation. `watch` provides bounded all-Run event pages and reconnectable cursors; event/output/PTY attach follow modes use runtime notifications. Telemetry follow is available over the local protocol. Pause, resume, and mutable resource controls are accepted only when the Run's effective backend capabilities permit them.

### Nix

The repository root is a Nix flake for `x86_64-linux` and `aarch64-linux`. It builds Jinushi from repository source with `buildGoModule`; `flake.lock` pins nixpkgs.

```sh
nix build .                                  # ./result/bin/jinushi
nix run . -- --help
nix run . -- supervisor --state-dir ./jinushi-state
nix profile install .
```

The same commands work from a GitHub reference:

```sh
nix build github:yohn-jp/jinushi
nix run github:yohn-jp/jinushi -- --help
nix profile install github:yohn-jp/jinushi
```

## What Jinushi observes

Jinushi records physical execution facts, including:

- Run and process identities;
- lifecycle transitions;
- child/descendant process membership;
- exit code and signal;
- stdout/stderr/PTY activity and retained-byte bounds;
- wall time, CPU consumption, memory use, process count, and Linux task/PID count as separate metrics;
- process-level CPU/RSS and membership evidence where the backend exposes it, without raw argv or environment;
- I/O and PSI pressure observations where the kernel exposes them;
- last physical activity evidence;
- applied resource limits and limit-triggered termination;
- explicit signals, cancellation, forced termination, and cleanup outcome;
- a terminal receipt describing the final physical execution outcome.

Resource telemetry is time-series evidence, not only a final peak. High-rate samples are stored separately from the lifecycle/control journal. The local protocol's `telemetry` operation returns bounded raw samples and coarser aggregates with process identity/evidence, I/O/PSI values where supported, resolution, retained ranges, and gaps. Set `follow` with a `telemetryQuery` to receive wakeup-driven bounded pages; `watermark` is the cursor represented by a response, while `nextCursor` is present when more points remain. Notifications coalesce per Run, and a telemetry cursor made stale by compaction returns an explicit error. There is no dedicated telemetry CLI command. Output byte totals and elapsed wall time are available through Run/output events and the terminal receipt rather than the high-rate telemetry sample itself.

Terminal retention runs automatically on supervisor startup and at `retentionIntervalMs`. It only collects terminal Runs with a complete receipt. When configured to preserve tombstones, later `inspect` and `await` calls return the compact terminal outcome and an explicit incomplete-evidence marker after detailed evidence has been removed.

## AI-native without agent semantics

Jinushi is designed for long-lived agent workloads but does not interpret them.

A future agent adapter may correlate semantic tool events with Jinushi Run/event identities:

```text
agent/tool event                Jinushi evidence
----------------                ----------------
tool call: tests   -----------> child Run/process tree
                                CPU/RSS spike
                                output bytes
                                duration
                                terminal outcome
```

Jinushi itself does not decide what the tool call meant, whether a task succeeded, or whether repeated work should be deduplicated.

## Product boundaries

```text
Mottainai
  orchestration: what/when/whether to run
        |
        v
Nawabari
  workspace / repository / execution authority
        |
        v
Jinushi
  physical execution ownership
        |
        v
Agent or arbitrary process
```

The sequence is compositional, not a mandatory dependency. Jinushi is independently usable. In a governed agent flow, a workspace and authority can be established by Nawabari first, then Jinushi starts and owns the allowed process in that prepared environment.

See:

- [Architecture](docs/ARCHITECTURE.md)
- [Runtime contract](docs/RUNTIME_CONTRACT.md)
- [Implementation plan](docs/IMPLEMENTATION.md)
- [Wave 2 — Linux hardening and evidence runtime](docs/WAVE2.md)

## Explicit non-goals

Jinushi does **not**:

- understand Codex, Pi, OpenCode, or any other agent protocol;
- own prompts, tasks, models, or LLM providers;
- schedule a workflow or choose the next unit of work;
- create Git worktrees, branches, commits, or repository claims;
- decide filesystem/repository authorization;
- understand GitHub Issues, PRs, reviews, or CI semantics;
- judge whether an agent's semantic work succeeded;
- perform code review, merging, or approval;
- require tools invoked by an agent to be child processes.

A harness may choose to route tool execution through Jinushi to make those executions independently owned and observable, but that is a harness/orchestration policy rather than Jinushi semantics.

## Initial implementation direction

Jinushi is implemented in Go. Linux is the primary supported development target. The existing Windows backend is retained as an experimental, frozen implementation; Wave 2 does not require Windows feature parity or new Windows capability work.

The initial product milestone includes:

1. resident local supervisor;
2. durable Run identities and lifecycle;
3. process-tree ownership and descendant cleanup;
4. bounded stdio and PTY operation;
5. append-only typed execution events;
6. time-series resource telemetry;
7. enforceable resource budgets;
8. explicit Run lifetime/lease policy;
9. client disconnect tolerance;
10. supervisor restart reconciliation with fail-closed uncertain state;
11. terminal execution receipts;
12. local machine-readable control protocol and CLI.

No CI or product integration is required to define this initial architecture.
