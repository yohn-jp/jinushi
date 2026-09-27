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

Representative public operations:

```text
jinushi run -- <executable> <args...>
jinushi list
jinushi inspect <run-id>
jinushi await <run-id>
jinushi events <run-id>
jinushi attach <run-id>
jinushi signal <run-id> <signal>
jinushi cancel <run-id>
```

Exact syntax is implementation work. The architecture requires equivalent machine-readable operations and stable Run identities.

## What Jinushi observes

Jinushi records physical execution facts, including:

- Run and process identities;
- lifecycle transitions;
- child/descendant process membership;
- exit code and signal;
- stdout/stderr/PTY activity and retained-byte bounds;
- wall time, CPU consumption, memory use, process count, and I/O observations where the OS exposes them;
- last physical activity evidence;
- applied resource limits and limit-triggered termination;
- explicit signals, cancellation, forced termination, and cleanup outcome;
- a terminal receipt describing the final physical execution outcome.

Resource telemetry is time-series evidence, not only a final peak. This makes it possible to attribute a host-level resource spike to an exact Run and process subtree.

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

Jinushi is implemented in Go and targets Linux and Windows first.

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
