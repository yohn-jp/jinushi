# Wave 2 — Linux hardening and evidence runtime

Wave 2 takes the initial Jinushi runtime from a functionally complete execution supervisor to a Linux-first execution substrate suitable for sustained coding-agent orchestration.

This programme is derived from a code-level audit of `main@06d34423b1a466c828ad69eab5794248555527ea`.

No agent semantics, repository policy, GitHub semantics, scheduling, or workflow logic belongs in this wave.

## Platform posture

Linux is authoritative.

The existing Windows backend remains in the repository as an experimental, frozen implementation. Shared-contract work may keep it compiling, but Wave 2 does not require Windows parity, Windows certification, or new Windows features.

## Audit findings at the original baseline

The initial runtime is already substantial: resident supervisor, bbolt persistence, durable per-Run guardians, Linux cgroup/session ownership, PTY, bounded output, resource sampling, lease/deadline enforcement, restart reconciliation, terminal receipts, and broad Linux integration coverage are present.

The next risks are therefore not missing basic process management. They are correctness under retries, long-lived/high-concurrency evidence, host-level pressure, and operational lifetime.

The code audit identified these concrete items at that baseline; they are historical findings, not a statement that every item remains open on current `main`:

1. Run creation is not idempotent across an ambiguous client retry.
2. Linux `pids.max` task semantics are exposed through a process-count-shaped model.
3. terminal receipts currently derive capabilities from the host at terminal time rather than freezing the effective backend/capabilities used by that Run.
4. output retention is divided into three fixed stream shares, so an interactive PTY Run receives only one third of the requested aggregate retention.
5. wall-time deadline checking exists in both Guardian and Supervisor; the Guardian already has the monotonic lifetime authority.
6. event kind names are not one canonical typed vocabulary across emitters and retention classification.
7. a very fast terminal workload can complete inside the Guardian before Supervisor ownership import; the current short path can lose physical receipt detail.
8. high-rate `resource.sample` records share the lifecycle event journal. At the default 250 ms sampling interval, 4096 events is only about 17 minutes before other event traffic.
9. Guardian and Supervisor both durably write frequent resource state, creating avoidable write amplification for many concurrent long-lived Runs.
10. Linux backend already reads per-process PID/PPID/start/RSS/CPU facts internally, but does not expose bounded process-level evidence.
11. per-Run limits cannot prevent aggregate host exhaustion by many individually valid Runs.
12. terminal Runs have bounded per-Run evidence but no global retention/GC policy; bounded × unbounded Run count remains unbounded.
13. event following and PTY output observation use short polling loops rather than runtime wakeups.
14. multiple PTY observers are possible, but stdin mutation has no exclusive writer ownership.
15. Supervisor restart is handled strongly, but Guardian loss while descendants remain alive needs an explicit Linux fail-closed recovery policy.
16. Linux state/run path handling still deserves stronger replacement/symlink defenses at the execution-runtime trust boundary.

## Track A — Canonical alignment and correctness closure

Goal: make the implementation, protocol, and documents describe one coherent Linux runtime before expanding capability.

Required work:

- make Linux-primary / Windows-frozen posture effective in docs and implementation decisions;
- create canonical typed/versioned event kinds and payloads;
- split task/PID count from process count;
- persist effective per-Run backend/capabilities;
- correct aggregate output-retention semantics for stdio versus PTY;
- make Guardian the authoritative monotonic wall-time/deadline owner;
- preserve full Guardian physical receipt/evidence for fast terminal-before-import Runs;
- close Supervisor shutdown/start goroutine races;
- harden Linux state/run/control paths against symlink/replacement ambiguity;
- update status/receipt/protocol schemas coherently.

Completion gate: no known contract mismatch above remains in shared Linux code.

## Track B — Durable/idempotent control

Goal: a caller can retry ambiguous physical mutations without duplicating execution or input.

Required work:

- caller-supplied bounded submission identity for Run creation;
- immutable accepted-spec digest/binding;
- same submission + same spec returns the existing Run;
- same submission + different spec returns explicit conflict;
- idempotency retention compatible with terminal GC;
- request identity/currentness for non-idempotent control where retry duplication matters, especially input;
- durable behavior across Supervisor restart.

Do not turn submission identity into task identity or authorization.

Completion gate: an ambiguous client response loss can be retried without creating a second physical Run.

## Track C — Physical evidence runtime

Goal: retain enough Linux physical evidence to explain resource spikes without understanding agent semantics.

Required work:

- bounded process evidence with PID+start identity;
- parent/membership observations;
- safe executable/comm identity;
- per-process CPU/RSS observations where practical;
- process start/exit/membership evidence;
- separate high-rate telemetry storage/retention from lifecycle/control events;
- telemetry downsampling/aggregation with explicit resolution/gaps;
- Linux cgroup I/O counters where supported;
- PSI CPU/memory/I/O pressure where supported;
- input/resize physical activity timestamps/counters without retaining input contents.

A useful retention model is raw recent samples plus progressively coarser aggregates. Exact windows are implementation policy and must remain bounded/configurable.

Completion gate: a long Run can preserve lifecycle history while still retaining bounded, queryable evidence of a short resource spike and the process subtree associated with it.

## Track D — Linux host safety envelope

Goal: prevent many valid Runs from exhausting the machine while remaining below the orchestration layer.

Required work:

- Jinushi workload-root cgroup when delegation permits;
- Supervisor/Guardian control plane kept outside the workload exhaustion boundary;
- aggregate workload memory and task/PID ceilings;
- configurable maximum active physical Runs;
- workload pressure/status projection;
- explicit admission failure when a physical safety ceiling prevents a new Run;
- no semantic task selection, queueing, or scheduler policy.

Completion gate: Jinushi has a final physical safety boundary above individual Run cgroups without becoming a scheduler.

## Track E — Event-driven observation and interactive control

Goal: remove unnecessary local polling for long-lived orchestration and terminal attachment.

Required work:

- Supervisor-internal wakeup/notifier on event/output/terminal changes;
- wakeup-driven per-Run event following;
- bounded all-Run watch/multiplex surface;
- reconnectable cursor/watermark behavior;
- wakeup-driven PTY/output following;
- multiple observers with single active input-writer lease;
- clean detach/reconnect semantics preserved.

Polling may remain only as a bounded OS/backend fallback, not as the primary client subscription mechanism.

Completion gate: Mottainai-like consumers can observe many Runs through bounded event-driven local subscriptions without per-Run 100 ms polling loops.

## Track F — Operational lifetime controls

Goal: keep a resident runtime bounded and controllable over long use.

Required work:

- terminal retention policy by age/count/byte budget;
- GC that never mutates live Runs;
- receipt/tombstone preservation policy;
- bbolt compaction strategy after historical deletion;
- runtime/store usage in `status`;
- Linux pause/resume using cgroup freeze/thaw where supported;
- bounded mutable physical controls such as soft/high memory or CPU controls where semantics are stable;
- explicit control evidence for changes;
- Guardian-loss recovery using independently proven Linux ownership, with fail-closed termination when safe re-control cannot be established.

Jinushi does not decide which Run should be paused, resumed, deprioritized, or removed for semantic reasons.

Completion gate: long-running use has bounded historical state and an explicit recovery/control story for both host pressure and Guardian loss.

## Current implementation status

This status snapshot was reviewed against `main@0d154cac`:

| Track / issue | Status | Notes |
| --- | --- | --- |
| A / #4 — canonical contract | Complete | Shared event, process/task, capability, receipt, retention, deadline, and Linux safety contracts are implemented. |
| B / #5 — idempotent control | Complete | Run submission and retry-sensitive control identities are implemented. |
| C / #6 — physical evidence | Partial | Linux process evidence and bounded Guardian/store telemetry pieces exist. The supervisor still writes high-rate `resource.sample` events into the lifecycle journal; supervisor-to-client telemetry query and acceptance coverage remain incomplete. |
| D / #7 — host envelope | Complete | Host workload limits, admission, capability reporting, and status projection are implemented. Host-specific cgroup and kernel proofs remain environment-dependent. |
| E / #8 — observation and writer ownership | Partial | Event/output subscriptions, all-Run watch, reconnectable cursors, and writer leases exist. Interactive CLI attach still polls for output/terminal state, so the track acceptance gate remains open. |
| F / #9 — operational lifetime | Pending | Terminal GC/status integration, capability-gated pause/resume and mutable controls, and Guardian-loss recovery remain governed by this track's requirements and the runtime/architecture contracts. |

No manual real-machine certification is claimed. Code-level tests do not prove
cgroup delegation, PSI availability, or other kernel-specific guarantees; report
those as unsupported or blocked unless the target environment proves them.

## Dependency structure

The intended dependency shape is:

```text
A: canonical contracts
   |
   +----> B: idempotent control
   |
   +----> C: evidence model
   |        |
   |        +----> E: subscriptions
   |
   +----> D: host envelope
   |
   +----> F: GC/control/recovery
```

After A freezes shared model/protocol types, B/C/D can proceed largely in parallel. E depends on the event/telemetry contracts from C. F can proceed in parallel except where it consumes the finalized effective-capability and retention models.

## Verification posture for this wave

This programme may be implemented and reviewed without a manual real-machine certification pass.

Code-level and deterministic automated verification should cover the contracts wherever the current environment supports them. Environment-dependent kernel/delegation proofs must remain explicitly unsupported/blocked rather than being simulated as proven.

Do not spend this wave building CI infrastructure solely to claim certification.

## Exit criteria

Wave 2 is complete when:

- the audit mismatches in Track A are closed;
- Run submission is retry-safe;
- lifecycle/control events and high-rate telemetry are separated and bounded;
- per-process physical evidence can explain Run-level resource changes;
- Linux host-level workload safety exists without scheduling semantics;
- subscriptions no longer depend on tight client polling;
- interactive input has writer ownership;
- terminal historical state has explicit bounded retention/GC;
- Guardian loss has a deliberate fail-closed Linux recovery path;
- pause/resume and mutable physical controls are capability-gated;
- architecture, runtime contract, implementation docs, and code agree;
- Windows remains frozen except for necessary shared-contract compile maintenance.

The final canonical audit should report only genuine environment-dependent certification gaps, not known design or code-contract debt.
