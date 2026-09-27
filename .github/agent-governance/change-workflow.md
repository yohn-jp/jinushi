# Agent change workflow

> Canonical source: `yohn-jp/.github/docs/agent-change-workflow.md`.
> Consumer repositories receive a generated copy at `.github/agent-governance/change-workflow.md`; do not edit the generated copy as a competing authority.

This document defines the organization-level execution model for coding-agent changes. It complements executable governance; it does not replace Inari schemas, validators, repository rulesets, tests, or product-specific contracts.

## 1. Authority and precedence

Use the following order when deciding what work is allowed and what evidence is current:

1. The latest explicit user instruction and the accepted Issue define intent, scope, and requested lifecycle.
2. Executable policy, Inari schemas/skills, validators, workflows, rulesets, and tests define exact governed behavior.
3. The current repository and GitHub state define what already exists, what has merged, what is stale, and what is actually blocked.
4. A repository-local overlay may strengthen or specialize this shared workflow but must not silently weaken shared invariants.
5. A runtime profile adapts execution/delegation to a model or agent runtime; it never changes repository governance.

If these disagree, reconcile the authority before mutating state. Do not pick the interpretation that is easiest to execute, and do not treat remembered or previously reported state as fresher than the repository.

## 2. Classify the requested mode before acting

The requested verb is part of scope.

- **Analysis / review / design only:** read-only unless the user explicitly requests creation or mutation. Do not turn a design request into implementation.
- **Standalone implementation:** one accepted Issue, one implementation branch/worktree, one PR to the governed standalone base.
- **Epic development:** one tracking Epic, one Epic integration branch and Draft integration PR, and independently reviewable child Issues/branches/worktrees/PRs.
- **Merge / release operation:** re-evaluate the current target immediately before each merge or publication action. A prior green state is not sufficient.

When the user requests the complete lifecycle, do not stop after analysis or implementation if branch, validation, PR creation, or other requested lifecycle steps remain available. Conversely, never perform an unrequested later lifecycle step.

## 3. Reconcile live state before creating work

Before creating an Issue, branch, worktree, PR, or implementation plan, establish only the bounded facts needed for the next decision:

- current default-branch head when the base matters;
- target Issue state and accepted scope;
- existing open/merged PRs that may already implement the work;
- existing branch/worktree ownership and expected base;
- current PR head/base and relevant validation state for review or merge work.

Rules:

- Do not recreate completed work because an Issue, plan, memory, or earlier message is stale.
- Do not create duplicate/no-op Issues, branches, or PRs merely because a preliminary plan listed them.
- Do not close an Issue as complete without repository evidence that its acceptance criteria are satisfied or superseded.
- If live state contradicts the proposed dependency order, reconcile the plan to reality before execution.
- Do not perform a repository-wide rediscovery when the task already names the relevant Issue, PR, file, symbol, or failure.

## 4. Define an implementation envelope

Before a write-capable implementation session, make the intended change boundary explicit enough to detect scope drift:

- semantic outcome;
- required write-set and likely derived/generated files;
- dependencies and expected base;
- forbidden escalations or explicitly excluded adjacent work;
- required validation evidence;
- requested lifecycle end state.

The envelope is not permission to predict every touched line. It is a guard against turning a thin change into an unrelated refactor or expanding a one-session Issue into an unreviewable change set.

If the implementation grows materially beyond the envelope, stop expanding it. Reconcile the Issue or split work only when the user and governance model call for a split. Do not create follow-up work pre-emptively as a substitute for finishing the accepted task.

## 5. Standalone Issue workflow

Canonical standalone flow:

```text
accepted Issue
  -> governed task branch
  -> dedicated worktree/session
  -> smallest coherent implementation
  -> targeted validation
  -> final required validation
  -> PR to main/default governed base
  -> review/merge when separately requested or authorized
```

Invariants:

- Never implement directly on `main`/`master`.
- One implementation leaf should normally be one Issue = one branch = one worktree = one PR.
- Use the current Issue/task worktree if it already exists; do not create a second owner for the same leaf.
- A PR is Ready for review by default after implementation is complete unless the user or executable policy explicitly requires Draft.

## 6. Epic development workflow

An Epic branch is an integration boundary, not an implementation leaf. The currently implemented canonical Epic branch class is:

```text
epic/<epic-issue-number>-<slug>
```

The canonical Epic PR title class is:

```text
epic(<scope>): <description>
```

Canonical topology:

```text
main
  <- Draft Epic integration PR
     epic/<epic>
       <- child PR A <- child branch/worktree A
       <- child PR B <- child branch/worktree B
       <- child PR C <- child branch/worktree C
```

Use this sequence:

1. Create or confirm the tracking Epic Issue. The Epic itself is not a leaf implementation task.
2. Create the canonical Epic branch from the current intended base and open the Epic -> `main` integration PR as Draft.
3. For an architecture- or governance-heavy Epic whose semantic contract is not already stable, create the documentation/decision child first. Capture the complete agreed design there and merge that PR into the Epic branch before finalizing implementation child decomposition. Do not split implementation work against an unstable contract merely to increase parallelism.
4. Create child Issues only for independently reviewable, one-session-sized implementation authorities. Each child states dependencies, acceptance criteria, expected branch, and expected PR base.
5. Create each child implementation branch/worktree from the correct Epic baseline. Child PRs target the Epic branch, not `main`.
6. Execute independent children in parallel only when dependencies and write-sets permit it. Reconcile children that have become stale against the current Epic branch before integration.
7. Integrate child PRs into the Epic. A green child proves only that leaf; it does not certify the composed Epic.
8. Before Epic -> `main`, incorporate the current `main` as required by executable policy and run the full integration/product certification required for the composed tree.
9. If the relevant base or tested Epic head advances after certification, treat the evidence as stale and refresh it before merge.
10. Merge/close/cleanup only when the Epic acceptance criteria and executable governance are satisfied.

Standalone Issues continue to target `main`. Independent Epics stay parallel and must not be stacked on one another merely for convenience.

The organization Epic tracker may define intended routing and merge semantics before all mechanics are executable. Where executable Inari/ruleset support is not yet available, do not simulate enforcement by silently inventing a different workflow. Follow the currently implemented authority and report the missing enforcement boundary.

## 7. Parallelism and ownership

Parallelism is derived from the dependency DAG and expected write-sets, not from the number of available agents.

- Independent leaves with non-conflicting semantic authorities may run concurrently.
- Changes to the same files, shared schemas, canonical tables, generated authorities, or integration contracts must be serialized or deliberately integrated by the orchestrator.
- Do not split one requested orchestrated session into multiple top-level prompts merely because multiple workers are available.
- Each write-capable worker gets a dedicated worktree/branch and must not mutate another worker's worktree.
- Before overwriting, rebasing, merging, or publishing from a long-lived session, reconcile the expected base with live state. Stale state is a conflict to resolve, not permission to overwrite newer work.
- Clean up stale/prunable worktrees only through the runtime's governed lifecycle; do not bypass ownership or reconciliation guards.

Runtime-specific delegation rules are defined in `agent-runtime-profiles.md` and the machine-readable runtime profile source.

## 8. Issue and PR issuance

Use Inari for governed Issue/PR/template/normalization operations where supported.

- Construct semantic/structured fields first and let the governed renderer produce Markdown. Avoid shell-interpolated bodies, hand-escaped newlines, or ad-hoc Markdown reconstruction when Inari owns that surface.
- Validate required sections and checklist acceptance criteria before issuance.
- Search/reconcile existing work before creating a new artifact.
- When validation rejects an Issue or PR, repair the exact semantic violation. Do not bypass the validator with raw `gh`, a different template, or malformed escape sequences.
- Never infer branch/base/parent routing from naming alone when canonical Issue metadata or Inari semantics are available.

## 9. Review contract

A review is an evidence-producing operation, not a statement of confidence.

For each requested PR:

1. confirm the current PR head and base;
2. calculate/review the current base -> head change set;
3. reconcile the diff against the accepted Issue and current architecture/authority;
4. inspect relevant tests, generated outputs, and CI evidence;
5. record concrete findings or a concrete approval basis.

Rules:

- If the user asks to review a set of PRs, enumerate the target set and actually inspect every target before claiming the set is reviewed.
- Never call an unexecuted, pending, stale, unavailable, or merely assumed check successful.
- Green CI is necessary evidence where required; it is not a substitute for scope, architecture, security, or semantic review.
- Review against the current target base immediately before merge. Earlier review evidence can become stale as the base changes.
- Report exact blockers. One blocked PR does not stop review or integration of independent PRs unless it creates a real dependency.

## 10. Merge, CI, and release safety

Do not treat `all green` as an automatic merge instruction.

Immediately before each merge, re-evaluate:

- current base/head relationship and conflicts;
- linked Issue and scope;
- required governance and review status;
- required CI/certification freshness;
- dependency and integration state.

Do not force-merge or bypass a governance failure merely to make progress. An explicit user request to merge is authority to perform the governed merge lifecycle, not to falsify evidence or disable protections.

For write-capable CI/release paths, preserve the security boundary: privileged publication/metadata operations run from trusted/pinned code, untrusted artifacts are validated as data, permissions are least-privilege, and publication is tied to the exact certified revision. Product-specific release contracts remain repository-owned.

## 11. Operational truthfulness and progress

- Report actual repository state, not what a plan predicted should exist.
- Never say `done`, `all reviewed`, `all merged`, `passed`, or equivalent unless the stated set and evidence actually support it.
- Surface material progress, changed assumptions, and blockers at phase boundaries. Do not leave a long-running multi-step operation silent while no user-visible state changes.
- If one target is blocked, continue independent requested work and report the blocker precisely.
- Do not rerun unchanged checks or poll long-running jobs merely to create activity.
- Do not ask the user to repeat a fact that is already present in the current task context; verify volatile facts from the repository when verification matters.

## 12. Failure-prevention matrix

| Failure class                                             | Required behavior                                                                                           |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Stale plan/Issue metadata                                 | Reconcile against current `main`, Issue/PR state, and actual code before acting.                            |
| Duplicate or speculative artifacts                        | Search current Issues/PRs/branches first; create only artifacts required by the accepted lifecycle.         |
| Scope explosion                                           | Define an implementation envelope and reconcile the final diff to it.                                       |
| Wrong PR base in Epic work                                | Derive child routing from the parent Epic; child -> Epic, Epic -> `main`.                                   |
| Design request turned into implementation                 | Classify request mode first; read-only means no repository mutation unless explicitly requested.            |
| Incomplete bulk review presented as complete              | Enumerate targets, inspect each, and report per-target evidence/status.                                     |
| CI success treated as correctness                         | Perform semantic/scope/security review in addition to required CI.                                          |
| Stale review/certification                                | Re-evaluate against the current base/head before merge/publication.                                         |
| Governance formatting/escaping mistakes                   | Use structured Inari inputs/rendering; repair the exact validator error instead of bypassing it.            |
| Unsafe parallelism                                        | Parallelize only independent dependency/write-set leaves; centralize or serialize shared authority changes. |
| Requested single orchestrator split into several sessions | Preserve the requested top-level session; delegate internally according to the runtime profile.             |
| Long silent execution                                     | Report material phase progress/blockers without polling noise.                                              |
| Forced progress through guard failure                     | Treat the guard as authoritative; fix the cause or report the blocker.                                      |

## 13. Distribution and repository overlays

The shared source lives in `yohn-jp/.github` and is synchronized to participating repositories. Generated consumer files carry the shared contract; they are not independent authorities.

A repository may add local execution constraints in:

```text
.github/agent-governance/repository-overlay.md
```

The overlay may specify product architecture, local validation, additional protected paths, or stricter execution constraints. It must not silently weaken shared no-direct-main, worktree, scope, evidence, or governed-GitHub invariants.

When an overlay conflicts with executable repository policy, the executable policy wins and the mismatch should be corrected rather than rationalized in a prompt.
