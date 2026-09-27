# Epic and orchestration

Use this skill for Epic execution and multi-leaf orchestration.

- Preserve the canonical producer/integrator/certification dependency graph.
- Parallelize only independent leaves.
- Integration branches are not implementation leaves; child work uses its governed branch/worktree.
- Sol may orchestrate and integrate while bounded implementation leaves are delegated to Luna when that runtime profile is requested.
- Stop at user-requested checkpoints when human review is part of the workflow.
- When Inari is available, obtain the current Epic/orchestration procedure with `inari skill <scenario>`.
- Inari use is limited to the organization-approved `inari pr`, `inari issue`, and `inari template` surfaces. Other advertised capabilities require explicit authorization.
- Do not duplicate or guess Inari command semantics in this file.
