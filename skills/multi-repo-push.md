# Multi-repo push (template)

A reusable task template for when a project's working state spans
multiple git repositories that need to be pushed in dependency order.
Customize for your repo set; this is a pattern, not a fixed recipe.

## What this is for

Some projects depend on other repos you also maintain — a fork of an
upstream library, a build artifact source, a submodule. When you've
made changes across several of them and want them shipped safely
without breaking the dependency order, this template gives the worker
a careful, dry-run-first push procedure.

## Inputs to fill in

When invoking this skill, replace these in the worker task:

- `{{REPO_LIST}}` — list of repos involved, in **push order**. The
  rule is: a repo whose SHA another repo *embeds* (via submodule,
  pinned dependency, vendored binary) must be pushed first.
- `{{ORIGIN_REMOTE}}` — the remote name to push to (typically
  `origin`). The worker must NEVER push to any other remote without
  explicit confirmation.
- `{{ALLOWED_BRANCHES}}` — branches the worker is allowed to push
  (e.g. `main`, `release/*`). If a repo's current branch isn't in
  this list, the worker must escalate before pushing.

## Worker task

Perform a careful multi-repo push across the project's repos. Do NOT
push anything until the dry-run summary is posted and the orchestrator
has confirmed (via `request_decision_from_om`).

### Steps

1. **Inspect each repo in dependency order** ({{REPO_LIST}}):
   1. `cd` into the repo.
   2. Record the current branch with `git rev-parse --abbrev-ref HEAD`.
   3. **Do not stash, reset, or touch the working tree.** If there
      are uncommitted changes, report and skip that repo.
   4. `git status --short` — note untracked and modified files.
   5. `git log @{u}..HEAD --oneline` — commits ahead of remote.
   6. `git log HEAD..@{u} --oneline` — commits behind remote (warn
      loudly if non-zero; suggests a rebase is needed first).
   7. If the repo is a parent of a submodule that's also in
      {{REPO_LIST}}, record whether the submodule SHA staged in this
      parent matches the submodule's `HEAD` — if not, flag it as a
      pre-push action (the parent will push a stale submodule
      pointer otherwise).

2. **Summarize and request a decision.** Use
   `request_decision_from_om` with:
   - Branch name per repo.
   - Commit count and a list of `oneline` summaries for each repo.
   - A `WILL_PUSH` flag for repos with local-ahead commits and a
     branch in `{{ALLOWED_BRANCHES}}`.
   - A `NOTHING_TO_PUSH` flag for repos with no local-ahead commits.
   - A `BLOCKED` flag for any repo with uncommitted changes,
     remote-ahead commits, an out-of-list branch, or a stale
     submodule pointer.
   - The push order you intend to follow.

3. **If the orchestrator answers "proceed":** push in the order
   listed, one at a time, only the repos flagged `WILL_PUSH`. After
   each push, verify by reading the remote SHA and report it via
   `report_to_om`.

4. **`mark_task_complete`** with:
   - Repos pushed (with new SHAs).
   - Repos skipped (with reasons).
   - Any warnings (uncommitted changes left behind, remote-ahead
     situations, stale submodule pointers).

### Safety rules

- **Never `--force` or `-f` push.** If a repo's remote is ahead, stop
  and escalate. The user will rebase manually.
- **Only push to `{{ORIGIN_REMOTE}}`.** Never push to any upstream
  fork remote.
- **Never touch the working tree** — no stash, no reset, no clean.
  Dirty trees mean the worker reports and skips.
- **Submodule SHA mismatches are flagged, not auto-fixed.** Surface
  them in the dry-run summary; let the user decide.
- **If any push fails partway through, stop the sequence and report.**
  Partial pushes across multiple repos are recoverable but require
  human attention.

## When to invoke this skill

The user signals an intent to ship work that spans multiple repos:
"push the multi-repo work", "ship this across all the repos",
"do a multi-repo push".

Don't use this for:
- Single-repo pushes — just have the worker `git push`.
- First-time remote configuration.
- Merging upstream into origin (that's a fetch + merge, not a push).
