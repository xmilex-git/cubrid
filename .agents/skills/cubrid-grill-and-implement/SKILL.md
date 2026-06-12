---
name: cubrid-grill-and-implement
description: "Iteratively implement CUBRID C/C++ code changes by looping a writer subagent against a relentless CUBRID PR-style reviewer subagent, with a build gate every round, until the reviewer explicitly approves or a round cap is hit. Accepts a JIRA issue, a GitHub PR URL, a spec path, or an inline spec - refuses to start without one. Trigger on phrases like 'implement and grill', 'grill the implementation', 'CUBRID grill-and-revise', 'implement CBRD-XXXXX with grill', 'implement and adversarially review', or 'grill until it compiles and passes review'."
argument-hint: "<CBRD-XXXXX | pr-url | spec-path | inline-spec>"
allowed-tools: Bash(gh *), Bash(git *), Bash(jq *), Bash(just *), Bash(tee *), Bash(tail *), Bash(cat *), Bash(test *), Bash(grep *), Bash(realpath *), Read, Write, Edit, Glob, Grep, mcp__plugin_oh-my-claudecode_t__lsp_diagnostics, mcp__plugin_oh-my-claudecode_t__lsp_diagnostics_directory, mcp__plugin_oh-my-claudecode_t__lsp_hover, mcp__plugin_oh-my-claudecode_t__lsp_goto_definition, mcp__plugin_oh-my-claudecode_t__lsp_find_references, mcp__plugin_oh-my-claudecode_t__lsp_document_symbols, Agent
---

# CUBRID Grill-and-Implement

Iteratively implement CUBRID code changes by looping a writer subagent (plans, then writes code) against a relentless reviewer subagent (CUBRID PR-style review with LSP/clangd) until the reviewer approves or a round cap is hit. The loop guarantees the final tree compiles, every reviewer concern is addressed in code (not in argument), and the writer never short-circuits with self-approval.

This is the code-implementation counterpart to `/grill-with-docs`. Same two-agent loop and `VERDICT` contract; the artifact is a working CUBRID code change in the user's tree, not a document.

> NOTE for installers: The agent-spawning tool in this Claude Code harness is named `Agent`. If your runtime exposes it as `Task` instead, substitute `Task` for `Agent` in the `allowed-tools` frontmatter and in every spawn step below. Do not list both.

## Cost Characteristic

Each round runs an opus writer plus a critic-class reviewer over a possibly-large CUBRID diff with LSP, plus a full build (15-min timeout). Roughly 10x more expensive per round than `/grill-with-docs`. Default `max_rounds=3`. If at any reviewer round the cumulative line count from `git diff --stat <baseline_ref> --` exceeds 5000, print a warning and require an explicit `yes` from the user before continuing past round 2.

## When to Use

- User says "implement and grill", "grill the implementation", "CUBRID grill-and-revise", "implement CBRD-XXXXX with grill", "implement and adversarially review", or "grill until it compiles and passes review".
- User wants an adversarially-reviewed CUBRID code change driven by a JIRA ticket, a PR to extend, or a written spec.
- User is willing to trade tokens for rigor.

## Arguments

- `/cubrid-grill-and-implement CBRD-26583` - Implement the JIRA ticket.
- `/cubrid-grill-and-implement https://github.com/CUBRID/cubrid/pull/6950` - Extend or fix the existing PR.
- `/cubrid-grill-and-implement /path/to/spec.md` - Implement the spec file.
- `/cubrid-grill-and-implement "<inline multi-line spec>"` - Implement the inline spec.
- `/cubrid-grill-and-implement` - Refuse with the four input options. Do not assume, do not infer from `git status`, do not pick the most recent JIRA mention from the conversation.

## Required Input

Exactly one of:

- JIRA issue: `CBRD-XXXXX` - fetched via `/jira CBRD-XXXXX`.
- PR URL: existing GitHub PR to extend or fix - fetched via `gh pr view` and `gh pr diff`.
- Spec file path: a markdown spec describing what to build.
- Inline spec: multi-line description passed as the argument.

## State to Track Across Rounds

- `input_kind` - one of `jira` / `pr` / `spec-path` / `inline-spec`. Used as the trigger for PR-mode-only rules; do not key those rules off how `baseline_ref` was resolved.
- `input_payload` - the resolved spec content (JIRA description+comments, PR diff+body, file contents, or inline text).
- `baseline_ref` - captured before round 1 writer pass per Step 3; never reassigned.
- `round` - starts at 1; increments after every reviewer or build-gate failure round.
- `max_rounds` - default 3; user can override at invocation.
- `last_critique` - `""` on round 1; otherwise either the reviewer's most recent numbered critique or the synthesized build-failure critique.
- `last_reviewer_critique` - `""` until the first reviewer round runs; thereafter the most recent reviewer-issued critique. Independent of `last_critique`. Used for the empty-diff prepend in Step 5 and the cap-reached print in Step 7.
- `previous_round_diff_summary` - the diff stat captured at the end of the most recent build-passing, non-empty round; `""` on round 1; may be stale if subsequent rounds reverted.
- `build_status` - `pass` / `fail` / `skipped` for the most recent round.
- `cubrid_grill_log` - the per-round build-log path, recomputed at the start of every Step 5 invocation: `/home/cubrid/dev/cubrid/.claude/scratch/cubrid-grill-build-${baseline_ref:0:12}-r${round}.log`. Pass this literal string through state. Do not use `$$`.
- `last_writer_summary` - the line beginning with `Summary:` from the writer subagent's response, captured at Step 4 exit; the `Summary:` prefix is stripped, and the captured value is the trimmed remainder. `""` until Step 4 has run at least once. Used by Step 6 to seed the reviewer's "Justified rename/move/reformat" check.

## Subagent Choices

- Writer: `subagent_type=executor`, `model=opus`. CUBRID is C/C++ with concurrency, page-buffer, WAL, and MVCC rules - sonnet under-thinks too often.
- Reviewer: prefer `subagent_type=critic`. Fallback chain in this exact order: `critic` -> `code-reviewer` -> `executor`. Operational definition of "unavailable": the spawn tool returns an error mentioning unknown/unregistered subagent type, or the subagent type is absent from the runtime agent catalog. Try the next entry only on unavailability. A substantive review failure (the agent ran but returned no parseable verdict) is handled by Step 6's re-prompt-and-stop rule, not by the chain. Use the bare names listed here. If your harness exposes them as namespaced (e.g., `oh-my-claudecode:critic`), the skill must attempt the bare name first and, on `unknown subagent type` errors, attempt the namespaced variant before falling through to the next chain entry.
- Never have the orchestrator self-review. If every chain entry fails to spawn, surface the error and stop the loop.

## Execution Steps

### Step 1: Parse argument and refuse-on-empty

If the argument is empty, print the four input options and stop. Otherwise classify the argument:

- matches `^CBRD-\d+$` -> `jira`
- matches `^https?://github.com/.+/pull/\d+` -> `pr`
- existing file path -> `spec-path`
- otherwise -> `inline-spec`

### Step 2: Verify working tree is a CUBRID checkout

Run: `test -f CMakeLists.txt && grep -qE '^project[[:space:]]*\(CUBRID[[:space:]]*\)' CMakeLists.txt`. If exit is non-zero, refuse to start with: "Not in a CUBRID source checkout (CMakeLists.txt missing or does not declare a CUBRID project). Re-run from a CUBRID worktree." Do not proceed past this step on failure.

The regex tolerates both `project(CUBRID)` (no space, the actual form in CUBRID/cubrid's CMakeLists.txt line 42) and `project (CUBRID)` (one or more spaces). The trailing `[[:space:]]*\)` anchors the close paren so unrelated forks like `project(CUBRIDdb)` are correctly rejected.

### Step 3: Fetch context and capture baseline

Per `input_kind`:

- `jira`: invoke `/jira CBRD-XXXXX`.
- `pr`: `gh pr view <url> --json title,body,headRefName,baseRefName` and `gh pr diff <url>`. PR-mode preconditions: require the user to already be on the PR's `headRefName` (validate via `gh pr view --json headRefName --jq .headRefName` vs `git branch --show-current`); refuse with a clear error if not. The PR diff and body are treated as already-applied work and as the spec for extending or fixing the PR. If the PR is closed/merged and `gh pr diff` returns empty, refuse with: "PR diff is empty (closed or merged). Pass a JIRA, spec path, or inline spec instead." If the PR title contains a CBRD ticket, also fetch JIRA context.
- `spec-path`: Read the file.
- `inline-spec`: store the argument verbatim.

Always Read the resolved `reference.md` path (see Reference Files below) and any `CLAUDE.md` / `AGENTS.md` in the working tree's relevant directories.

Capture baseline:

- For `jira` / `spec-path` / `inline-spec`: `baseline_ref = $(git rev-parse HEAD)`.
- For `pr`: detect the upstream CUBRID remote (do NOT hardcode `origin`; CUBRID checkouts commonly use `cub`, `vk`, `hg`, etc.). Use the `detect_cubrid_remote` helper from Shared snippets below to set `UPSTREAM_REMOTE`. Run `git fetch "$UPSTREAM_REMOTE" <baseRefName>` to refresh the remote-tracking ref. Then validate with `git rev-parse --verify "$UPSTREAM_REMOTE/<baseRefName>"`; if that fails, refuse with the message the helper emits and stop. On success: `baseline_ref = $(git merge-base HEAD "$UPSTREAM_REMOTE/<baseRefName>")`. The reviewer must judge the PR's full delta, not only the writer's new edits on top of the PR head.

Print `baseline_ref` to the user. Initialize `round=1`, `max_rounds=3` (or user override), `last_critique=""`, `last_reviewer_critique=""`, `previous_round_diff_summary=""`, `build_status="skipped"`.

### Step 4: Writer pass

Spawn the writer subagent. Pass:

- `input_kind`, `input_payload`, `baseline_ref`, `round`, `last_critique`, `previous_round_diff_summary`.
- The resolved absolute path to `reference.md`.
- The contents of `references/writer-prompt.md` verbatim.

`references/writer-prompt.md` is the single source of truth for writer rules - do not duplicate any rules inline in the spawn prompt. Wait for the writer to finish before continuing.

Capture `last_writer_summary` as follows: scan the writer's response from the bottom up for the first line matching `^Summary:[[:space:]]*(.*)$`; the captured group (trimmed) is `last_writer_summary`. If no such line is found (writer protocol violation against rule 3), set `last_writer_summary = ""`, surface a warning at Step 6 input, and let the reviewer flag the missing summary as a finding.

### Step 5: Build gate

At the start of this step, compute `cubrid_grill_log = /home/cubrid/dev/cubrid/.claude/scratch/cubrid-grill-build-${baseline_ref:0:12}-r${round}.log` and use the literal string in every command below.

Detect the build command and validate the build environment:

- Check for a `build` recipe via `just --list 2>/dev/null | grep -qE '^\s*build( |$)'`.
- If absent, refuse the loop with: "No `just build` recipe found in this checkout. Add one or invoke `/cubrid-build` manually before re-running this skill." Do not improvise a substitute build command.
- CUBRID's `just build` recipe reads `$env.PRESET_MODE` to pick a CMake preset. If the variable is unset or holds a stale value (e.g., a preset from another worktree that does not exist in this worktree's `CMakePresets.json`), the build fails with a `No such build preset` CMake error that has nothing to do with the user's code. Validate before invoking the build:
  ```bash
  if [ -z "${PRESET_MODE:-}" ]; then
    echo "PRESET_MODE is not set. just build requires a CMake preset."
    echo "Available presets in this worktree:"
    cmake --list-presets 2>/dev/null
    echo "Set PRESET_MODE to one of the above (e.g. PRESET_MODE=debug_clang) and re-invoke."
    exit 1
  fi
  if ! cmake --list-presets 2>/dev/null | awk -F'"' '/"/ { print $2 }' | grep -qx "$PRESET_MODE"; then
    echo "PRESET_MODE=$PRESET_MODE is not a valid CMake preset for this worktree."
    echo "Available presets:"
    cmake --list-presets 2>/dev/null
    echo "Set PRESET_MODE to a valid preset and re-invoke."
    exit 1
  fi
  ```
  Refuse, do not default. Defaulting would couple the skill to one developer's setup and contradicts the "Do not improvise a substitute build command" anti-pattern below. The user is responsible for picking a preset.
- Once `PRESET_MODE` is validated, trust `just build` and run:
  ```bash
  # The `;` between `just build` and `echo $?` is load-bearing: both must run
  # in the same Bash invocation so `$?` reflects the just exit status. Do NOT
  # split into separate Bash tool calls.
  just build > "$cubrid_grill_log" 2>&1; echo $? > "$cubrid_grill_log.status"
  ```
  Use Bash `timeout: 900000` (15 minutes). Do not pipe through `tee`; do not invoke `set -o pipefail`. Read the exit status via `cat "$cubrid_grill_log.status"`.

On non-zero status:

- `build_status=fail`.
- Synthesize `last_critique` as: line 1 = `Build failed at round <round>; address the build error first, then re-address any prior reviewer items (the reviewer will re-raise unfixed items in the next reviewer round).` line 2+ = `Tail of <cubrid_grill_log>:` followed by the output of `tail -n 40 "$cubrid_grill_log"`. Treat the build log as opaque text; never interpolate it through a shell context. Pass it to the writer as a single string argument.
- Do not modify `last_reviewer_critique`. Build-failure synthesis replaces only `last_critique` for the round; carry-forward across build failures is by re-review, not by concatenation.
- Increment `round`. If `round > max_rounds` go to Step 7 (cap-reached); else go back to Step 4.

On zero status:

- `build_status=pass`.
- Capture `previous_round_diff_summary = $(git diff --stat <baseline_ref> --)`.
- If the writer produced an empty diff (`git diff --quiet <baseline_ref> --` returns 0):
  - If `round == 1`: print the writer's response, the spec, `baseline_ref`, and stop with: "Writer made no changes on round 1; clarify the spec and re-invoke."
  - If `round >= 2`: synthesize `last_critique`. If `last_reviewer_critique` is non-empty, set `last_critique` = "Writer reverted prior changes; re-address the previous reviewer critique below." prepended to `last_reviewer_critique`. If `last_reviewer_critique` is empty (no reviewer round has run yet), set `last_critique` = "Writer reverted or made no edits; the prior critique was a build failure - re-read it and produce a real change." Increment `round` and go back to Step 4.
- If the cumulative line count in `previous_round_diff_summary` exceeds 5000 and `round >= 2`, print the warning per Cost Characteristic and require explicit `yes` before continuing.
- Continue to Step 6.

### Step 6: Reviewer pass

Spawn the reviewer subagent per the fallback chain. Pass:

- The contents of `references/reviewer-prompt.md` verbatim.
- The resolved absolute path to `reference.md`.
- `baseline_ref`.
- `input_kind` (so the reviewer can gate PR-mode-only categories).
- The output of `git diff --stat <baseline_ref> --` (size summary only).
- The output of `git diff --name-only <baseline_ref> --` (changed-file paths only).
- `input_payload` so the reviewer can judge fit-to-spec.
- `previous_round_diff_summary`.
- `last_writer_summary` (captured at Step 4 exit per the State definition above; used to verify any rename/move/reformat justification). If `last_writer_summary == ""`, prepend a one-line warning to the reviewer prompt: "Writer protocol violation: no `Summary:` line in the writer's response. Flag the missing summary as a finding."
- The verdict contract: end the response with exactly one of `VERDICT: APPROVED` or `VERDICT: REVISE` on its own line, no markdown, no surrounding punctuation.

Do not pass the full diff body. Instruct the reviewer to fetch each file's diff itself via `git diff <baseline_ref> -- <path>` and to Read each changed file for full-function context.

Parse the verdict:

- `APPROVED` -> go to Step 7 with success.
- `REVISE` -> save everything before the verdict line as `last_critique` and as `last_reviewer_critique`; increment `round`; if `round > max_rounds` go to Step 7 with cap-reached, else go back to Step 4.
- No parseable verdict -> re-prompt the same reviewer once with the contract reminder. If the re-prompted response also lacks a parseable verdict (i.e., two consecutive responses with no verdict), surface the issue and stop the loop. Malformed-verdict failures do not trigger the fallback chain - the chain is reserved for unavailability, and verdict-format failures of a registered reviewer are surfaced to the user instead.

### Step 7: Terminate

- Success: print the labeled template below verbatim. The TODO-marker heading prints even when the marker list is empty (use `(none)` so the absence-of-markers signal is explicit). Do not commit.

  ```
  Approved at round <N>.
  baseline_ref: <ref>

  Final diff stat:
  <output of `git diff --stat <baseline_ref> --`>

  Changed files:
  <output of `git diff --name-only <baseline_ref> --`, one path per line>

  TODO markers added (review or resolve before invoking cubrid-pr-create):
  <output of `git diff <baseline_ref> -- | grep -nE '^\+.*\b(TODO|FIXME|XXX)\b'`, or "(none)" if empty>
  ```

  Compute the marker list with `git diff <baseline_ref> -- | grep -nE '^\+.*\b(TODO|FIXME|XXX)\b'` (per-file equivalent: iterate `git diff --name-only <baseline_ref> --` and grep each). The heading must always print so the user always sees the absence-of-markers signal.
- Cap reached with REVISE open: print `last_critique` verbatim. If `last_reviewer_critique` differs from `last_critique` (e.g., the cap was hit on a build-failure round), print `last_reviewer_critique` verbatim under the heading "Most recent reviewer critique (still unresolved):". Print the final diff stat, `baseline_ref`, and ask the user to choose (a) accept as-is, (b) extend cap by `N` rounds, or (c) abandon. Cap-extension protocol: if the user picks (b), prompt for `N` in a follow-up turn ("How many additional rounds? Enter a positive integer."), parse the integer reply, set `max_rounds += N`, then resume from Step 4. **Do NOT reset `round`.** Any reply that is not (a) or a valid positive integer for (b) is treated as (c) - leave the tree as-is, do not loop further unless the user re-invokes the skill. Never silently loop past the cap.

  Worked example. Suppose `round=4`, `max_rounds=3`, the cap-reached branch has fired, and the user picks (b) with `N=2`. The orchestrator sets `max_rounds=5` and does not touch `round`. The next loop iteration's check `round > max_rounds` evaluates `4 > 5` → false → continue from Step 4 at round 4. The loop terminates either when the reviewer approves or when `round` reaches 6 after two more REVISE rounds. Resetting `round` would silently re-do prior work and burn the extension; not resetting it preserves the "extend by N more rounds" semantics the user asked for.
- Reviewer infrastructure failure: print the error, `baseline_ref`, and the current diff stat so the user can resume manually.

## Anti-patterns

- Don't sanitize the critique before passing it to the writer. Harsh stays harsh.
- Don't be the reviewer yourself. Spawn a real subagent every round, even when the diff looks small.
- Don't auto-commit, auto-stage, or auto-stash. The user's git state is theirs.
- Don't re-baseline mid-loop. `baseline_ref` is captured once.
- Don't run the reviewer on a broken build. Build gate first, every round.
- Don't manufacture findings to look thorough.
- Don't pre-read the changed files or the full diff body in the orchestrator. The reviewer fetches per-file diffs and Reads files itself.
- Don't fall back to a different reviewer subagent on a malformed verdict. Surface and stop.
- Don't leave unflagged deferrals. Every stub, deferred edge case, known limitation, missing test, or assumption-to-revisit must carry a marker at the exact site in the source - `// TODO(CBRD-XXXXX): <what and why>` (or `/* TODO(CBRD-XXXXX): */`) when the JIRA ticket is known, otherwise `// TODO: <what and why>`. For a missing test, the production code path that lacks coverage is the "exact site." The reviewer treats unflagged deferrals as a finding.
- Don't introduce unjustified non-functional churn in PR mode (`input_kind == 'pr'`). Reformatting, renames, block moves, or drive-by fixes that do not serve the spec are findings - except when the writer explicitly justifies them in the one-line summary using the form "Justified rename: <identifier> -> <new>; reason: <why required by spec>" (or analogous "Justified move:" / "Justified reformat:"). Any unsummarized rename/move/reformat is unjustified.

## Out of Scope

- No auto-PR creation. `cubrid-pr-create` handles that as a separate user-invoked step.
- No worktree management. Assume the user is already in the working tree they want changed.
- No CUBRID test-case generation. The writer focuses on production code; the user runs `create-testcases` separately after approval.
- No commit/squash/rebase logic. The user owns git history.

## Shared snippets

### `detect_cubrid_remote`

Used by both `cubrid-grill-and-implement` (Step 3 PR mode) and `cubrid-loop-pr` (Step 4 push). The helper resolves `UPSTREAM_REMOTE` to the git remote that points at the CUBRID/cubrid repository, with deterministic tie-breaks. Both skills MUST source this helper text verbatim — keep them in lockstep on edits.

```bash
detect_cubrid_remote() {
  # Stage 1: branch upstream, only if it points at CUBRID/cubrid (case-insensitive).
  local upstream
  upstream=$(git rev-parse --abbrev-ref @{upstream} 2>/dev/null) || upstream=""
  if [ -n "$upstream" ]; then
    local up_remote="${upstream%%/*}"
    local up_url
    up_url=$(git remote get-url "$up_remote" 2>/dev/null) || up_url=""
    if [ -n "$up_url" ] && printf '%s' "$up_url" \
        | grep -qiE 'github\.com[:/]cubrid/cubrid(\.git)?$'; then
      echo "Detected CUBRID remote via branch upstream: $up_remote ($up_url)" >&2
      printf '%s\n' "$up_remote"
      return 0
    fi
  fi

  # Stage 2: scan all remotes; collect every remote whose URL matches CUBRID/cubrid.
  local matches
  matches=$(git remote -v | awk '
    tolower($0) ~ /github\.com[:\/]cubrid\/cubrid(\.git)?[[:space:]]+\(fetch\)/ {
      print $1
    }
  ' | sort -u)
  if [ -z "$matches" ]; then
    echo "No git remote points at CUBRID/cubrid (case-insensitive). Available remotes:" >&2
    git remote -v >&2
    return 1
  fi

  # Stage 3: tie-break. Prefer a remote literally named "origin" or "cub" (in
  # that order); otherwise take the first match alphabetically and log the
  # alternatives so the user sees which one was picked and which were ignored.
  local picked=""
  if printf '%s\n' "$matches" | grep -qx 'origin'; then
    picked='origin'
  elif printf '%s\n' "$matches" | grep -qx 'cub'; then
    picked='cub'
  else
    picked=$(printf '%s\n' "$matches" | head -1)
  fi
  echo "Detected CUBRID remote via URL scan: $picked" >&2
  local others
  others=$(printf '%s\n' "$matches" | grep -vx "$picked" | tr '\n' ' ')
  if [ -n "$others" ]; then
    echo "Other CUBRID-pointing remotes (ignored): $others" >&2
  fi
  printf '%s\n' "$picked"
}
```

The cascade fails closed: Stage 2 returning empty refuses the skill rather than guessing. The variable is named `UPSTREAM_REMOTE` (not `ORIGIN_REMOTE`) so the name reflects its semantics — "the remote that points at the canonical upstream repository" — regardless of whether it resolves to `origin`, `cub`, or some other local name.

## Reference Files

- `references/reviewer-prompt.md` - relentless reviewer persona, code-review variant. Loaded and passed verbatim to every reviewer subagent.
- `references/writer-prompt.md` - single source of truth for writer rules. Loaded and passed verbatim to every writer subagent.
- `/home/cubrid/dev/cubrid/.agents/skills/cubrid-pr-review/reference.md` - CUBRID knowledge base (six-place error-code rule, lock/page-buffer/WAL/MVCC protocols, build-mode guards, key data structures, false-positive guidance). If the file does not exist, refuse with: "cubrid-pr-review/reference.md not found at <resolved path>. Run `just install` in your skills repo (or equivalent) to populate it." Skill development against an uninstalled local copy is a developer-workflow concern (e.g., `just install` symlinks during dev, or a one-off symlink into your dev tree), not a skill-spec concern.
