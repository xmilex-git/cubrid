---
name: cubrid-loop-pr
description: Auto-iterate on a CUBRID PR by looping fix -> commit -> push -> trigger CI -> wait, until both `test_sql` and `test_medium` checks pass or a 24-hour cap is hit. Use when the user wants hands-off iteration on a PR until CI is green and is willing to leave the session running. Triggers on phrases like 'loop pr', 'loop on pr', 'auto-fix until ci passes', 'keep fixing until tests pass', 'iterate until sql and medium pass', 'run pr loop'.
argument-hint: <pr-url> <intent>
allowed-tools: Bash(gh *), Bash(git *), Bash(jq *), Bash(date *), Bash(sleep *), Bash(awk *), Bash(grep *), Bash(sed *), Bash(tail *), Bash(head *), Read, Write, Edit, Glob, Grep, Skill
---

# CUBRID PR CI Loop

Iteratively fix a CUBRID PR until the CircleCI `test_sql` and `test_medium` checks both pass, or until a 24-hour wall-clock cap is reached. Each round delegates the actual code fixing to `/cubrid-grill-and-implement` and only handles the outer loop: commit, push, trigger CI, wait, parse results, and either stop or start the next round.

## When to Use

- User wants hands-off iteration on a PR until CI is green and is OK leaving the session running for hours.
- User says "loop the PR", "keep fixing until CI passes", "auto-iterate on PR until sql/medium pass".
- User explicitly shares a PR URL plus an intent describing what the PR should achieve.

Do NOT trigger this for one-off fixes. The skill is expensive (multi-round grill + multiple CircleCI runs) and only makes sense when the user has explicitly accepted the long-running, multi-hour shape.

## Arguments

- `/cubrid-loop-pr <pr-url> <intent>` — Loop on the given PR with the given intent (single-line description of what should ultimately work).
- `/cubrid-loop-pr <pr-url>` — Refuse and ask for an explicit intent string.
- `/cubrid-loop-pr` — Refuse and ask for both arguments.

`<intent>` should be a complete sentence. It is forwarded verbatim to `/cubrid-grill-and-implement` every round and is what the loop is ultimately optimizing for. Vague intents produce vague fixes.

## Cost & Time Characteristics

- One round =
  - `/cubrid-grill-and-implement` (its own multi-round writer + reviewer + build, default 3 rounds, opus)
  - `git push`
  - One CircleCI run for `sql medium` (`test_sql` and `test_medium` jobs are slow; expect 30-90 min each, possibly longer under contention).
- Realistic round cap inside 24h: 4-8 rounds. The skill does not impose a round cap separate from the wall-clock cap.
- Hard wall-clock cap: 24 hours from `start_epoch`, captured at Step 1.
- Polling cadence: every 10 minutes. Each poll prints one status line so the user can check progress from the Claude app on a phone.

## Skill Name Reference

The user may type `/cubrid-grill-and-implement-prompt` because that is the name of the draft brief in `drafts/cubrid-grill-and-implement-prompt.md`. The actual installed skill is `/cubrid-grill-and-implement`. Use the latter. If the installed skill is missing, refuse the loop with a clear pointer to the draft.

## State to Track

Spell these out at invocation and carry them through the loop:

- `pr_url` — argument 1.
- `intent` — argument 2 (verbatim).
- `start_epoch` — `$(date +%s)` at Step 1.
- `deadline_epoch` — `start_epoch + 86400`.
- `round` — starts at 1.
- `last_failure_summary` — empty on round 1; on round >= 2, the tails of the failed CI job logs from the previous round.
- `head_sha` — refreshed after every push; checks are scoped to this SHA.
- `trigger_epoch` — when the latest `/run sql medium` comment was posted; used to ignore stale check runs.
- `LOOP_BASELINE_DIRTY_FILE` — path to a NUL-separated snapshot of `git status --porcelain -z` taken at Step 1 substep 6. Step 4 substep 2 subtracts this set from the per-round dirty list to stage only the loop's own additions.

## Execution Steps

### Step 1: Parse and Validate

1. Refuse with a usage hint if `<pr-url>` or `<intent>` is empty.
2. Validate the URL: `gh pr view <pr-url> --json number,headRefName,baseRefName,state,isDraft,url`. If exit is non-zero, refuse.
3. If `state != OPEN`, warn once and ask whether to proceed (closed/merged PRs cannot be pushed to).
4. Verify the working tree is on the PR's `headRefName`, AND that the local HEAD matches the PR's `headRefOid`. Branch-name match alone is not enough: a teammate may have force-pushed, or the user may not have pulled, leaving the local commit older than the PR head. The first `git push` would then either be rejected or — worse — silently fast-forward and drop the teammate's commits.

   ```bash
   # Branch name must match.
   test "$(git branch --show-current)" = "$(gh pr view <pr-url> --json headRefName --jq .headRefName)" \
     || { echo "Check out the PR's headRef first; this loop pushes to the PR branch."; exit 1; }

   # Refresh remote refs (uses the auto-detected upstream remote from Step 4's
   # detect_cubrid_remote helper; if you reach this substep before the helper
   # has been called, just call `git fetch` against all remotes here).
   git fetch "$UPSTREAM_REMOTE" 2>/dev/null || git fetch --all
   local_head=$(git rev-parse HEAD)
   pr_head=$(gh pr view <pr-url> --json headRefOid --jq .headRefOid)

   if [ "$local_head" != "$pr_head" ]; then
     # Determine ahead/behind counts so the remediation hint matches the
     # divergence shape. git rev-list --left-right --count emits "<left> <right>".
     read ahead behind < <(git rev-list --left-right --count "$pr_head...$local_head" \
       | awk '{print $2, $1}')
     # After the awk swap: $ahead = commits local has that pr_head doesn't,
     #                     $behind = commits pr_head has that local doesn't.
     echo "Local HEAD ($local_head) does not match PR head ($pr_head)."
     echo "Local is ahead by $ahead, behind by $behind."
     if [ "$ahead" = "0" ] && [ "$behind" -gt "0" ]; then
       echo "Local is strictly behind. Fast-forward with:"
       echo "  git pull --ff-only $UPSTREAM_REMOTE <headRefName>"
     elif [ "$ahead" -gt "0" ] && [ "$behind" = "0" ]; then
       echo "Local has unpushed commits ahead of the PR head."
       echo "Either push them with 'git push' before starting the loop, or check"
       echo "out the PR head explicitly if those commits should be discarded."
     else
       echo "Local and PR head have diverged ($ahead ahead, $behind behind)."
       echo "Inspect with: git log --oneline --graph $pr_head...$local_head"
       echo "Then either rebase your local commits onto the PR head, or reset"
       echo "after backing up your work. This skill does not auto-resolve divergence."
     fi
     exit 1
   fi
   ```
   The hint never suggests `git reset --hard` unconditionally — that would lose unpushed local work in the "ahead" and "diverged" cases. Each remediation matches its divergence shape; the user pastes whichever applies.
5. Capture `start_epoch=$(date +%s)`, `deadline_epoch=$((start_epoch + 86400))`. Print both as ISO timestamps. (Captured here, before the baseline snapshot in substep 6, so the snapshot file path can be parameterized by `start_epoch`.)
6. Capture working-tree baseline (replaces the previous "refuse if dirty" rule). Real CUBRID worktrees commonly carry IDE config, submodule pointer drift, and unrelated local edits; refusing on any of them blocks startup. Instead, snapshot the current dirty set in NUL-separated form so subsequent rounds can stage only what the loop's own writer adds on top:
   ```bash
   LOOP_BASELINE_DIRTY_FILE="/home/cubrid/dev/cubrid/.claude/scratch/cubrid-loop-baseline-${start_epoch}.txt"
   git status --porcelain -z > "$LOOP_BASELINE_DIRTY_FILE"
   if [ -s "$LOOP_BASELINE_DIRTY_FILE" ]; then
     echo "Note: working tree is dirty at loop start; baseline captured at $LOOP_BASELINE_DIRTY_FILE."
     echo "Only files dirty AFTER baseline will be staged per round."
   fi
   ```
   Carry `LOOP_BASELINE_DIRTY_FILE` through state. Step 4 reads it.
7. Verify `/cubrid-grill-and-implement` is available. If not, refuse with: "/cubrid-grill-and-implement is not installed. See drafts/cubrid-grill-and-implement-prompt.md to scaffold it, then re-run."

### Step 2: Initialize Loop

Print a one-block summary so the user can confirm before the loop starts iterating without prompts:

```
PR:          <pr-url>
Intent:      <intent>
Branch:      <headRefName>
Start:       <ISO start_epoch>
Deadline:    <ISO deadline_epoch> (24h cap)
Loop policy: poll every 10 minutes; stop when test_sql AND test_medium pass, or at deadline.
```

Set `round=1`, `last_failure_summary=""`.

### Step 3: Fix Pass

Invoke `/cubrid-grill-and-implement` with the PR URL as the input. Pass the intent plus, on rounds >= 2, the prior CI failure summary, framed as:

- Round 1: `<intent>`
- Round >= 2: `Prior CI failure (round <round-1>): <last_failure_summary>. Fix the failure while preserving prior work and the original intent: <intent>.`

The grill skill captures its own baseline and runs its own writer/reviewer/build loop. Do not pre-read changed files here; let the grill skill own the code-change scope.

When the grill skill returns:

- If it returned with `Approved at round N`, continue to Step 4.
- If it returned cap-reached or infrastructure failure, surface the message verbatim, print elapsed wall-clock, and stop the loop. Do not push half-baked changes.

### Step 4: Commit and Push

The grill skill leaves the tree dirty. Commit only what the grill skill changed.

1. `git status --porcelain` — if empty, the grill skill made no real edits; print a warning and stop the loop with: "Round <round>: grill skill produced no diff; aborting to avoid spamming CI."
2. Stage only the files newly dirty since `LOOP_BASELINE_DIRTY_FILE` was captured at Step 1 substep 6. The previous one-line `git status --porcelain | awk '{print $2}' | xargs git add` was broken in three ways: (a) `awk '{print $2}'` truncates filenames containing spaces; (b) on a rename line `R  old -> new`, `$2` returns `old` (the path the user wants to UN-stage); (c) `xargs` without `-0` is unsafe on shell metacharacters. The replacement uses NUL-separated porcelain (`-z`) and a bytewise parser so all three issues are fixed at once:
   ```bash
   ROUND_PORCELAIN_FILE="/home/cubrid/dev/cubrid/.claude/scratch/cubrid-loop-round-${start_epoch}-r${round}.txt"
   git status --porcelain -z > "$ROUND_PORCELAIN_FILE"

   # Porcelain -z format: <XY> <SP> <path>\0[<orig-path>\0 for R/C status]
   # Parse bytewise, advance past the orig-path token on rename/copy entries,
   # and stage only paths newly dirty this round (round set minus baseline set).
   python3 - "$LOOP_BASELINE_DIRTY_FILE" "$ROUND_PORCELAIN_FILE" <<'PY' \
     | xargs -0 -r git add --
   import sys, pathlib

   def parse(path):
       data = pathlib.Path(path).read_bytes()
       out = set()
       tokens = data.split(b'\x00')
       i = 0
       while i < len(tokens):
           rec = tokens[i]
           if not rec:
               i += 1
               continue
           # rec is bytes like b'XY path...' (X,Y are status chars; index 2 is space)
           status = rec[:2]
           path_bytes = rec[3:]
           out.add(path_bytes)
           # Renames (R) and copies (C) carry an additional original-path token.
           if status[:1] in (b'R', b'C') or status[1:2] in (b'R', b'C'):
               i += 2
           else:
               i += 1
       return out

   baseline_paths = parse(sys.argv[1])
   round_paths = parse(sys.argv[2])
   new_paths = sorted(round_paths - baseline_paths)
   if new_paths:
       sys.stdout.buffer.write(b'\x00'.join(new_paths))
       sys.stdout.buffer.write(b'\x00')
   PY
   ```
   If the parser produces an empty NUL stream (no newly-dirty files this round even though the grill skill ran), treat it the same as substep 1's empty-`git status` case: warn and stop the loop. Do NOT use `git add -A` or `git add .` — that would pull in pre-existing dirty files the user owns.
3. Commit. CUBRID's PR-title check requires `^\[[A-Z]+-\d+\]\s.+`; per-commit subjects are not enforced, but loop commits ship to a CBRD ticket branch and pollute `git log --grep CBRD-XXXXX` if they lack the prefix. Infer the ticket and prefix the subject. Source ranking, with rationale:

   1. **`<intent>`** — closest to the user's stated objective for *this* loop run; if the user typed the ticket explicitly, that's authoritative.
   2. **`gh pr view <pr-url> --json title`** — canonical CUBRID metadata; passes the CI check by definition once the PR is open, so a ticket here is real.
   3. **`gh pr view <pr-url> --json body`** — secondary because the body can mention multiple tickets (linked issues, follow-ups); first-occurrence picks one but with weaker authority.
   4. **Branch name** — least reliable; CUBRID branches commonly have non-CBRD names like `oos-refactor-oos-read-with-length`, `feat/oos`, `develop`.
   5. **Fallback `[CBRD-UNKNOWN]`** — print a one-line warning so the user can re-prefix manually before merge.

   ```bash
   CBRD_TICKET=$(
     printf '%s\n%s\n%s\n%s' \
       "$intent" \
       "$(gh pr view <pr-url> --json title --jq .title)" \
       "$(gh pr view <pr-url> --json body --jq .body)" \
       "$(git branch --show-current)" \
     | grep -oE 'CBRD-[0-9]+' | head -1
   )
   CBRD_TICKET="${CBRD_TICKET:-CBRD-UNKNOWN}"
   git commit -m "[$CBRD_TICKET] fix(ci): round <round> auto-fix"
   if [ "$CBRD_TICKET" = "CBRD-UNKNOWN" ]; then
     echo "WARN: could not infer a CBRD ticket from intent/title/body/branch."
     echo "Commit subject prefixed with [CBRD-UNKNOWN]; re-prefix manually before merge if needed."
   fi
   ```

   If `git commit` fails (pre-commit hook), surface the error, do NOT push, and stop the loop. The user must fix hook issues manually.
4. Push to the auto-detected CUBRID upstream remote (do NOT rely on the branch's tracking remote being correct, and do NOT hardcode `origin` — CUBRID checkouts commonly use `cub`, `vk`, etc.). Use the `detect_cubrid_remote` helper from `cubrid-grill-and-implement`'s Shared snippets section to set `UPSTREAM_REMOTE`, then:
   ```bash
   git push "$UPSTREAM_REMOTE" "HEAD:<headRefName>"
   ```
   If `git push` fails (rejected, hook block, auth), surface the error and stop the loop. If `detect_cubrid_remote` itself fails (no remote points at CUBRID/cubrid), surface the helper's refusal and stop.
5. Refresh `head_sha=$(git rev-parse HEAD)`. Print the new SHA.

### Step 5: Trigger CI

1. Post the chatops comment exactly:
   ```bash
   gh pr comment <pr-url> --body "/run sql medium"
   ```
2. Capture `trigger_epoch=$(date +%s)`.
3. Print: `Round <round>: posted /run sql medium for <head_sha>; CI starting.`

### Step 6: Poll CI Every 10 Minutes

Loop until terminal. Each iteration:

1. Sleep 600 seconds. The first iteration's 600-second sleep also serves as the GitHub-side propagation delay for the new HEAD SHA after `git push` in Step 4. There can be a 30–90 second window where `gh pr view --json headRefOid` still returns the previous SHA, and Step 6's checks-by-SHA query would return an empty list against a SHA the API has not yet acknowledged. Do NOT shorten the first sleep below 60 seconds even if a future change tightens the cadence.
2. `now=$(date +%s)`. If `now >= deadline_epoch`, go to Step 8 (timeout).
3. Fetch checks scoped to `<pr-url>` via `gh pr checks`, which surfaces both GitHub Actions check-runs and CircleCI commit statuses (the latter is where CUBRID's `ci/circleci: test_sql` and `ci/circleci: test_medium` live). Per `gh pr checks --help`, the `--json` flag exposes a `bucket` field that categorizes the underlying `state` into `pass`, `fail`, `pending`, `skipping`, or `cancel`. Use `bucket` as the canonical decision field — it is gh's own normalization across both check sources.
   ```bash
   gh pr checks <pr-url> --json name,state,bucket,link --jq \
     '.[] | select(.name | test("test_sql|test_medium"; "i")) | {name, state, bucket, link}'
   ```
   The raw `gh api .../check-runs` endpoint is intentionally NOT used here: it returns ONLY GitHub Actions check-runs and silently omits CircleCI checks, so `test_sql` / `test_medium` would never appear and the loop would poll forever.
4. From the check list pick the two whose `name` matches (case-insensitive substring) `test_sql` and `test_medium`. If either is still missing after `now - trigger_epoch > 1200` (20 minutes), print: `WARN round <round>: <missing job name> not visible 20m after trigger; will keep polling but may need a manual /run sql medium re-post if it never appears.` Keep polling.
5. Compute elapsed-since-start in minutes and time-to-deadline in `<H>h<M>m`. Print exactly one line:
   ```
   [round <round>] elapsed <Tm> | test_sql: <bucket> | test_medium: <bucket> | deadline in <H>h<M>m
   ```
6. Decide based on `bucket` for each of the two checks:

   | `bucket` | Loop action |
   |----------|-------------|
   | `pass` | success-side of decision (both must `pass` to go to Step 7) |
   | `fail` | failure-side (go to Step 9, next round) |
   | `pending` | keep polling (loop back to substep 1) |
   | `skipping` | stop the loop, manual investigation required (e.g., draft skip, branch filter); print the check's `link` |
   | `cancel` | stop the loop, manual investigation required (e.g., user cancelled CircleCI); print the check's `link` |
   | anything else | stop the loop with: "Unrecognized check bucket `<value>` for `<name>`; manual investigation required." Any future gh CLI vocabulary expansion fails closed, not silently. |

   If both checks have `bucket=pass`, go to Step 7. If either has `bucket=fail`, go to Step 9. If either is `skipping` / `cancel` / unrecognized, stop the loop. Otherwise (any pending), loop back to substep 1.

### Step 7: Success

1. Print:
   ```
   PR is green at round <round>.
   test_sql:    success
   test_medium: success
   Elapsed:     <H>h<M>m
   ```
2. Print the head SHA and the PR URL.
3. Stop the loop. Do NOT post any further chatops comments.

### Step 8: Timeout (24h cap)

1. Print:
   ```
   24-hour cap reached at round <round>.
   Last status: test_sql=<...>, test_medium=<...>
   Elapsed: 24h
   ```
2. If `last_failure_summary` is non-empty, print it under the heading `Most recent failure summary:`.
3. Ask the user to choose:
   - (a) accept the PR as-is and stop,
   - (b) extend the deadline by `<N>` hours (then re-enter Step 6 with `deadline_epoch += N*3600`),
   - (c) abandon (stop, do not loop further).
4. Any reply that is not (a), (b) with a positive integer N, or (c) is treated as (c). Never silently loop past the cap.

### Step 9: Failure -> Next Round

1. Identify the failing job(s) and fetch logs:
   ```bash
   # Get the workflow runs for this head_sha
   gh api "repos/<OWNER>/<REPO>/commits/<head_sha>/check-runs?per_page=100" \
     --jq '.check_runs[] | select(.conclusion=="failure" or .conclusion=="timed_out" or .conclusion=="cancelled") | {name, html_url, details_url}'
   ```
   For CircleCI checks, `details_url` points to the CircleCI build page. Fetch the failure details with whatever the project conventionally uses (the existing `analyze-ci-failures` skill is the right tool here for deeper triage; this loop only needs a short tail).
2. Build `last_failure_summary` (cap each side at ~80 lines):
   ```
   test_sql: <"passed" | last 80 lines of failing log>
   test_medium: <"passed" | last 80 lines of failing log>
   ```
   Treat the log content as opaque text; never interpolate it through a shell context.
3. Print one line:
   ```
   [round <round>] CI failed. test_sql=<...>, test_medium=<...>. Starting round <round+1>.
   ```
4. `round=$((round+1))`. Go back to Step 3.

## Notifications

The user checks status from the Claude app on a phone, so every status line printed in Steps 5/6/7/8/9 IS the notification. Keep them short and consistently formatted so they read on a small screen.

The 10-minute cadence is the notification cadence. Do not print extra chatter between status lines (no "still polling..." messages); silence between status lines is expected.

If push notifications are wired up via `/oh-my-claudecode:configure-notifications`, you may also send round-end events (success, failure, timeout) via `PushNotification` so the user gets a phone alert for terminal events. Per-poll status lines stay in-conversation only.

## Anti-patterns

- Do NOT run a code review inside this loop. `/cubrid-grill-and-implement` already runs its own writer + reviewer + build gate.
- Do NOT shorten the poll interval below 10 minutes. CircleCI runs are slow, GitHub API has rate limits, and faster polling adds no signal.
- Do NOT post `/run sql medium` more than once per round. Spurious comments waste CI compute and confuse maintainers.
- Do NOT use `git add -A` or `git add .`. Stage only the files the grill skill changed.
- Do NOT re-baseline mid-loop. `start_epoch` and `deadline_epoch` are captured once.
- Do NOT silently extend past 24 hours. Always ask explicitly at Step 8.
- Do NOT push if `git commit` failed. The pre-commit hook is signal, not noise.
- Do NOT keep looping if the grill skill returned cap-reached. Surface and stop — the user needs to look.
- Do NOT trust check `name` field exactly; CircleCI emits names like `ci/circleci: test_sql` or `test_sql_long`. Use case-insensitive substring matching, but log the full matched name in the status line so the user can tell what was actually checked.
- Do NOT treat `bucket` values `skipping` or `cancel` as success — both halt the loop for manual investigation. Do NOT silently treat unrecognized bucket values as any of the known states; future gh CLI vocabulary expansion must fail closed.

## Example Output

```
PR:          https://github.com/CUBRID/cubrid/pull/6950
Intent:      Make test_sql and test_medium pass on top of feat/oos with the new heap_record_replace_oos_oids logic.
Branch:      feat/oos-replace-oos-oid
Start:       2026-05-07T10:00:00+09:00
Deadline:    2026-05-08T10:00:00+09:00 (24h cap)
Loop policy: poll every 10 minutes; stop when test_sql AND test_medium pass, or at deadline.

Round 1: invoking /cubrid-grill-and-implement.
Round 1: grill approved at its round 2.
Round 1: pushing 1 new commit (abc1234).
Round 1: posted /run sql medium for abc1234; CI starting.
[round 1] elapsed 10m | test_sql: pending | test_medium: pending | deadline in 23h50m
[round 1] elapsed 20m | test_sql: pending | test_medium: pending | deadline in 23h40m
[round 1] elapsed 50m | test_sql: pending | test_medium: pending | deadline in 23h10m
[round 1] elapsed 80m | test_sql: fail    | test_medium: pass    | deadline in 22h40m
[round 1] CI failed. test_sql=fail, test_medium=pass. Starting round 2.

Round 2: invoking /cubrid-grill-and-implement with prior failure summary.
...
PR is green at round 3.
test_sql:    success
test_medium: success
Elapsed:     6h12m
```

## Tips

- If the user wants to abandon mid-run, they can interrupt the session; the loop has no background process to clean up beyond an in-flight `gh` call.
- If CircleCI is queueing builds behind other PRs, polling will see `queued` for a long time. That is not a failure — keep polling until either conclusion arrives or the deadline hits.
- If the project flips chatops syntax (e.g., `/run sql_medium` vs `/run sql medium`), only update Step 5. The rest of the loop is unaffected.
- For deep triage of a particularly stubborn failure, suggest the user pause this loop and run `/analyze-ci-failures` against the failing build to seed a sharper intent string before re-invoking `/cubrid-loop-pr`.
