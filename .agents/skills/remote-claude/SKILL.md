---
name: remote-claude
description: >-
  Supervisor skill: run claude inside tmux on remote workers (cubrid@192.168.6.30/.32/.33),
  dispatch tasks to them, watch for 5h/weekly token exhaustion, auto-rotate the account
  (work <-> personal) and resume so work never stalls. The remote runs PLAIN Claude Code
  (no custom skills/plugins), so any SPEC/plan authored here must be fully self-contained and
  runnable by a vanilla claude on its own.
  Triggers (user usually types these in Korean): "30/32/33에서 돌려/일 시켜", "원격에서 작업",
  "remote claude", "토큰 소진 계정 전환 resume", "원격 감시", "ssh cubrid@192.168.6 tmux claude",
  "원격용 spec/plan 만들어".
---

# remote-claude — remote claude dispatch & supervision

The local claude acts as a **supervisor**: it dispatches work to a claude running on a remote
host and, when tokens run out, rotates the account so the task continues without interruption.

## Roles
- **Me (supervisor, local)**: write/receive the SPEC -> dispatch -> start the watch daemon ->
  handle anything that needs judgment (steering, follow-up, reporting).
- **`rc-watch.sh` (background daemon, local)**: handles the mechanical work unattended —
  detect exhaustion -> rotate account + resume, nudge on idle.
- **Remote claude (inside tmux)**: takes SPEC.md and runs it end-to-end with no human in the
  loop. **Plain Claude Code — no custom skills/plugins.**

---

## Environment facts (verified 2026-06-01)
| Item | Value |
|---|---|
| Workers | `cubrid@192.168.6.30` · `.32` · `.33` (connect by IP directly, no ssh alias). Short tag = last octet `30/32/33` |
| Readiness | `.32`/`.33` ready. `.30` setup in progress (claude/tmux/repo may be missing -> dispatch preflight blocks it) |
| Policy | **A different task per worker** (one task is not split across workers). Default account `personal`. |
| Accounts | Remote `claude-acct use work|personal` (both have tokens registered; login auto-runs `use work`) |
| On exhaustion | On that worker, switch `work`<->`personal`, then `claude --continue` to resume the same conversation |
| Work repo | `~/dev/cubrid` (branch differs per worker/task — stated in the SPEC) |
| Control files (remote) | `~/rc/SPEC.md` (task body) · `~/rc/PROGRESS.md` · `~/rc/BLOCKED.md` · `~/rc/DONE` (completion signal) · `~/rc/PAUSED` |
| tmux session | One `rc` session per host |
| Remote tools | claude 2.1.x, tmux, node v24, bash. **No custom skills/plugins (plain Claude Code).** |
| Local state | `~/.claude/scratch/remote-claude/` (`<short>.state`, `events.log`, `ATTENTION`, `watch.pid`, logs) |

**Token values are never stored anywhere.** Account switching is delegated entirely to the
remote `claude-acct` (which already holds the tokens).

---

## Scripts (`scripts/`)
```
rc-dispatch.sh <30|32|33> <work|personal> <spec.md> [--force]  # dispatch a new task (host = last octet)
rc-watch.sh [30 32 33]                                         # background watch daemon (exhaust->rotate, idle->re-issue)
rc-poll.sh <30|32|33>                                          # one-shot status poll (STATE/CHANGED/BLOCKED/RESET/TAIL)
rc-rotate.sh <30|32|33> <work|personal>                       # switch account + resume the conversation
rc-steer.sh <30|32|33> "<message>" [--interrupt]              # inject direction/nudge into a running claude
rc-down.sh <30|32|33>                                         # tear session down (archive final screen/PROGRESS)
preseed.js                                                     # (helper) seed start-gate flags — used automatically by dispatch
```
Path: `~/dev/cubrid/.claude/skills/remote-claude/scripts/`. They all source `rc-env.sh`
(no need to source it yourself — each script does it).

---

## Workflow 1 — authoring a SPEC (self-contained)
You will usually **bring a finished SPEC yourself**. Whatever produces it, the output SPEC.md
must be runnable by a **vanilla claude alone**, because the remote cannot ask follow-up questions
and has no custom tooling. Rules:

- ✅ **Self-contained**: the remote can't ask back -> put **all** context, decisions, acceptance
  criteria, and out-of-scope guards **directly in the body**.
- ✅ **Self-driving**: a checklist + the completion protocol (below) so it runs on its own. If you
  need parallelism, only optionally use claude's **native Task (sub-agent)** tool (it must still
  work without it).
- ✅ **No special tooling**: do not reference any local-only skill, plugin, custom agent, or MCP
  server by name — the remote has none of them. Plain shell + claude built-ins only.
- ✅ **Verification**: write the build/test **commands verbatim** and state the pass criteria.

Template: copy `templates/SPEC.template.md` and fill it in. Local stash location:
`~/.claude/scratch/remote-claude/specs/<name>.md`.

**Completion protocol** (must be in the SPEC; it's in the template):
- After finishing each item: set `[x]` in `~/rc/SPEC.md` + one line in `~/rc/PROGRESS.md`.
- Hard blockers go in `~/rc/BLOCKED.md`; keep going with other items.
- All done + verified -> `echo done > ~/rc/DONE`. Mid-run stop -> `echo "<reason>" > ~/rc/PAUSED`.
- Completion is detected via the **file sentinel** (`~/rc/DONE`) — screen-text matching collides
  with the kickoff instructions and false-positives, so we use the file. (The dispatch kickoff
  prompt injects this same protocol.)

---

## Workflow 2 — dispatch
Give each worker a **different task**. e.g. 33 = refactor, 32 = bugfix. (Use 30 once its setup is done.)
```bash
S=~/dev/cubrid/.claude/skills/remote-claude/scripts
$S/rc-dispatch.sh 33 personal ~/.claude/scratch/remote-claude/specs/refactor.md
$S/rc-dispatch.sh 32 personal ~/.claude/scratch/remote-claude/specs/bugfix.md
# host can be the 30/32/33 short tag, or cubrid@192.168.6.NN / a raw IP
```
- Default account is `personal`. If a session already exists it is refused -> to overwrite a running
  task use `--force`.
- dispatch does: scp SPEC -> create tmux `rc` -> `claude-acct use <acct>` -> `cd ~/dev/cubrid` ->
  start `claude --dangerously-skip-permissions` -> inject the kickoff prompt -> write local `.state`.
- Because it runs unattended it uses permission bypass (`--dangerously-skip-permissions`). For risky
  work, guard it in the SPEC's "Out of scope" section.

---

## Workflow 3 — watching (the core)
After dispatch, start a **background watch daemon**. It handles exhaust -> rotate -> resume unattended.
```bash
$S/rc-watch.sh 32 33 &     # run via the Bash tool's run_in_background (or nohup). No args = every worker with a .state
```
Daemon behavior (polls every 120s):
- **`limit_5h`**: auto-rotate `personal->work` (or back) + resume. If you just rotated and hit 5h
  again (= both exhausted at once), wait without blocking until `WAIT_UNTIL` (default 30 min), then resume.
- **`limit_weekly`**: mark that account `WEEKLY_DEAD`, switch to the other. **If both accounts are
  weekly-exhausted, raise the `ATTENTION` flag** -> I handle it.
- **`idle` twice in a row**: native `/loop` owns progress, so persistent idle means the loop ended
  early -> re-issue `/loop`. No progress after 4 tries -> `ATTENTION` (stuck).
- **`done` (`~/rc/DONE` created) / `error` / `no_session`**: raise `ATTENTION` + log to `events.log`,
  stop watching that host (the session is preserved).

### What I (supervisor) do — light polling
The daemon handles the mechanical part, so I only need to **check `ATTENTION`/`events.log` occasionally**.
```bash
ST=~/.claude/scratch/remote-claude
[ -f $ST/ATTENTION ] && echo "⚠ needs attention" ; tail -n 20 $ST/events.log
for s in $ST/*.state; do echo "== $s =="; cat "$s"; done
```
- For unattended continuation, set a self-polling loop with **`ScheduleWakeup`**. Cadence
  (mind the 5-min cache TTL):
  - active task running / just acted -> `~270s`
  - all healthy and running long -> `~1200s`
  - token reset / long wait -> `1800–3600s`
- On each wake: if `ATTENTION` exists, handle it (below) then `rm $ST/ATTENTION`; otherwise schedule
  the next wakeup. If the daemon is dead too (no `watch.pid` process) and no host is active, end the loop.
- (The user can also drive this skill via `/loop` — same effect.)

### Handling ATTENTION (needs judgment)
| Event | Action |
|---|---|
| `DONE` | `rc-down.sh <h>` to archive PROGRESS/final screen -> summarize to the user. If there's follow-up, dispatch a new SPEC. |
| `BOTH_WEEKLY_DEAD` | Report to the user. Wait until reset (set `WAIT_UNTIL` manually if you want auto-resume) or keep going on other hosts only. |
| `ROTATE_FAILED` | `ssh <h> tmux attach -t rc` to check. Retry `rc-rotate.sh` if needed, or re-dispatch with `--force` (safe thanks to SPEC checkpoints). |
| `STUCK` | Look at the pane (TAIL from `rc-poll.sh <h>`) and give a specific `rc-steer.sh <h> "<instruction>"`. |
| `ERROR`/`no_session` | Check pane/session, then recover or re-dispatch. |

---

## Steering
To change direction or give a hint mid-run:
```bash
$S/rc-steer.sh 33 "Tests: unit only, skip integration. And start with the leak in src/foo.c."
$S/rc-steer.sh 33 "Stop the current approach and try a different one." --interrupt   # Esc first if it's generating
```

## Completion / cleanup
- After completion (`DONE`): `rc-down.sh <h>` -> close the session + archive
  `~/.claude/scratch/remote-claude/<h>.final.log` and `<h>.progress.md`.
- Report to the user **with evidence**: what was done (PROGRESS), whether verification passed,
  and any blockers (BLOCKED.md). If it didn't pass, say so plainly.

---

## Tuning / gotchas
- **Exhaustion-banner regexes**: the `limit_5h`/`limit_weekly` patterns in `rc-poll.sh` are
  best-effort. **The first time you actually hit exhaustion**, check the exact wording via
  `rc-poll.sh <h>` TAIL and `<h>.final.log`, and refine the patterns (especially the 5h vs weekly
  distinction).
- **`/loop`·`/goal` (assumed built-in)**: assumed present in vanilla claude, so default
  `RC_KICKOFF_MODE=loop`. dispatch starts the self-loop with `/loop <mission>` (the mission tells
  it to stop once `~/rc/DONE` exists), and rotate-resume / idle-recovery re-issue via `/loop`. The
  mission text lives in one place — `rc_mission()` in `rc-env.sh`. Use `RC_KICKOFF_MODE=goal` for
  `/goal`, or `RC_KICKOFF_MODE=prompt` for a plain prompt.
- **Start gate (preseed, verified)**: three gates block unattended startup — login-method choice /
  folder trust / bypass-permissions warning. dispatch runs `scripts/preseed.js` to seed
  `~/.claude.json` with `hasCompletedOnboarding`·`theme`·`bypassPermissionsModeAccepted`·
  `projects[repo].hasTrustDialogAccepted` and clear them all. `rc_wait_ready` confirms the chat is
  ready; if an unexpected gate remains, dispatch reports `exit 6` (then `ssh <h> tmux attach -t rc`
  to inspect).
- **Noise**: the remote `.bashrc` prints an "active account…" banner on every ssh -> the `RC_NOISE`
  filter in `rc-env.sh` strips it from state decisions. (Those patterns are literal Korean strings
  matching the remote banner — do not translate them.)
- **State reset**: when a task ends, `rm ~/.claude/scratch/remote-claude/<h>.state
  ~/.claude/scratch/remote-claude/<h>.lasthash` (so stale state like `WEEKLY_DEAD_*` doesn't leak
  into the next task).
