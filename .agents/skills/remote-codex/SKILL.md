---
name: remote-codex
description: >-
  Supervisor skill: run the codex CLI inside tmux on remote workers (cubrid@192.168.6.30/.32/.33),
  dispatch tasks to them via codex's /goal long-running mode, watch the status line for 5h/weekly
  limit exhaustion, and — since codex has a single ChatGPT login (no account rotation) — wait for the
  5h limit to reset then auto-resume (codex resume --last) so work continues; weekly exhaustion raises
  ATTENTION. The remote runs PLAIN codex (shell access / YOLO), so any SPEC authored here must be
  fully self-contained and runnable by codex on its own.
  Triggers (user usually types these in Korean): "codex로 30/32/33에서 돌려/일 시켜", "원격 codex",
  "remote codex", "codex 원격 작업", "codex 한도 리셋 대기 resume", "codex 원격 감시",
  "ssh cubrid@192.168.6 tmux codex", "codex용 spec 만들어". Sibling of remote-claude (codex variant).
---

# remote-codex — remote codex dispatch & supervision

The local claude acts as a **supervisor**: it dispatches work to **codex** running on a remote host
and, when the 5h limit runs out, waits for the reset and resumes so the task continues. This is the
codex sibling of `remote-claude` — same supervision model, adapted to codex's CLI.

**Key difference from remote-claude:** codex on these workers has a **single ChatGPT login**
(`~/.codex/auth.json`) and **no account-rotation helper**. So instead of swapping accounts on
exhaustion, the watch daemon **waits for the 5h limit to reset** (it polls codex's `5h N% left`
status line) and then resumes. Weekly exhaustion can't be worked around with one account → ATTENTION.

To coexist with remote-claude on the same worker, remote-codex uses **distinct names**:
tmux session `rcx`, control dir `~/rcx`, local state `~/.claude/scratch/remote-codex/`.
(So you can run claude on 32 and codex on 33 at the same time.)

## Roles
- **Me (supervisor, local)**: write/receive the SPEC -> dispatch -> start the watch daemon ->
  handle anything that needs judgment (steering, follow-up, reporting).
- **`rcx-watch.sh` (background daemon, local)**: mechanical work unattended — detect 5h exhaustion ->
  wait for reset -> resume; nudge on idle; alert on weekly/done/error.
- **Remote codex (inside tmux)**: takes SPEC.md and runs it end-to-end via `/goal`, with no human in
  the loop. **Plain codex CLI — shell access (YOLO), no custom skills/plugins.**

---

## Environment facts (verified 2026-06-18, codex-cli 0.141.0)
| Item | Value |
|---|---|
| Workers | `cubrid@192.168.6.30` · `.32` · `.33` (connect by IP directly, no ssh alias). Short tag = last octet. **codex is set up on all three.** |
| Launch | `command codex --dangerously-bypass-approvals-and-sandbox` (YOLO / Full Access — bypasses the shell alias so the flag isn't doubled) |
| Resume | `codex resume --last` — restores the full session history + context (verified) |
| Autonomy | `/goal <one-line mission>` — codex's long-running-task driver (there is **no** `/loop`). Shows `Pursuing goal (Ns)` → `Goal achieved (Ns)` |
| Auth | Single ChatGPT login in `~/.codex/auth.json`. **No `codex-acct` rotation.** |
| On 5h exhaustion | **Wait** for the limit to reset (watch polls `5h N% left`), then `codex resume --last` + re-issue `/goal` |
| On weekly exhaustion | Can't rotate → raise `ATTENTION`; the supervisor decides whether to wait (days) |
| Quit TUI | two `Ctrl-C` → drops to shell |
| Start gates | **none** — `~/.codex/config.toml` already trusts `~/dev/cubrid` and YOLO skips approvals (no preseed needed) |
| Status line | `model · dir · project · branch · <Ready\|Working> · Full Access · Context N% left · 5h N% left · weekly N% left` — the **authoritative** state signal |
| Work repo | `~/dev/cubrid` (branch differs per worker/task — stated in the SPEC) |
| Control files (remote) | `~/rcx/SPEC.md` (task body) · `PROGRESS.md` · `BLOCKED.md` · `DONE` (completion signal) · `PAUSED` |
| tmux session | One `rcx` session per host |
| Local state | `~/.claude/scratch/remote-codex/` (`<short>.state`, `events.log`, `ATTENTION`, `watch.pid`, logs) |

---

## Scripts (`scripts/`)
```
rcx-dispatch.sh <30|32|33> <spec.md> [--force]   # dispatch a new task (no account arg — single login)
rcx-watch.sh [30 32 33]                          # background watch daemon (5h→wait+resume, idle→re-goal, weekly→ATTENTION)
rcx-poll.sh <30|32|33>                            # one-shot status poll (STATE/CHANGED/BLOCKED/RESET/TAIL/FIVEH/WEEKLY)
rcx-resume.sh <30|32|33>                          # resume the conversation (codex resume --last + re-issue /goal)
rcx-steer.sh <30|32|33> "<message>" [--interrupt] # inject direction/nudge into a running codex
rcx-down.sh <30|32|33>                            # tear session down (archive final screen/PROGRESS)
```
Path: `~/dev/cubrid/.claude/skills/remote-codex/scripts/`. They all source `rcx-env.sh`
(no need to source it yourself — each script does it). **No `preseed.js`** (codex needs no start-gate seeding).

---

## Workflow 1 — authoring a SPEC (self-contained)
You will usually **bring a finished SPEC yourself**. The output SPEC.md must be runnable by **codex
alone**, because the remote cannot ask follow-up questions. Rules:

- ✅ **Self-contained**: put **all** context, decisions, acceptance criteria, and out-of-scope guards
  **directly in the body**.
- ✅ **Self-driving**: a checklist + the completion protocol (below). codex reads the repo's
  `AGENTS.md` automatically, but do not reference any local-only skill/plugin/MCP server by name.
- ✅ **Verification**: write the build/test **commands verbatim** and state the pass criteria.

Template: copy `templates/SPEC.template.md` and fill it in. Local stash:
`~/.claude/scratch/remote-codex/specs/<name>.md`.

**Completion protocol** (must be in the SPEC; it's in the template):
- After finishing each item: set `[x]` in `~/rcx/SPEC.md` + one line in `~/rcx/PROGRESS.md`.
- Hard blockers go in `~/rcx/BLOCKED.md`; keep going with other items.
- All done + verified -> `echo done > ~/rcx/DONE`. Mid-run stop -> `echo "<reason>" > ~/rcx/PAUSED`.
- Completion is detected via the **file sentinel** (`~/rcx/DONE`) — the `/goal` kickoff prompt injects
  this same protocol.

---

## Workflow 2 — dispatch
Give each worker a **different task**. (One task per worker; not split across workers.)
```bash
S=~/dev/cubrid/.claude/skills/remote-codex/scripts
$S/rcx-dispatch.sh 33 ~/.claude/scratch/remote-codex/specs/refactor.md
# host can be the 30/32/33 short tag, or cubrid@192.168.6.NN / a raw IP
```
- If a session already exists it is refused -> use `--force` to overwrite a running task.
- dispatch does: preflight (codex logged in + tmux + repo) -> place SPEC -> create tmux `rcx` ->
  `cd ~/dev/cubrid` -> start codex (YOLO) -> wait ready -> inject `/goal <mission>` -> write local `.state`.
- Because it runs unattended it uses the bypass flag (YOLO). For risky work, guard it in the SPEC's
  "Out of scope" section.

---

## Workflow 3 — watching (the core)
After dispatch, start a **background watch daemon**. It handles 5h-exhaust -> wait -> resume unattended.
```bash
$S/rcx-watch.sh 33 &     # run via the Bash tool's run_in_background (or nohup). No args = every worker with a .state
```
Daemon behavior (polls every 120s):
- **`limit_5h`**: set a wait flag and **keep polling**; when the `5h N% left` recovers (limit reset),
  auto-`rcx-resume`. No account switching.
- **`limit_weekly`**: raise `ATTENTION` (single account — can't rotate) and stop watching that host.
- **`idle` twice in a row**: `/goal` owns progress, so persistent idle means the goal ended early ->
  re-issue `/goal`. No progress after 4 tries -> `ATTENTION` (stuck).
- **`done` (`~/rcx/DONE` created) / `error` / `no_session`**: raise `ATTENTION` + log to `events.log`,
  stop watching that host (the session is preserved).
- **cgroup PID pressure** (container limit 2048): steer a process-leak cleanup before fork locks out.

### What I (supervisor) do — light polling
The daemon handles the mechanical part, so I only **check `ATTENTION`/`events.log` occasionally**.
```bash
ST=~/.claude/scratch/remote-codex
[ -f $ST/ATTENTION ] && echo "⚠ needs attention" ; tail -n 20 $ST/events.log
for s in $ST/*.state; do echo "== $s =="; cat "$s"; done
```
- For unattended continuation, set a self-polling loop with **`ScheduleWakeup`**. Cadence (mind the
  5-min cache TTL): active/just-acted -> `~270s`; all healthy -> `~1200s`; waiting on a 5h reset ->
  `1800–3600s`.
- On each wake: if `ATTENTION` exists, handle it (below) then `rm $ST/ATTENTION`; otherwise schedule
  the next wakeup. If the daemon is dead (no `watch.pid` process) and no host is active, end the loop.

### Handling ATTENTION (needs judgment)
| Event | Action |
|---|---|
| `DONE` | `rcx-down.sh <h>` to archive PROGRESS/final screen -> summarize to the user. If there's follow-up, dispatch a new SPEC. |
| `WEEKLY_LIMIT` | Report to the user. Single account → decide: wait days for the weekly reset, or move the task to another worker / to remote-claude. |
| `RESUME_FAILED` | `ssh <h> tmux attach -t rcx` to check. Retry `rcx-resume.sh`, or re-dispatch with `--force` (safe thanks to SPEC checkpoints). |
| `STUCK` | Look at the pane (TAIL from `rcx-poll.sh <h>`) and give a specific `rcx-steer.sh <h> "<instruction>"`. |
| `PID_HIGH`/`PID_CRIT` | Confirm the cleanup steer landed; if not, `rcx-steer.sh <h> "..."` a manual cleanup. |
| `ERROR`/`no_session` | Check pane/session, then recover (`rcx-resume.sh`) or re-dispatch. |

---

## Steering
```bash
$S/rcx-steer.sh 33 "Tests: unit only, skip integration. Start with the leak in src/foo.c."
$S/rcx-steer.sh 33 "Stop the current approach and try a different one." --interrupt   # Esc first if generating
```

## Completion / cleanup
- After completion (`DONE`): `rcx-down.sh <h>` -> close the session + archive
  `~/.claude/scratch/remote-codex/<h>.final.log` and `<h>.progress.md`.
- Report to the user **with evidence**: what was done (PROGRESS), whether verification passed, and any
  blockers (BLOCKED.md). If it didn't pass, say so plainly.

---

## Tuning / gotchas
- **Limit detection**: `rcx-poll.sh` reads the numeric `5h N% left` / `weekly N% left` off the status
  line (0% → exhausted) plus a best-effort banner regex. **The first time you actually hit a limit**,
  capture the exact pane via `rcx-poll.sh <h>` TAIL + `<h>.final.log` and refine the regex — in
  particular confirm whether codex stays in the TUI or drops to a shell/modal at 0% (it determines
  whether `rcx-resume.sh` needs `codex resume --last` or just a re-`/goal`). `rcx-resume.sh` already
  handles both, but verify on first hit.
- **`/goal` is the loop**: codex keeps pursuing the goal until it thinks it's achieved. The mission
  tells it to only create `~/rcx/DONE` when every item is checked AND verification passes; if codex
  declares "Goal achieved" without `DONE`, the screen goes idle and the watch re-issues `/goal`.
- **Single account**: there is no `work`/`personal` to switch to. 5h limits reset on a rolling window
  (auto-resumed); weekly limits do not (ATTENTION → human decision).
- **Coexistence**: session `rcx` / dir `~/rcx` are distinct from remote-claude's `rc` / `~/rc`, so
  both skills can run on the same worker. Don't point both at the same task.
- **Noise**: the remote `.bashrc` prints an "active account…" banner on every ssh -> the `RCX_NOISE`
  filter in `rcx-env.sh` strips it from state decisions. (Literal Korean strings matching the banner —
  do not translate.)
- **State reset**: when a task ends, `rm ~/.claude/scratch/remote-codex/<h>.state
  ~/.claude/scratch/remote-codex/<h>.lasthash` (so stale flags like `WAITING_RESET` don't leak into
  the next task).
