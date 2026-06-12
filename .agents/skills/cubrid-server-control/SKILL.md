---
name: cubrid-server-control
description: "Start, stop, or restart a local CUBRID database server safely via a bundled wrapper script. ALWAYS use this skill (never the raw `cubrid server ...` command) whenever you are about to start/stop/restart/bounce a CUBRID server or a specific database — because the raw command HANGS forever when its output is captured by a pipe (the agent Bash tool, CI, `$(...)`, `| tee`). Use when the user says 'start/stop/restart the server', 'bounce cubrid', 'cubrid server start|stop|restart <db>', '서버 켜/꺼/재시작', 'demodb 띄워/내려', or whenever a task requires the CUBRID server to be running or stopped."
argument-hint: "<start|stop|restart|status> [db_name]"
---

# CUBRID Server Control (hang-proof)

Control the local CUBRID server lifecycle. **All server start/stop/restart MUST go
through the bundled wrapper** — never run `cubrid server ...` directly.

## The one rule

`cubrid server start <db>` spawns the long-lived `cub_server`/`cub_master` daemons,
which inherit the calling process's stdout/stderr. When those fds are a **pipe**
— exactly what the agent Bash tool, CI, `$(...)`, `| tee`, or `> >(...)` create —
the daemon holds the pipe open, the reader never gets EOF, and the call **HANGS
FOREVER**. The wrapper redirects the command's output to a regular log file
(no reader → no hang), waits for completion, and prints a short, capture-safe
summary. So:

- ✅ Run the wrapper. Its output is safe to capture/pipe/redirect.
- ❌ NEVER run `cubrid server start|stop|restart` yourself — not even with
  `> log`, `2>&1`, `| tee`, or inside `$(...)`. Always delegate to the wrapper.

## Usage

```bash
.claude/skills/cubrid-server-control/scripts/cubrid-server-ctl.sh <start|stop|restart|status> [db_name]
```

Examples:

```bash
# start / stop / restart a specific database
scripts/cubrid-server-ctl.sh start demodb
scripts/cubrid-server-ctl.sh restart demodb
scripts/cubrid-server-ctl.sh stop demodb

# all databases (omit the name)
scripts/cubrid-server-ctl.sh start
scripts/cubrid-server-ctl.sh stop

# just check what's running (safe, read-only)
scripts/cubrid-server-ctl.sh status
```

The wrapper prints `RESULT: OK | FAILED | TIMEOUT`, the captured cubrid output,
and a `cubrid server status` snapshot. It exits with cubrid's own exit code
(`0` = success), so callers can branch on `$?`.

## Workflow checklist

When you need to start/stop/restart a CUBRID server:

1. Resolve the wrapper path (prefer the path relative to repo root, or the
   absolute `/home/cubrid/dev/cubrid/.claude/skills/cubrid-server-control/scripts/cubrid-server-ctl.sh`).
2. Invoke it with the action and optional `db_name`. Let the Bash tool capture
   its output normally — that is safe.
3. Read the `RESULT:` line and the status snapshot to confirm the outcome.
   On `FAILED`/`TIMEOUT`, open the log file printed in the `[log: ...]` header.
4. Never fall back to a raw `cubrid server ...` call if the wrapper is present.

## Notes

- Requires `$CUBRID` set (auto-detected; falls back to `cubrid` on `PATH`).
- Logs go to `<repo>/.claude/scratch/cubrid-server-ctl/` (override with
  `CUBRID_SERVER_CTL_LOGDIR`). Never `/tmp`.
- Force-kill backstop after `CUBRID_SERVER_CTL_TIMEOUT` seconds (default 120).
- `status` is read-only and never hangs; the others manage daemons.
