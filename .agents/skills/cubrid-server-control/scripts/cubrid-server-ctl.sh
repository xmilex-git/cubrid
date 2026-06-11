#!/usr/bin/env bash
#
# cubrid-server-ctl.sh — hang-proof wrapper around `cubrid server start|stop|restart [db]`
#
# WHY THIS EXISTS
# --------------
# `cubrid server start <db>` launches the long-lived `cub_server` / `cub_master`
# daemons, which INHERIT the stdout/stderr file descriptors of the `cubrid`
# utility that spawned them. If those fds are a PIPE — which is exactly what
# happens when an agent (Claude Code's Bash tool), CI, `$(...)`, `... | tee`, or
# `> >(...)` captures the output — the daemon keeps the pipe's write end open
# forever. The reader never sees EOF and the call HANGS indefinitely.
#
# THE FIX
# -------
# Point the cubrid command's stdin at /dev/null and its stdout/stderr at a
# regular LOG FILE (a regular file has no reader waiting on EOF, so it can never
# cause this hang). The daemon then inherits the log-file fd, never the agent's
# capture pipe. This wrapper waits for the short-lived `cubrid` utility to finish,
# then prints a small, captured-safe summary to ITS OWN stdout and exits promptly
# — so the agent's pipe gets a clean EOF and never blocks.
#
# => Callers may capture THIS wrapper's output freely. Never run the raw
#    `cubrid server ...` command under a pipe/redirection yourself.
#
# USAGE
#   cubrid-server-ctl.sh <start|stop|restart|status> [db_name]
#
# ENV
#   CUBRID                       CUBRID install root (auto-detected if unset)
#   CUBRID_SERVER_CTL_LOGDIR     where to write logs (default: <repo>/.claude/scratch/cubrid-server-ctl)
#   CUBRID_SERVER_CTL_TIMEOUT    seconds before the cubrid call is force-killed (default: 120)
#
# EXIT CODE: forwards the underlying `cubrid` exit code (0 = success).

set -u

action="${1:-}"
db="${2:-}"

usage() {
  echo "usage: $(basename "$0") <start|stop|restart|status> [db_name]" >&2
}

case "$action" in
  start|stop|restart|status) ;;
  ""|-h|--help) usage; exit 2 ;;
  *) echo "ERROR: unknown action '$action'" >&2; usage; exit 2 ;;
esac

# --- locate the cubrid binary --------------------------------------------------
if [ -n "${CUBRID:-}" ] && [ -x "$CUBRID/bin/cubrid" ]; then
  CUBRID_BIN="$CUBRID/bin/cubrid"
elif command -v cubrid >/dev/null 2>&1; then
  CUBRID_BIN="$(command -v cubrid)"
else
  echo "ERROR: cannot find the 'cubrid' binary (set \$CUBRID or add it to PATH)" >&2
  exit 127
fi

# --- resolve a disk-backed log dir (NEVER /tmp) --------------------------------
# Prefer the project repo's .claude/scratch; fall back to the user's home scratch.
self="${BASH_SOURCE[0]}"
self="$(readlink -f "$self" 2>/dev/null || echo "$self")"
repo_root="$(cd "$(dirname "$self")/../../../.." 2>/dev/null && pwd || true)"

if [ -n "${CUBRID_SERVER_CTL_LOGDIR:-}" ]; then
  LOGDIR="$CUBRID_SERVER_CTL_LOGDIR"
elif [ -n "$repo_root" ] && [ -d "$repo_root/.claude" ]; then
  LOGDIR="$repo_root/.claude/scratch/cubrid-server-ctl"
else
  LOGDIR="$HOME/.claude/scratch/cubrid-server-ctl"
fi
mkdir -p "$LOGDIR" 2>/dev/null || { echo "ERROR: cannot create log dir '$LOGDIR'" >&2; exit 1; }

stamp="$(date +%Y%m%d-%H%M%S)"
LOG="$LOGDIR/${action}${db:+-$db}-$stamp.log"
TIMEOUT="${CUBRID_SERVER_CTL_TIMEOUT:-120}"

# --- run the command with fds DETACHED from any capturing pipe -----------------
# stdin  <- /dev/null   (no controlling terminal needed)
# stdout -> log file    (regular file: the daemon inheriting this fd cannot hang us)
# stderr -> log file
# A `timeout` backstop guards against any unexpected block.
echo "==> cubrid server $action ${db:-(all)}   [log: $LOG]"

if command -v timeout >/dev/null 2>&1; then
  timeout -k 10 "$TIMEOUT" "$CUBRID_BIN" server "$action" ${db:+"$db"} </dev/null >>"$LOG" 2>&1
  rc=$?
else
  "$CUBRID_BIN" server "$action" ${db:+"$db"} </dev/null >>"$LOG" 2>&1
  rc=$?
fi

# --- report back over the wrapper's own (safely-capturable) stdout -------------
echo "----- cubrid output -------------------------------------------------------"
if [ -s "$LOG" ]; then
  cat "$LOG"
else
  echo "(no output)"
fi
echo "---------------------------------------------------------------------------"

if [ "$rc" -eq 124 ]; then
  echo "RESULT: TIMEOUT after ${TIMEOUT}s (cubrid call was force-killed). exit=$rc"
elif [ "$rc" -eq 0 ]; then
  echo "RESULT: OK (cubrid server $action ${db:-(all)}). exit=$rc"
else
  echo "RESULT: FAILED. exit=$rc"
fi

# Quick state confirmation (status is short-lived and safe to capture directly).
echo "----- cubrid server status ------------------------------------------------"
"$CUBRID_BIN" server status </dev/null 2>&1 || true
echo "---------------------------------------------------------------------------"

exit "$rc"
