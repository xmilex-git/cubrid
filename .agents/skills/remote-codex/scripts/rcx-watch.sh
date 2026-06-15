#!/usr/bin/env bash
# Background watch daemon (runs locally) for codex workers. Handles the mechanical part unattended:
#   - 5h limit exhaustion → WAIT for reset (single account, no rotation), auto-resume when 5h% recovers
#   - weekly limit        → raise ATTENTION (can't rotate; the supervisor decides whether to wait days)
#   - idle twice in a row  → re-issue the /goal mission (codex's loop ended early)
#   - done / error / no_session → log to events.log + raise ATTENTION, stop watching that host
#   - cgroup PID pressure  → steer a process-leak cleanup before the 2048 fork limit locks the box out
# Judgment work (steering, follow-up spec, reporting) is for the supervisor (parent claude).
#
#   rcx-watch.sh [host ...]      (e.g. rcx-watch.sh 32 33 ; no args = every worker with a .state)
#   background:  nohup rcx-watch.sh 33 >/dev/null 2>&1 &
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rcx-env.sh"

RCX_WATCH_INTERVAL="${RCX_WATCH_INTERVAL:-120}"      # poll interval (s)
# cgroup pids pressure (container limit 2048). At the limit fork/SSH lock out → steer cleanup before then.
RCX_PID_WARN="${RCX_PID_WARN:-1500}"                 # warning line (>= → leak-check steer)
RCX_PID_CRIT="${RCX_PID_CRIT:-1900}"                 # critical (near 2048) → forced cleanup (interrupt)
RCX_PID_OK="${RCX_PID_OK:-1200}"                     # back below this → reset steer cooldown
RCX_PID_STEER_COOLDOWN="${RCX_PID_STEER_COOLDOWN:-600}"  # min interval between WARN steers (s)

now() { date +%s; }
flag_attention() { : > "$RCX_STATE_DIR/ATTENTION"; rcx_event "$1" "$2"; }
resume_host() {  # resume_host <host>
  if "$DIR/rcx-resume.sh" "$1" >/dev/null 2>&1; then
    rcx_event "$1" "✅ resumed"; return 0
  fi
  flag_attention "$1" "RESUME_FAILED: rcx-resume.sh failed — check tmux attach"; return 1
}

# PID-pressure cleanup steer body ($1=current, $2=max). The remote codex reads and runs it.
rcx_pid_cleanup_msg() {
  printf '%s' "[SYSTEM ALERT — automated supervisor message] This container's cgroup PID usage is $1/$2, approaching the limit (2048). At 2048, fork is blocked and the build/SSH/this session die and are hard to recover. Pause the current work briefly and clean up process leaks first. (1) Inspect: ps -eLf | wc -l (total threads), ps -u cubrid -o pid,ppid,nlwp,stat,etime,args --sort=-nlwp | head -40, zombie count ps -el | awk '\$2==\"Z\"' | wc -l. (2) Clean up: finished build leftovers (ninja/cc1plus/cc1/gcc/ld/as), unused CUBRID (cub_server/cub_master/cub_broker/cub_cas/csql), finished CTP/test processes — prefer a proper shutdown like cubrid service stop / cubrid broker stop / cubrid server stop <db> first, otherwise kill only orphan PIDs. But do NOT kill the running build.sh or this codex process itself. (3) Prevent recurrence: run new builds/tests with lower parallelism (fewer concurrent jobs), and always stop any server you start after use. After cleanup, record the ps -eLf | wc -l result and what you did in one line in ~/rcx/PROGRESS.md and continue. If the limit doesn't clear, log it to ~/rcx/BLOCKED.md."
}

hosts=("$@")
if [ "${#hosts[@]}" -eq 0 ]; then
  for f in "$RCX_STATE_DIR"/*.state; do [ -f "$f" ] || continue; hosts+=("$(basename "$f" .state)"); done
fi
[ "${#hosts[@]}" -eq 0 ] && { echo "no hosts to watch (no .state). rcx-dispatch first."; exit 1; }

echo "👁  rcx-watch start: ${hosts[*]} (interval=${RCX_WATCH_INTERVAL}s)  pid=$$"
echo "$$" > "$RCX_STATE_DIR/watch.pid"
rcx_event "watch" "start hosts=${hosts[*]} pid=$$"

active=("${hosts[@]}")
while [ "${#active[@]}" -gt 0 ]; do
  next=()
  for h in "${active[@]}"; do
    P="$("$DIR/rcx-poll.sh" "$h" 2>/dev/null)"
    STATE="$(printf '%s\n' "$P" | sed -n 's/^STATE=//p')"
    BLOCKED="$(printf '%s\n' "$P" | sed -n 's/^BLOCKED=//p')"
    RESET="$(printf '%s\n' "$P" | sed -n 's/^RESET=//p')"
    PIDS="$(printf '%s\n' "$P" | sed -n 's/^PIDS=//p')"
    PIDMAX="$(printf '%s\n' "$P" | sed -n 's/^PIDMAX=//p')"
    FIVEH="$(printf '%s\n' "$P" | sed -n 's/^FIVEH=//p')"
    WEEKLY="$(printf '%s\n' "$P" | sed -n 's/^WEEKLY=//p')"
    rcx_state_set "$h" STATUS "$STATE"
    wr="$(rcx_state_get "$h" WAITING_RESET)"; [ -z "$wr" ] && wr=0

    # ── Recovery: we were waiting on a 5h reset and the limit is no longer hit → resume. ──
    if [ "$wr" = "1" ] && [ "$STATE" != "limit_5h" ] && [ "$STATE" != "no_session" ]; then
      rcx_event "$h" "5h limit reset (5h=${FIVEH:-?}%) → resuming"
      resume_host "$h"
      rcx_state_set "$h" WAITING_RESET 0
      next+=("$h"); continue
    fi

    case "$STATE" in
      no_session)
        flag_attention "$h" "no_session: session is gone — stop watching"
        ;;  # excluded from active (not added to next)

      done)
        rcx_state_set "$h" STATUS done
        flag_attention "$h" "DONE: ~/rcx/DONE created — task complete. Needs review/archive"
        ;;  # stop watching (keep the session alive)

      limit_weekly)
        rcx_event "$h" "weekly limit hit (weekly=${WEEKLY:-?}%, ${RESET:-reset?})"
        flag_attention "$h" "WEEKLY_LIMIT: codex weekly limit exhausted — single account, no rotation. Decide whether to wait for reset (days) or stop."
        ;;  # stop watching

      limit_5h)
        if [ "$wr" != "1" ]; then
          rcx_state_set "$h" WAITING_RESET 1
          rcx_event "$h" "5h exhausted (5h=${FIVEH:-?}%, ${RESET:-reset?}) — single account: waiting for reset, will auto-resume"
        fi
        next+=("$h") ;;

      error)
        flag_attention "$h" "ERROR: error in pane. Check with tmux attach"
        next+=("$h") ;;

      idle)
        # /goal owns progress, so persistent idle = the goal ended early → re-issue it.
        ic="$(rcx_state_get "$h" IDLE_COUNT)"; [ -z "$ic" ] && ic=0; ic=$((ic+1))
        rcx_state_set "$h" IDLE_COUNT "$ic"
        if [ "$ic" -eq 2 ]; then
          rcx_event "$h" "persistent idle → re-issue /goal (restart the goal)"
          "$DIR/rcx-steer.sh" "$h" "$(rcx_mission resume)" >/dev/null 2>&1
        elif [ "$ic" -ge 4 ]; then
          flag_attention "$h" "STUCK: no progress even after re-issue — needs steering"
        fi
        next+=("$h") ;;

      working|*)
        rcx_state_set "$h" IDLE_COUNT 0
        next+=("$h") ;;
    esac

    # ── cgroup PID pressure: leak-cleanup steer before the 2048 limit (>=WARN). CRIT → interrupt. ──
    if [ "$STATE" != "no_session" ] && [ "$STATE" != "done" ] && printf '%s' "$PIDS" | grep -qE '^[0-9]+$'; then
      rcx_state_set "$h" PIDS "$PIDS"
      lps="$(rcx_state_get "$h" LAST_PID_STEER_EPOCH)"; [ -z "$lps" ] && lps=0
      if [ "$PIDS" -lt "$RCX_PID_OK" ]; then
        [ "$lps" -ne 0 ] && rcx_state_set "$h" LAST_PID_STEER_EPOCH 0   # back to normal → respond to next spike immediately
      elif [ "$PIDS" -ge "$RCX_PID_CRIT" ]; then
        if [ $(( $(now) - lps )) -ge 180 ]; then
          rcx_event "$h" "PID_CRIT: cgroup pids $PIDS/${PIDMAX:-2048} — near 2048, forced cleanup steer (interrupt)"
          flag_attention "$h" "PID_CRIT: cgroup pids $PIDS/${PIDMAX:-2048} near limit — clean up processes now (lockout risk)"
          "$DIR/rcx-steer.sh" "$h" "$(rcx_pid_cleanup_msg "$PIDS" "${PIDMAX:-2048}")" --interrupt >/dev/null 2>&1
          rcx_state_set "$h" LAST_PID_STEER_EPOCH "$(now)"
        fi
      elif [ "$PIDS" -ge "$RCX_PID_WARN" ]; then
        if [ $(( $(now) - lps )) -ge "$RCX_PID_STEER_COOLDOWN" ]; then
          rcx_event "$h" "PID_HIGH: cgroup pids $PIDS/${PIDMAX:-2048} (>=$RCX_PID_WARN) — leak-check steer"
          flag_attention "$h" "PID_HIGH: cgroup pids $PIDS/${PIDMAX:-2048} over warning line — check for process leaks"
          "$DIR/rcx-steer.sh" "$h" "$(rcx_pid_cleanup_msg "$PIDS" "${PIDMAX:-2048}")" >/dev/null 2>&1
          rcx_state_set "$h" LAST_PID_STEER_EPOCH "$(now)"
        fi
      fi
    fi

    [ "$BLOCKED" = "yes" ] && rcx_event "$h" "BLOCKED.md written (continuing with other items)"
  done

  # On bash 3.2 + set -u, expanding an empty array "${next[@]}" errors → guard by size
  if [ "${#next[@]}" -eq 0 ]; then break; fi
  active=("${next[@]}")
  sleep "$RCX_WATCH_INTERVAL"
done

rcx_event "watch" "stop (no active hosts)"
rm -f "$RCX_STATE_DIR/watch.pid"
echo "👁  rcx-watch stopped"
