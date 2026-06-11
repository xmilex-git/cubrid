#!/usr/bin/env bash
# Background watch daemon (runs locally). Handles the mechanical part unattended:
#   - 5h token exhaustion → auto-rotate to the other account on the same host + resume
#   - weekly limit → mark that account weekly-dead, switch to the other
#   - both accounts exhausted at once → defer to WAIT_UNTIL without blocking, then retry
#   - idle twice in a row → "continue" nudge
#   - done / error / both-weekly-dead → log to events.log + raise ATTENTION (call the supervisor)
# Judgment work (steering, follow-up spec, reporting) is for the supervisor (parent claude), which
# reads events.log and acts.
#
#   rc-watch.sh [host ...]      (e.g. rc-watch.sh 32 33 ; no args = every worker with a .state)
#   background:  nohup rc-watch.sh 32 33 >/dev/null 2>&1 &
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rc-env.sh"

RC_WATCH_INTERVAL="${RC_WATCH_INTERVAL:-120}"      # poll interval (s)
RC_BOTH_WAIT="${RC_BOTH_WAIT:-1800}"               # wait before resume when both exhausted at once (s)
RC_WEEKLY_TTL="${RC_WEEKLY_TTL:-604800}"           # how long weekly-dead is kept (s, 7 days)
# cgroup pids pressure (container limit 2048). At the limit fork/SSH lock out → steer cleanup before then.
RC_PID_WARN="${RC_PID_WARN:-1500}"                 # warning line (>= → leak-check steer)
RC_PID_CRIT="${RC_PID_CRIT:-1900}"                 # critical (near 2048) → forced cleanup (interrupt)
RC_PID_OK="${RC_PID_OK:-1200}"                     # back below this → reset steer cooldown
RC_PID_STEER_COOLDOWN="${RC_PID_STEER_COOLDOWN:-600}"  # min interval between WARN steers (s)

now() { date +%s; }
flag_attention() { : > "$RC_STATE_DIR/ATTENTION"; rc_event "$1" "$2"; }
weekly_dead() {  # 0 if <acct> on <host> is weekly-dead within TTL
  local ts; ts="$(rc_state_get "$1" "WEEKLY_DEAD_$2")"
  [ -z "$ts" ] && return 1
  [ $(( $(now) - ts )) -lt "$RC_WEEKLY_TTL" ]
}
rotate_to() {  # rotate_to <host> <acct> ; on success record LAST_ROTATE
  if "$DIR/rc-rotate.sh" "$1" "$2" >/dev/null 2>&1; then
    rc_state_set "$1" ACCOUNT "$2"; rc_state_set "$1" LAST_ROTATE_EPOCH "$(now)"
    rc_event "$1" "✅ resumed on $2"; return 0
  fi
  flag_attention "$1" "ROTATE_FAILED: failed to switch to $2"; return 1
}

# PID-pressure cleanup steer body ($1=current, $2=max). The remote vanilla claude reads and runs it.
rc_pid_cleanup_msg() {
  printf '%s' "[SYSTEM ALERT — automated supervisor message] This container's cgroup PID usage is $1/$2, approaching the limit (2048). At 2048, fork is blocked and the build/SSH/this session die and are hard to recover. Pause the current work briefly and clean up process leaks first. (1) Inspect: ps -eLf | wc -l (total threads), ps -u cubrid -o pid,ppid,nlwp,stat,etime,args --sort=-nlwp | head -40, zombie count ps -el | awk '\$2==\"Z\"' | wc -l. (2) Clean up: finished build leftovers (ninja/cc1plus/cc1/gcc/ld/as), unused CUBRID (cub_server/cub_master/cub_broker/cub_cas/csql), finished CTP/test processes — prefer a proper shutdown like cubrid service stop / cubrid broker stop / cubrid server stop <db> first, otherwise kill only orphan PIDs. But do NOT kill the running build.sh or this claude process itself. (3) Prevent recurrence: run new builds/tests with lower parallelism (fewer concurrent jobs/Tasks), and always stop any server you start after use. After cleanup, record the ps -eLf | wc -l result and what you did in one line in ~/rc/PROGRESS.md and continue. If the limit doesn't clear, log it to ~/rc/BLOCKED.md."
}

hosts=("$@")
if [ "${#hosts[@]}" -eq 0 ]; then
  for f in "$RC_STATE_DIR"/*.state; do [ -f "$f" ] || continue; hosts+=("$(basename "$f" .state)"); done
fi
[ "${#hosts[@]}" -eq 0 ] && { echo "no hosts to watch (no .state). rc-dispatch first."; exit 1; }

echo "👁  rc-watch start: ${hosts[*]} (interval=${RC_WATCH_INTERVAL}s)  pid=$$"
echo "$$" > "$RC_STATE_DIR/watch.pid"
rc_event "watch" "start hosts=${hosts[*]} pid=$$"

active=("${hosts[@]}")
while [ "${#active[@]}" -gt 0 ]; do
  next=()
  for h in "${active[@]}"; do
    cur="$(rc_state_get "$h" ACCOUNT)"; [ -z "$cur" ] && cur="work"

    # Waiting (both exhausted at once)? Resume if it's time, else pass.
    wu="$(rc_state_get "$h" WAIT_UNTIL)"; [ -z "$wu" ] && wu=0
    if [ "$wu" -gt 0 ]; then
      if [ "$(now)" -lt "$wu" ]; then next+=("$h"); continue; fi
      rc_state_set "$h" WAIT_UNTIL 0
      pend="$(rc_state_get "$h" PENDING_ACCT)"; [ -z "$pend" ] && pend="work"
      rc_event "$h" "wait over → trying to resume $pend"
      rotate_to "$h" "$pend"; next+=("$h"); continue
    fi

    P="$("$DIR/rc-poll.sh" "$h" 2>/dev/null)"
    STATE="$(printf '%s\n' "$P" | sed -n 's/^STATE=//p')"
    BLOCKED="$(printf '%s\n' "$P" | sed -n 's/^BLOCKED=//p')"
    RESET="$(printf '%s\n' "$P" | sed -n 's/^RESET=//p')"
    PIDS="$(printf '%s\n' "$P" | sed -n 's/^PIDS=//p')"
    PIDMAX="$(printf '%s\n' "$P" | sed -n 's/^PIDMAX=//p')"
    rc_state_set "$h" STATUS "$STATE"
    other="$(rc_other_acct "$cur")"

    case "$STATE" in
      no_session)
        flag_attention "$h" "no_session: session is gone — stop watching"
        ;;  # excluded from active (not added to next)

      done)
        rc_state_set "$h" STATUS done
        flag_attention "$h" "DONE: ~/rc/DONE created — task complete. Needs review/archive"
        ;;  # stop watching (keep the session alive)

      limit_weekly)
        rc_state_set "$h" "WEEKLY_DEAD_$cur" "$(now)"
        rc_event "$h" "weekly limit hit: $cur (${RESET:-reset?})"
        if weekly_dead "$h" "$other"; then
          flag_attention "$h" "BOTH_WEEKLY_DEAD: both work/personal weekly-exhausted — decide whether to wait for reset"
        else
          rotate_to "$h" "$other"; next+=("$h")
        fi ;;

      limit_5h)
        lr="$(rc_state_get "$h" LAST_ROTATE_EPOCH)"; [ -z "$lr" ] && lr=0
        if [ $(( $(now) - lr )) -lt 240 ] || weekly_dead "$h" "$other"; then
          # Just rotated and hit 5h again (= both 5h at once), or the other account is weekly-dead → wait
          rc_state_set "$h" WAIT_UNTIL "$(( $(now) + RC_BOTH_WAIT ))"
          rc_state_set "$h" PENDING_ACCT "$cur"
          rc_event "$h" "both likely exhausted — wait $((RC_BOTH_WAIT/60))min then resume (${RESET:-reset?})"
        else
          rc_event "$h" "5h exhausted: $cur → $other (${RESET:-reset?})"
          rotate_to "$h" "$other"
        fi
        next+=("$h") ;;

      error)
        flag_attention "$h" "ERROR: error in pane. Check with tmux attach"
        next+=("$h") ;;

      idle)
        # Native /loop owns progress, so persistent idle = the loop ended early.
        ic="$(rc_state_get "$h" IDLE_COUNT)"; [ -z "$ic" ] && ic=0; ic=$((ic+1))
        rc_state_set "$h" IDLE_COUNT "$ic"
        if [ "$ic" -eq 2 ]; then
          rc_event "$h" "persistent idle → re-issue /loop (restart the loop)"
          "$DIR/rc-steer.sh" "$h" "$(rc_mission resume)" >/dev/null 2>&1
        elif [ "$ic" -ge 4 ]; then
          flag_attention "$h" "STUCK: no progress even after re-issue — needs steering"
        fi
        next+=("$h") ;;

      working|*)
        rc_state_set "$h" IDLE_COUNT 0
        next+=("$h") ;;
    esac

    # ── cgroup PID pressure: leak-cleanup steer before the 2048 limit (>=WARN). CRIT → interrupt. ──
    if [ "$STATE" != "no_session" ] && [ "$STATE" != "done" ] && printf '%s' "$PIDS" | grep -qE '^[0-9]+$'; then
      rc_state_set "$h" PIDS "$PIDS"
      lps="$(rc_state_get "$h" LAST_PID_STEER_EPOCH)"; [ -z "$lps" ] && lps=0
      if [ "$PIDS" -lt "$RC_PID_OK" ]; then
        [ "$lps" -ne 0 ] && rc_state_set "$h" LAST_PID_STEER_EPOCH 0   # back to normal → respond to next spike immediately
      elif [ "$PIDS" -ge "$RC_PID_CRIT" ]; then
        if [ $(( $(now) - lps )) -ge 180 ]; then
          rc_event "$h" "PID_CRIT: cgroup pids $PIDS/${PIDMAX:-2048} — near 2048, forced cleanup steer (interrupt)"
          flag_attention "$h" "PID_CRIT: cgroup pids $PIDS/${PIDMAX:-2048} near limit — clean up processes now (lockout risk)"
          "$DIR/rc-steer.sh" "$h" "$(rc_pid_cleanup_msg "$PIDS" "${PIDMAX:-2048}")" --interrupt >/dev/null 2>&1
          rc_state_set "$h" LAST_PID_STEER_EPOCH "$(now)"
        fi
      elif [ "$PIDS" -ge "$RC_PID_WARN" ]; then
        if [ $(( $(now) - lps )) -ge "$RC_PID_STEER_COOLDOWN" ]; then
          rc_event "$h" "PID_HIGH: cgroup pids $PIDS/${PIDMAX:-2048} (>=$RC_PID_WARN) — leak-check steer"
          flag_attention "$h" "PID_HIGH: cgroup pids $PIDS/${PIDMAX:-2048} over warning line — check for process leaks"
          "$DIR/rc-steer.sh" "$h" "$(rc_pid_cleanup_msg "$PIDS" "${PIDMAX:-2048}")" >/dev/null 2>&1
          rc_state_set "$h" LAST_PID_STEER_EPOCH "$(now)"
        fi
      fi
    fi

    [ "$BLOCKED" = "yes" ] && rc_event "$h" "BLOCKED.md written (continuing with other items)"
  done

  # On bash 3.2 + set -u, expanding an empty array "${next[@]}" errors → guard by size
  if [ "${#next[@]}" -eq 0 ]; then break; fi
  active=("${next[@]}")
  sleep "$RC_WATCH_INTERVAL"
done

rc_event "watch" "stop (no active hosts)"
rm -f "$RC_STATE_DIR/watch.pid"
echo "👁  rc-watch stopped"
