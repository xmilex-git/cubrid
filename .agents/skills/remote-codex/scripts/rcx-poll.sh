#!/usr/bin/env bash
# Poll one host's codex status once (single ssh). The supervisor calls this periodically.
#   rcx-poll.sh <host>
# Output (easy-to-parse KV):
#   HOST= SESSION= STATE= CHANGED= BLOCKED= RESET= TAIL= PIDS= PIDMAX= FIVEH= WEEKLY=
# STATE ∈ no_session | working | idle | limit_5h | limit_weekly | done | error
#
# codex's bottom status line is the authoritative signal, e.g.:
#   gpt-5.5 high · ~/dev/cubrid · cubrid · branch · Ready · Full Access · Context 97% left · 5h 90% left · weekly 98% left
#   - run-state: `Ready` (idle) | `Working` (busy)   - limits: `5h N% left`, `weekly N% left`
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rcx-env.sh"

host="$(rcx_host "${1:-}")"; short="$(rcx_short "$host")"
[ -z "$host" ] && { echo "usage: rcx-poll.sh <30|32|33>"; exit 2; }

# In one ssh: session check + pane capture + BLOCKED last line + DONE flag + cgroup pids.
raw="$(ssh "${RCX_SSH_OPTS[@]}" "$host" "
  if ! tmux has-session -t $RCX_SESSION 2>/dev/null; then echo __RCX_NOSESSION__; exit 0; fi
  echo __RCX_CAP_START__
  tmux capture-pane -p -J -t $RCX_SESSION -S -220
  echo __RCX_CAP_END__
  printf '__RCX_BLOCKED__:'; ( test -s ~/$RCX_CTRL/BLOCKED.md && tail -n1 ~/$RCX_CTRL/BLOCKED.md ) || true; echo
  printf '__RCX_DONE__:'; ( test -f ~/$RCX_CTRL/DONE && printf yes ) || printf no; echo
  printf '__RCX_PIDS__:'; c=\$(cat /sys/fs/cgroup/pids/pids.current 2>/dev/null); m=\$(cat /sys/fs/cgroup/pids/pids.max 2>/dev/null); printf '%s/%s' \"\${c:-}\" \"\${m:-}\"; echo
" 2>/dev/null | rcx_clean)"

emit() { # emit STATE CHANGED BLOCKED RESET TAIL PIDS PIDMAX FIVEH WEEKLY
  printf 'HOST=%s\nSESSION=%s\nSTATE=%s\nCHANGED=%s\nBLOCKED=%s\nRESET=%s\nTAIL=%s\nPIDS=%s\nPIDMAX=%s\nFIVEH=%s\nWEEKLY=%s\n' \
    "$host" "$RCX_SESSION" "$1" "$2" "$3" "$4" "$5" "${6:-}" "${7:-}" "${8:-}" "${9:-}"
}

if printf '%s' "$raw" | grep -q '__RCX_NOSESSION__'; then
  emit no_session no no "" "" "" "" "" ""
  exit 0
fi

# Extract capture body / BLOCKED / DONE / pids
cap="$(printf '%s\n' "$raw" | sed -n '/__RCX_CAP_START__/,/__RCX_CAP_END__/p' | sed '1d;$d')"
blocked_line="$(printf '%s\n' "$raw" | grep -m1 '__RCX_BLOCKED__:' | sed 's/^__RCX_BLOCKED__://')"
blocked="no"; [ -n "${blocked_line// /}" ] && blocked="yes"
done_flag="$(printf '%s\n' "$raw" | grep -m1 '__RCX_DONE__:' | sed 's/.*__RCX_DONE__://' | tr -d ' \r')"
pids_line="$(printf '%s\n' "$raw" | grep -m1 '__RCX_PIDS__:' | sed 's/.*__RCX_PIDS__://' | tr -d ' \r')"
pids_cur="${pids_line%%/*}"; pids_max="${pids_line##*/}"

# Change detection (hash): same as previous capture → CHANGED=no.  While codex works, the
# `Working (Ns)` / `Pursuing goal (Ns)` timer ticks each second → CHANGED stays yes (never false idle).
hashfile="$RCX_STATE_DIR/$short.lasthash"
newhash="$(printf '%s' "$cap" | cksum | awk '{print $1}')"
changed="yes"
[ -f "$hashfile" ] && [ "$(cat "$hashfile" 2>/dev/null)" = "$newhash" ] && changed="no"
printf '%s' "$newhash" > "$hashfile"

# Only judge the recent area (live UI) for state → avoid false positives from SPEC body text.
tail_area="$(printf '%s\n' "$cap" | tail -n 40)"
lastline="$(printf '%s\n' "$cap" | grep -vE '^[[:space:]]*$' | tail -n 1 | cut -c1-160)"

# Pull the limit percentages off the status line ('' if not visible).
# NB: grab the digits right before '%' — a naive "first number" would pick the '5' out of "5h".
fiveh="$(printf '%s' "$tail_area" | grep -ioE '5h[[:space:]]+[0-9]+%' | head -1 | grep -oE '[0-9]+%' | grep -oE '[0-9]+')"
weekly="$(printf '%s' "$tail_area" | grep -ioE 'weekly[[:space:]]+[0-9]+%' | head -1 | grep -oE '[0-9]+%' | grep -oE '[0-9]+')"

reset=""
state="working"
# Limit detection: when the status-line % is visible, trust ONLY the % (0 = exhausted) — this avoids
# false positives from stray "rate limit"/"limit reached" text in codex's own task output. Fall back
# to a best-effort banner regex only when the % isn't captured (e.g. codex replaced it with a modal).
if [ "$done_flag" = "yes" ]; then
  state="done"
elif { [ -n "$weekly" ] && [ "$weekly" = "0" ]; } || \
     { [ -z "$weekly" ] && printf '%s' "$tail_area" | grep -qiE 'week(ly)?[^.]{0,30}limit reached|weekly[^.]{0,30}(usage|rate) limit'; }; then
  state="limit_weekly"
  reset="$(printf '%s' "$tail_area" | grep -ioE 'reset[s]?( by| at| in)?[^|·]{0,40}' | head -1)"
elif { [ -n "$fiveh" ] && [ "$fiveh" = "0" ]; } || \
     { [ -z "$fiveh" ] && printf '%s' "$tail_area" | grep -qiE 'usage limit reached|hit your [a-z ]*limit|out of (usage|credits)|rate limit reached|try again (later|in)'; }; then
  state="limit_5h"
  reset="$(printf '%s' "$tail_area" | grep -ioE 'reset[s]?( by| at| in)?[^|·]{0,40}' | head -1)"
elif printf '%s' "$tail_area" | grep -qiE '· Working ·|esc to interrupt|Pursuing goal'; then
  state="working"   # active-working takes precedence over a stray 'Error:'/'fatal:' string in the pane
                    # (test-campaign panes legitimately show errors/*.err as normal data — don't drop a live worker)
elif printf '%s' "$tail_area" | grep -qiE '· Ready ·'; then
  state="idle"      # codex finished its turn and is waiting. The watch's idle-twice gate avoids premature nudges.
elif printf '%s' "$tail_area" | grep -qiE 'command not found|Error:|fatal:|panic:|Cannot find|ENOENT|EACCES'; then
  state="error"
elif [ "$changed" = "no" ]; then
  state="idle"
else
  state="working"
fi

emit "$state" "$changed" "$blocked" "$reset" "$lastline" "$pids_cur" "$pids_max" "${fiveh:-}" "${weekly:-}"
rcx_log "$host" "poll state=$state changed=$changed blocked=$blocked 5h=${fiveh:-?}% weekly=${weekly:-?}% ${reset:+reset=$reset}"
