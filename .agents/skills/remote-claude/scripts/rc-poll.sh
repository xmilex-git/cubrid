#!/usr/bin/env bash
# Poll one host's status once (single ssh). The supervisor calls this periodically.
#   rc-poll.sh <host>
# Output (easy-to-parse KV):
#   HOST= SESSION= STATE= CHANGED= BLOCKED= RESET= TAIL=
# STATE ∈ no_session | working | idle | limit_5h | limit_weekly | done | error
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rc-env.sh"

host="$(rc_host "${1:-}")"; short="$(rc_short "$host")"
[ -z "$host" ] && { echo "usage: rc-poll.sh <30|32|33>"; exit 2; }

# In one ssh: session check + pane capture + BLOCKED last line, all with markers.
raw="$(ssh "${RC_SSH_OPTS[@]}" "$host" "
  if ! tmux has-session -t $RC_SESSION 2>/dev/null; then echo __RC_NOSESSION__; exit 0; fi
  echo __RC_CAP_START__
  tmux capture-pane -p -J -t $RC_SESSION -S -220
  echo __RC_CAP_END__
  printf '__RC_BLOCKED__:'; ( test -s ~/$RC_CTRL/BLOCKED.md && tail -n1 ~/$RC_CTRL/BLOCKED.md ) || true; echo
  printf '__RC_DONE__:'; ( test -f ~/$RC_CTRL/DONE && printf yes ) || printf no; echo
  printf '__RC_PIDS__:'; c=\$(cat /sys/fs/cgroup/pids/pids.current 2>/dev/null); m=\$(cat /sys/fs/cgroup/pids/pids.max 2>/dev/null); printf '%s/%s' \"\${c:-}\" \"\${m:-}\"; echo
" 2>/dev/null | rc_clean)"

emit() { # emit STATE CHANGED BLOCKED RESET TAIL [PIDS] [PIDMAX]
  printf 'HOST=%s\nSESSION=%s\nSTATE=%s\nCHANGED=%s\nBLOCKED=%s\nRESET=%s\nTAIL=%s\nPIDS=%s\nPIDMAX=%s\n' \
    "$host" "$RC_SESSION" "$1" "$2" "$3" "$4" "$5" "${6:-}" "${7:-}"
}

if printf '%s' "$raw" | grep -q '__RC_NOSESSION__'; then
  emit no_session no no "" ""
  exit 0
fi

# Extract capture body / BLOCKED
cap="$(printf '%s\n' "$raw" | sed -n '/__RC_CAP_START__/,/__RC_CAP_END__/p' | sed '1d;$d')"
blocked_line="$(printf '%s\n' "$raw" | grep -m1 '__RC_BLOCKED__:' | sed 's/^__RC_BLOCKED__://')"
blocked="no"; [ -n "${blocked_line// /}" ] && blocked="yes"
done_flag="$(printf '%s\n' "$raw" | grep -m1 '__RC_DONE__:' | sed 's/.*__RC_DONE__://' | tr -d ' \r')"

# cgroup pids: "current/max" (if both empty → empty string → watch skips)
pids_line="$(printf '%s\n' "$raw" | grep -m1 '__RC_PIDS__:' | sed 's/.*__RC_PIDS__://' | tr -d ' \r')"
pids_cur="${pids_line%%/*}"; pids_max="${pids_line##*/}"

# Change detection (hash): same as previous capture → CHANGED=no → idle/stuck candidate
hashfile="$RC_STATE_DIR/$short.lasthash"
newhash="$(printf '%s' "$cap" | cksum | awk '{print $1}')"
changed="yes"
[ -f "$hashfile" ] && [ "$(cat "$hashfile" 2>/dev/null)" = "$newhash" ] && changed="no"
printf '%s' "$newhash" > "$hashfile"

# Only judge the recent area (live UI) for banners → avoid false positives from SPEC body text
tail_area="$(printf '%s\n' "$cap" | tail -n 40)"
lastline="$(printf '%s\n' "$cap" | grep -vE '^[[:space:]]*$' | tail -n 1 | cut -c1-160)"

reset=""
state="working"
if [ "$done_flag" = "yes" ]; then
  state="done"
elif printf '%s' "$tail_area" | grep -qiE 'week(ly)?[^.]*limit|limit[^.]*week|weekly (usage|rate)'; then
  state="limit_weekly"
  reset="$(printf '%s' "$tail_area" | grep -ioE 'reset[s]?( by| at)?[^|]{0,40}' | head -1)"
elif printf '%s' "$tail_area" | grep -qiE 'usage limit|limit reached|rate limit|reached your (usage|limit)|out of usage|session limit|hit your [a-z ]*limit|resets? (at|in|[0-9])|upgrade to (a )?(higher|paid)'; then
  state="limit_5h"
  reset="$(printf '%s' "$tail_area" | grep -ioE 'reset[s]?( by| at| in)?[^|]{0,40}' | head -1)"
elif [ "$changed" = "yes" ] && printf '%s' "$tail_area" | grep -qiE 'esc to interrupt|to interrupt\)|Thinking|Forging|Compacting|Running|tokens|Esc to'; then
  state="working"   # active-working takes precedence over a stray 'Error:'/'fatal:' string in the pane
                    # (test-campaign panes legitimately show errors/*.err as normal data — don't drop a live worker)
                    # changed=yes guard: a dead pane with stale spinner text must not pin state=working forever
elif printf '%s' "$tail_area" | grep -qiE 'command not found|Error:|fatal:|panic:|Cannot find|ENOENT|EACCES'; then
  state="error"
elif [ "$changed" = "no" ]; then
  state="idle"
else
  state="working"
fi

emit "$state" "$changed" "$blocked" "$reset" "$lastline" "$pids_cur" "$pids_max"
rc_log "$host" "poll state=$state changed=$changed blocked=$blocked ${reset:+reset=$reset}"
