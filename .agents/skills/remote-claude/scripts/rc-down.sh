#!/usr/bin/env bash
# Tear the session down. Archive the last pane locally before closing.
#   rc-down.sh <host>
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rc-env.sh"

host="$(rc_host "${1:-}")"; short="$(rc_short "$host")"
[ -z "$host" ] && { echo "usage: rc-down.sh <30|32|33>"; exit 2; }

if rc_has_session "$host"; then
  ssh "${RC_SSH_OPTS[@]}" "$host" "tmux capture-pane -p -J -t $RC_SESSION -S -400" 2>/dev/null \
    | rc_clean > "$RC_STATE_DIR/$short.final.log"
  # Also archive the remote PROGRESS
  rc_ssh "$host" "cat ~/$RC_CTRL/PROGRESS.md 2>/dev/null" > "$RC_STATE_DIR/$short.progress.md" 2>/dev/null || true
  ssh "${RC_SSH_OPTS[@]}" "$host" "tmux kill-session -t $RC_SESSION 2>/dev/null" >/dev/null 2>&1
  echo "🛑 $host session closed. final screen: $RC_STATE_DIR/$short.final.log"
  rc_log "$host" "session down (archived)"
else
  echo "ℹ️  no session on $host"
fi
[ -f "$RC_STATE_DIR/$short.state" ] && echo "STOPPED=$(date '+%Y-%m-%d %H:%M:%S')" >> "$RC_STATE_DIR/$short.state"
rm -f "$RC_STATE_DIR/$short.lasthash"
