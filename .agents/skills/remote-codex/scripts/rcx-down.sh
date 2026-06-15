#!/usr/bin/env bash
# Tear the codex session down. Archive the last pane + PROGRESS locally before closing.
#   rcx-down.sh <host>
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rcx-env.sh"

host="$(rcx_host "${1:-}")"; short="$(rcx_short "$host")"
[ -z "$host" ] && { echo "usage: rcx-down.sh <30|32|33>"; exit 2; }

if rcx_has_session "$host"; then
  ssh "${RCX_SSH_OPTS[@]}" "$host" "tmux capture-pane -p -J -t $RCX_SESSION -S -400" 2>/dev/null \
    | rcx_clean > "$RCX_STATE_DIR/$short.final.log"
  rcx_ssh "$host" "cat ~/$RCX_CTRL/PROGRESS.md 2>/dev/null" > "$RCX_STATE_DIR/$short.progress.md" 2>/dev/null || true
  ssh "${RCX_SSH_OPTS[@]}" "$host" "tmux kill-session -t $RCX_SESSION 2>/dev/null" >/dev/null 2>&1
  echo "🛑 $host session closed. final screen: $RCX_STATE_DIR/$short.final.log"
  rcx_log "$host" "session down (archived)"
else
  echo "ℹ️  no session on $host"
fi
[ -f "$RCX_STATE_DIR/$short.state" ] && echo "STOPPED=$(date '+%Y-%m-%d %H:%M:%S')" >> "$RCX_STATE_DIR/$short.state"
rm -f "$RCX_STATE_DIR/$short.lasthash"
