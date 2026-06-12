#!/usr/bin/env bash
# Switch account and resume the same conversation (on token exhaustion).
#   rc-rotate.sh <host> <new-account>
# Steps: quit claude cleanly → claude-acct use <new> → claude --continue → resume nudge
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rc-env.sh"

host="$(rc_host "${1:-}")"; new="${2:-}"
[ -z "$host" ] || [ -z "$new" ] && { echo "usage: rc-rotate.sh <30|32|33> <work|personal>"; exit 2; }
case "$new" in work|personal) ;; *) echo "❌ account must be work|personal"; exit 2;; esac
rc_has_session "$host" || { echo "❌ no session on $host — needs rc-dispatch, not rotate"; exit 3; }

# Decide via capture whether claude is up or we dropped to the shell
in_claude() {
  ssh "${RC_SSH_OPTS[@]}" "$host" "tmux capture-pane -p -J -t $RC_SESSION -S -8" 2>/dev/null \
    | rc_clean | grep -qE 'esc to interrupt|to interrupt\)|for shortcuts|bypass permissions on|shift\+tab to cycle|for agents|╭|╰|│ >'
}

echo "⏹  trying to quit claude ($host)"
for i in 1 2 3; do
  in_claude || break
  rc_sk "$host" C-c; sleep 1
  rc_sk "$host" C-c; sleep 1
  rc_sk "$host" C-d; sleep 2
done
if in_claude; then
  echo "❌ claude won't quit — needs manual check (tmux attach)"; rc_log "$host" "rotate FAILED: claude won't quit"; exit 4
fi

echo "🔁 switching account → $new, resuming the conversation"
rc_paste_text "$host" "claude-acct use $new";            sleep 0.5; rc_enter "$host"; sleep 1
rc_paste_text "$host" "claude --continue $RC_CLAUDE_FLAGS"; sleep 0.5; rc_enter "$host"
rc_wait_ready "$host" || echo "⚠️  could not confirm chat-ready after resume — nudging anyway"

RESUME="$(rc_mission resume)"   # with RC_KICKOFF_MODE=loop, re-issues /loop → restarts the loop on the new account
rc_paste_text "$host" "$RESUME"; sleep 1; rc_enter "$host"

# Update state
short="$(rc_short "$host")"
if [ -f "$RC_STATE_DIR/$short.state" ]; then
  grep -v '^ACCOUNT=' "$RC_STATE_DIR/$short.state" > "$RC_STATE_DIR/$short.state.tmp" 2>/dev/null
  echo "ACCOUNT=$new" >> "$RC_STATE_DIR/$short.state.tmp"
  mv "$RC_STATE_DIR/$short.state.tmp" "$RC_STATE_DIR/$short.state"
fi
rm -f "$RC_STATE_DIR/$short.lasthash"   # reset hash (screen changed on resume)
rc_log "$host" "rotated to $new and resumed"
echo "✅ $host resumed on account $new"
