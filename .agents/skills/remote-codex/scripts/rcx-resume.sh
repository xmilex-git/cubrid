#!/usr/bin/env bash
# Resume / re-drive the codex session (after a 5h reset, a crash, or persistent idle).
#   rcx-resume.sh <host>
# Single account → no rotation. Two cases:
#   - codex still in the TUI  → just re-issue the /goal mission (nudge it to keep going).
#   - codex dropped to shell  → `codex resume --last` to restore the session, then re-issue /goal.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rcx-env.sh"

host="$(rcx_host "${1:-}")"
[ -z "$host" ] && { echo "usage: rcx-resume.sh <30|32|33>"; exit 2; }
rcx_has_session "$host" || { echo "❌ no session on $host — needs rcx-dispatch, not resume"; exit 3; }

# Is codex's TUI up, or did we drop to a shell?
in_codex() {
  ssh "${RCX_SSH_OPTS[@]}" "$host" "tmux capture-pane -p -J -t $RCX_SESSION -S -12" 2>/dev/null \
    | rcx_clean | grep -qiE 'OpenAI Codex|Full Access|% left|esc to interrupt|· Ready ·|· Working ·'
}

if in_codex "$host"; then
  echo "▶️  codex TUI is up — re-issuing the goal ($host)"
else
  echo "↻  codex dropped to shell — running 'codex resume --last' ($host)"
  rcx_paste_text "$host" "$RCX_LAUNCH $RCX_RESUME_ARGS"; sleep 0.5; rcx_enter "$host"
  rcx_wait_ready "$host" || echo "⚠️  could not confirm TUI-ready after resume — nudging anyway"
fi

RESUME="$(rcx_mission resume)"   # /goal <resume body> — re-sets the long-running goal on the restored session
rcx_paste_text "$host" "$RESUME"; sleep 1; rcx_enter "$host"

short="$(rcx_short "$host")"
rm -f "$RCX_STATE_DIR/$short.lasthash"   # reset hash (screen changed on resume)
rcx_log "$host" "resumed (codex --last + /goal)"
echo "✅ $host resumed"
