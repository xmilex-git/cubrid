#!/usr/bin/env bash
# Dispatch a new task to one host.
#   rc-dispatch.sh <host> <account> <local-spec-file> [--force]
# e.g. rc-dispatch.sh 33 work ~/.claude/scratch/remote-claude/specs/foo.md
#
# One task per host at a time. If a session is already alive, refuse without --force.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rc-env.sh"

host="$(rc_host "${1:-}")"; acct="${2:-personal}"; spec="${3:-}"; force="${4:-}"

[ -z "$host" ] || [ -z "$spec" ] && { echo "usage: rc-dispatch.sh <30|32|33> <work|personal> <spec-file> [--force]"; exit 2; }
[ -f "$spec" ] || { echo "❌ spec file not found: $spec"; exit 2; }
case "$acct" in work|personal) ;; *) echo "❌ account must be work|personal"; exit 2;; esac

# Preflight: worker readiness (claude+tmux+repo). Clear error for un-set-up hosts (e.g. new 30).
ready="$(rc_ssh "$host" "command -v claude >/dev/null 2>&1 && command -v tmux >/dev/null 2>&1 && [ -e ~/$RC_REPO_REL/.git ] && echo READY")"  # -e: worktree .git is a file, not a dir
if [ "$ready" != "READY" ]; then
  echo "❌ $(rc_host "$host") not ready — missing claude/tmux/~/$RC_REPO_REL. Retry after setup."; exit 7
fi

if rc_has_session "$host"; then
  if [ "$force" = "--force" ]; then
    echo "⚠️  killing existing session and re-dispatching ($host)"
    ssh "${RC_SSH_OPTS[@]}" "$(rc_host "$host")" "tmux kill-session -t $RC_SESSION 2>/dev/null" >/dev/null 2>&1
    sleep 1
  else
    echo "❌ $host already has a '$RC_SESSION' session. Use --force to overwrite the running task."; exit 3
  fi
fi

# Prepare control dir + place SPEC + reset PROGRESS/BLOCKED
rc_ssh "$host" "mkdir -p ~/$RC_CTRL" >/dev/null
rc_put "$host" "$spec" "~/$RC_CTRL/SPEC.md" || { echo "❌ SPEC transfer failed"; exit 4; }
rc_ssh "$host" "rm -f ~/$RC_CTRL/BLOCKED.md ~/$RC_CTRL/DONE ~/$RC_CTRL/PAUSED; : > ~/$RC_CTRL/PROGRESS.md" >/dev/null
echo "✅ SPEC placed: $host:~/$RC_CTRL/SPEC.md"

# Pre-clear claude start gates (onboarding/folder-trust/bypass-warning) — required for unattended startup
rc_put "$host" "$DIR/preseed.js" "~/$RC_CTRL/preseed.js"
rc_ssh "$host" "node ~/$RC_CTRL/preseed.js ~/$RC_REPO_REL" >/dev/null && echo "✅ start-gate preseed done"

# Create tmux session (large enough — keeps TUI/capture stable)
ssh "${RC_SSH_OPTS[@]}" "$(rc_host "$host")" \
  "tmux new-session -d -s $RC_SESSION -x $RC_PANE_X -y $RC_PANE_Y" >/dev/null 2>&1 || { echo "❌ tmux create failed"; exit 5; }
sleep 1

# Set account → cd to repo → start claude (all via paste for safe input)
rc_paste_text "$host" "claude-acct use $acct"; sleep 0.4; rc_enter "$host"; sleep 1
rc_paste_text "$host" "cd ~/$RC_REPO_REL";     sleep 0.4; rc_enter "$host"; sleep 1
rc_paste_text "$host" "claude $RC_CLAUDE_FLAGS"; sleep 0.4; rc_enter "$host"
echo "⏳ waiting for claude to start/be ready..."
rc_wait_ready "$host"; rdy=$?
case "$rdy" in
  2) echo "❌ blocked on a start gate (preseed likely failed). Inspect with 'ssh $host tmux attach -t $RC_SESSION'"; exit 6 ;;
  1) echo "⚠️  chat-ready timeout — kicking off anyway (may be a slow boot)" ;;
esac

# Kickoff prompt (SPEC.md is the task body; this prompt is the autonomous-run protocol)
KICK="$(rc_mission start)"
rc_paste_text "$host" "$KICK"; sleep 1; rc_enter "$host"

# Record local state
short="$(rc_short "$host")"
task_title="$(grep -m1 -E '^#' "$spec" | sed -E 's/^#+ *//')"
{
  echo "HOST=$(rc_host "$host")"
  echo "ACCOUNT=$acct"
  echo "STARTED=$(date '+%Y-%m-%d %H:%M:%S')"
  echo "KICKOFF=$RC_KICKOFF_MODE"
  echo "TASK=$task_title"
} > "$RC_STATE_DIR/$short.state"
rc_log "$host" "dispatched acct=$acct mode=$RC_KICKOFF_MODE task=$task_title"

echo "🚀 $host dispatched — account=$acct, mode=$RC_KICKOFF_MODE"
echo "   task: $task_title"
echo "   monitor: rc-poll.sh $short"
