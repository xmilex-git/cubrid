#!/usr/bin/env bash
# Dispatch a new task to one host (codex).
#   rcx-dispatch.sh <host> <local-spec-file> [--force]
# e.g. rcx-dispatch.sh 33 ~/.claude/scratch/remote-codex/specs/foo.md
#
# Single ChatGPT account → no account argument (unlike remote-claude's rc-dispatch).
# One task per host at a time. If a session is already alive, refuse without --force.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rcx-env.sh"

host="$(rcx_host "${1:-}")"; spec="${2:-}"; force="${3:-}"

[ -z "$host" ] || [ -z "$spec" ] && { echo "usage: rcx-dispatch.sh <30|32|33> <spec-file> [--force]"; exit 2; }
[ -f "$spec" ] || { echo "❌ spec file not found: $spec"; exit 2; }

# Preflight: worker readiness (codex installed + logged in + tmux + repo).
ready="$(rcx_ssh "$host" "command -v codex >/dev/null 2>&1 && command -v tmux >/dev/null 2>&1 && [ -e ~/$RCX_REPO_REL/.git ] && codex login status 2>&1 | grep -qi 'logged in' && echo READY")"  # -e: worktree .git is a file, not a dir; 'Logged in...' prints to stderr → merge with 2>&1
if [ "$ready" != "READY" ]; then
  echo "❌ $(rcx_host "$host") not ready — need codex (logged in) + tmux + ~/$RCX_REPO_REL. Check: ssh $(rcx_host "$host") 'codex login status'"; exit 7
fi

if rcx_has_session "$host"; then
  if [ "$force" = "--force" ]; then
    echo "⚠️  killing existing session and re-dispatching ($host)"
    ssh "${RCX_SSH_OPTS[@]}" "$(rcx_host "$host")" "tmux kill-session -t $RCX_SESSION 2>/dev/null" >/dev/null 2>&1
    sleep 1
  else
    echo "❌ $host already has a '$RCX_SESSION' session. Use --force to overwrite the running task."; exit 3
  fi
fi

# Prepare control dir + place SPEC + reset PROGRESS/BLOCKED/sentinels
rcx_ssh "$host" "mkdir -p ~/$RCX_CTRL" >/dev/null
rcx_put "$host" "$spec" "~/$RCX_CTRL/SPEC.md" || { echo "❌ SPEC transfer failed"; exit 4; }
rcx_ssh "$host" "rm -f ~/$RCX_CTRL/BLOCKED.md ~/$RCX_CTRL/DONE ~/$RCX_CTRL/PAUSED; : > ~/$RCX_CTRL/PROGRESS.md" >/dev/null
echo "✅ SPEC placed: $host:~/$RCX_CTRL/SPEC.md"

# Create tmux session (large enough — keeps TUI/capture stable)
ssh "${RCX_SSH_OPTS[@]}" "$(rcx_host "$host")" \
  "tmux new-session -d -s $RCX_SESSION -x $RCX_PANE_X -y $RCX_PANE_Y" >/dev/null 2>&1 || { echo "❌ tmux create failed"; exit 5; }
sleep 1

# cd to repo → start codex (all via paste for safe input). No account step (single login).
rcx_paste_text "$host" "cd ~/$RCX_REPO_REL"; sleep 0.4; rcx_enter "$host"; sleep 1
rcx_paste_text "$host" "$RCX_LAUNCH";        sleep 0.4; rcx_enter "$host"
echo "⏳ waiting for codex to start/be ready..."
rcx_wait_ready "$host"; rdy=$?
case "$rdy" in
  2) echo "❌ blocked on a login/auth gate — codex not logged in. Inspect: ssh $(rcx_host "$host") tmux attach -t $RCX_SESSION"; exit 6 ;;
  1) echo "⚠️  TUI-ready timeout — kicking off anyway (may be a slow boot)" ;;
esac

# Kickoff: set the goal (SPEC.md is the task body; this /goal line is the autonomous-run protocol)
KICK="$(rcx_mission start)"
rcx_paste_text "$host" "$KICK"; sleep 1; rcx_enter "$host"

# Record local state
short="$(rcx_short "$host")"
task_title="$(grep -m1 -E '^#' "$spec" | sed -E 's/^#+ *//')"
{
  echo "HOST=$(rcx_host "$host")"
  echo "STARTED=$(date '+%Y-%m-%d %H:%M:%S')"
  echo "TASK=$task_title"
} > "$RCX_STATE_DIR/$short.state"
rcx_log "$host" "dispatched (codex /goal) task=$task_title"

echo "🚀 $host dispatched (codex)"
echo "   task: $task_title"
echo "   monitor: rcx-poll.sh $short"
