#!/usr/bin/env bash
# remote-claude shared config/helpers.
# Other rc-*.sh source this via `source "$(dirname "$0")/rc-env.sh"`.
#
# Environment facts (verified 2026-06-01):
#   - Workers are reached by IP directly: cubrid@192.168.6.30 / .32 / .33 (no ssh config alias)
#   - Short tag = last octet (30/32/33). CLI args and state files use this tag.
#   - 32, 33: Linux el8, bash, claude, tmux, node, repo ~/dev/cubrid ready.
#     30: setup in progress — claude/tmux/repo may be missing (dispatch preflight checks).
#   - ~/.bashrc defines a Linux claude-acct function + auto-runs `claude-acct use work` on login
#   - claude-acct: work (default) / personal accounts registered. `claude-acct use <name>` switches
#     the current shell's token.
#   - The remote has no custom skills/plugins. A SPEC must run on a vanilla claude alone.
#
# Note: token values are never stored in this script/skill. Account switching is delegated entirely
#       to the remote claude-acct (which already holds the tokens).

# ── Config (overridable via env) ────────────────────────────────────────────
RC_SESSION="${RC_SESSION:-rc}"                       # one tmux session name per host
RC_CTRL="${RC_CTRL:-rc}"                              # control dir relative to remote home (~/rc)
RC_REPO_REL="${RC_REPO_REL:-dev/cubrid}"             # work repo relative to remote home (~/dev/cubrid)
RC_STATE_DIR="${RC_STATE_DIR:-$HOME/.claude/scratch/remote-claude}"  # local state/logs
RC_PANE_X="${RC_PANE_X:-220}"
RC_PANE_Y="${RC_PANE_Y:-50}"
RC_CLAUDE_FLAGS="${RC_CLAUDE_FLAGS:---dangerously-skip-permissions}"  # unattended run: bypass permission prompts
RC_KICKOFF_MODE="${RC_KICKOFF_MODE:-loop}"          # loop | goal | prompt  (uses vanilla claude built-in /loop·/goal)
RC_SSH_OPTS=(-o ConnectTimeout=10 -o BatchMode=yes -o LogLevel=ERROR)

# Strip the account banner the .bashrc prints on every ssh + the ssh PQ warning, from stdout/decisions.
# NOTE: the Korean substrings below are LITERAL — they match the remote banner text. Do not translate.
RC_NOISE='활성 계정|claude 에 적용|claude-acct|적용됨|토큰 해제|post-quantum|WARNING: connection|store now|decrypt later|may need to be upgraded|openssh.com/pq'

mkdir -p "$RC_STATE_DIR" 2>/dev/null || true

# ── Helpers ──────────────────────────────────────────────────────────────────
RC_SUBNET="${RC_SUBNET:-192.168.6}"   # worker subnet
RC_USER="${RC_USER:-cubrid}"          # worker account

rc_host() {  # 30/32/33 or IP or user@ip → cubrid@192.168.6.NN
  case "$1" in
    "$RC_USER"@*)        printf '%s' "$1" ;;
    "$RC_SUBNET".*)      printf '%s@%s' "$RC_USER" "$1" ;;
    [0-9]|[0-9][0-9]|[0-9][0-9][0-9]) printf '%s@%s.%s' "$RC_USER" "$RC_SUBNET" "$1" ;;
    *)                   printf '%s' "$1" ;;   # otherwise pass through (user may give a full target)
  esac
}

rc_short() { printf '%s' "${1##*.}"; }   # anything → last octet → 30/32/33

rc_clean() { grep -avE "$RC_NOISE" 2>/dev/null || true; }  # strip noise from stdin

rc_ssh() {  # rc_ssh <host> <remote-cmd-string>  : stdout only, noise stripped
  local h; h="$(rc_host "$1")"; shift
  ssh "${RC_SSH_OPTS[@]}" "$h" "$@" 2>/dev/null | rc_clean
}

rc_has_session() {  # 0 if a session exists
  local h; h="$(rc_host "$1")"
  ssh "${RC_SSH_OPTS[@]}" "$h" "tmux has-session -t $RC_SESSION 2>/dev/null" >/dev/null 2>&1
}

rc_sk() {  # rc_sk <host> <send-keys args...>  : control keys only (Enter, C-c, Escape, -l "literal")
  local h; h="$(rc_host "$1")"; shift
  ssh "${RC_SSH_OPTS[@]}" "$h" tmux send-keys -t "$RC_SESSION" "$@" >/dev/null 2>&1
}

rc_enter() { rc_sk "$1" Enter; }

# Write a local file to the remote verbatim. Uses `cat >` instead of scp — the account banner the
# remote .bashrc prints on every ssh breaks the scp protocol ("Received message too long").
# The banner only goes to remote stdout while the file gets stdin only, so this is safe.
rc_put() {  # rc_put <host> <local-file> <remote-path>   (remote-path may use ~)
  local h; h="$(rc_host "$1")"
  ssh "${RC_SSH_OPTS[@]}" "$h" "cat > $3" < "$2" >/dev/null 2>&1
}

# Paste arbitrary text (from a local file) into the pane input verbatim — safe for shell expansion /
# special chars. load-buffer reads the file literally, so $, `, " etc. are not mangled.
rc_paste_file() {  # rc_paste_file <host> <local-textfile>
  local h; h="$(rc_host "$1")"; local f="$2"
  rc_put "$h" "$f" "~/$RC_CTRL/.inject.txt" || return 1
  ssh "${RC_SSH_OPTS[@]}" "$h" \
    "tmux load-buffer -b rcbuf ~/$RC_CTRL/.inject.txt && tmux paste-buffer -b rcbuf -t $RC_SESSION -d" \
    >/dev/null 2>&1
}

rc_paste_text() {  # rc_paste_text <host> <text...>  : paste text via a temp file
  local h="$1"; shift
  local tmp; tmp="$RC_STATE_DIR/.inject.$(rc_short "$h").txt"
  printf '%s' "$*" > "$tmp"
  rc_paste_file "$h" "$tmp"
}

rc_log() {  # local timestamped log (per host)
  local h; h="$(rc_short "$1")"; shift
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >> "$RC_STATE_DIR/$h.log"
}

rc_event() {  # events the supervisor should see (shared timeline across hosts)
  printf '[%s] %s: %s\n' "$(date '+%m-%d %H:%M:%S')" "$(rc_short "$1")" "$2" >> "$RC_STATE_DIR/events.log"
}

rc_state_file() { printf '%s/%s.state' "$RC_STATE_DIR" "$(rc_short "$1")"; }

rc_state_get() {  # rc_state_get <host> <key>
  local f; f="$(rc_state_file "$1")"
  grep -m1 "^$2=" "$f" 2>/dev/null | cut -d= -f2-
}

rc_state_set() {  # rc_state_set <host> <key> <value>
  local f; f="$(rc_state_file "$1")"; touch "$f"
  grep -v "^$2=" "$f" > "$f.t" 2>/dev/null; printf '%s=%s\n' "$2" "$3" >> "$f.t"; mv "$f.t" "$f"
}

# The 'other' account to switch to on this host when tokens are exhausted.
rc_other_acct() { case "$1" in work) printf 'personal';; personal) printf 'work';; *) printf 'work';; esac; }

# Build the kickoff/resume mission line. Prefixes /loop · /goal based on RC_KICKOFF_MODE.
# Vanilla claude's built-in /loop (self-loop) drives "keep going"; completion is the file sentinel (~/rc/DONE).
rc_mission() {  # rc_mission start|resume   → one line to stdout
  local prefix="" body
  case "$RC_KICKOFF_MODE" in loop) prefix="/loop ";; goal) prefix="/goal ";; esac
  if [ "$1" = "resume" ]; then
    body="Resume the autonomous task in ~/rc/SPEC.md (repo ~/$RC_REPO_REL). If ~/rc/DONE already exists, stop immediately. Otherwise read ~/rc/PROGRESS.md for what is already done and continue from the next unchecked item in ~/rc/SPEC.md. Mark items [x] in SPEC.md, append progress to ~/rc/PROGRESS.md, log hard blockers to ~/rc/BLOCKED.md and keep going with other items. ONLY when every item is checked AND verification passes, run: echo done > ~/rc/DONE . Do not stop until then."
  else
    body="You are running fully autonomously; no human is watching. Your complete task is in ~/rc/SPEC.md; work inside the repo ~/$RC_REPO_REL. Execute it end to end without waiting for confirmation. After finishing each checklist item, mark it [x] in ~/rc/SPEC.md and append one line to ~/rc/PROGRESS.md. Make reasonable decisions yourself; never pause to ask. Log hard blockers to ~/rc/BLOCKED.md and continue with other items. Run the verification commands listed in SPEC.md. ONLY when every item is checked AND verification passes, run: echo done > ~/rc/DONE . If ~/rc/DONE already exists, stop. Keep working until done."
  fi
  printf '%s%s' "$prefix" "$body"
}

# Wait adaptively until claude's chat input is ready.
#   return 0: ready / 2: blocked on an unexpected start gate / 1: timeout
rc_wait_ready() {  # rc_wait_ready <host>
  local h="$1" i cap
  for i in $(seq 1 14); do
    cap="$(ssh "${RC_SSH_OPTS[@]}" "$(rc_host "$h")" "tmux capture-pane -p -J -t $RC_SESSION -S -10" 2>/dev/null | rc_clean)"
    printf '%s' "$cap" | grep -q 'bypass permissions on' && return 0
    printf '%s' "$cap" | grep -qiE 'for shortcuts|↵ to send' && return 0
    printf '%s' "$cap" | grep -qiE 'Select login method|trust this folder|Bypass Permissions mode' && return 2
    sleep 2
  done
  return 1
}
