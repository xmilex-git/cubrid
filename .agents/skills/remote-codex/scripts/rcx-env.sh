#!/usr/bin/env bash
# remote-codex shared config/helpers.
# Other rcx-*.sh source this via `source "$(dirname "$0")/rcx-env.sh"`.
#
# Sibling of remote-claude, but drives the **codex** CLI instead of claude. Distinct session name
# (rcx) / control dir (~/rcx) / local state dir so both skills can run on the same worker at once
# (e.g. claude on 32, codex on 33).
#
# Environment facts (verified 2026-06-18 on host .33, codex-cli 0.141.0):
#   - Workers reached by IP directly: cubrid@192.168.6.30 / .32 / .33 (no ssh alias). codex on all three.
#   - Interactive TUI:   codex            (alias adds --dangerously-bypass-approvals-and-sandbox = YOLO/Full Access)
#   - Resume most recent: codex resume --last   (restores full history + context — verified)
#   - Autonomy primitive: /goal <one-line mission>   (codex's long-running-task driver; no /loop exists)
#   - Quit TUI:          two Ctrl-C → drops to shell
#   - Auth: single ChatGPT login in ~/.codex/auth.json. NO account-rotation helper (unlike claude-acct).
#     → on 5h exhaustion we WAIT for the limit to reset, then resume; weekly exhaustion → raise ATTENTION.
#   - No onboarding/trust gate: ~/.codex/config.toml already trusts ~/dev/cubrid and the bypass flag
#     skips approvals, so codex launches straight into the TUI (no preseed needed).
#   - Status line (bottom of pane) is the authoritative state signal:
#       model · dir · project · branch · <Ready|Working> · Full Access · Context N% left · 5h N% left · weekly N% left
#     right side shows `Pursuing goal (Ns)` / `Goal achieved (Ns)`.
#
# Token/credential values are never stored in this skill — codex holds its own ChatGPT auth.

# ── Config (overridable via env) ────────────────────────────────────────────
RCX_SESSION="${RCX_SESSION:-rcx}"                     # one tmux session name per host (distinct from remote-claude's `rc`)
RCX_CTRL="${RCX_CTRL:-rcx}"                            # control dir relative to remote home (~/rcx)
RCX_REPO_REL="${RCX_REPO_REL:-dev/cubrid}"            # work repo relative to remote home (~/dev/cubrid)
RCX_STATE_DIR="${RCX_STATE_DIR:-$HOME/.claude/scratch/remote-codex}"  # local state/logs
RCX_PANE_X="${RCX_PANE_X:-220}"
RCX_PANE_Y="${RCX_PANE_Y:-50}"
# Launch codex bypassing the shell alias (so the bypass flag is passed exactly once, not doubled).
RCX_LAUNCH="${RCX_LAUNCH:-command codex --dangerously-bypass-approvals-and-sandbox}"
RCX_RESUME_ARGS="${RCX_RESUME_ARGS:-resume --last}"   # continue the most recent session without the picker
RCX_SSH_OPTS=(-o ConnectTimeout=10 -o BatchMode=yes -o LogLevel=ERROR)

# Strip the account banner the .bashrc prints on every ssh + the ssh PQ warning, from stdout/decisions.
# NOTE: the Korean substrings below are LITERAL — they match the remote banner text. Do not translate.
RCX_NOISE='활성 계정|claude 에 적용|claude-acct|적용됨|토큰 해제|post-quantum|WARNING: connection|store now|decrypt later|may need to be upgraded|openssh.com/pq'

mkdir -p "$RCX_STATE_DIR" 2>/dev/null || true

# ── Helpers ──────────────────────────────────────────────────────────────────
RCX_SUBNET="${RCX_SUBNET:-192.168.6}"   # worker subnet
RCX_USER="${RCX_USER:-cubrid}"          # worker account

rcx_host() {  # 30/32/33 or IP or user@ip → cubrid@192.168.6.NN
  case "$1" in
    "$RCX_USER"@*)        printf '%s' "$1" ;;
    "$RCX_SUBNET".*)      printf '%s@%s' "$RCX_USER" "$1" ;;
    [0-9]|[0-9][0-9]|[0-9][0-9][0-9]) printf '%s@%s.%s' "$RCX_USER" "$RCX_SUBNET" "$1" ;;
    *)                    printf '%s' "$1" ;;   # otherwise pass through (user may give a full target)
  esac
}

rcx_short() { printf '%s' "${1##*.}"; }   # anything → last octet → 30/32/33

rcx_clean() { grep -avE "$RCX_NOISE" 2>/dev/null || true; }  # strip noise from stdin

rcx_ssh() {  # rcx_ssh <host> <remote-cmd-string>  : stdout only, noise stripped
  local h; h="$(rcx_host "$1")"; shift
  ssh "${RCX_SSH_OPTS[@]}" "$h" "$@" 2>/dev/null | rcx_clean
}

rcx_has_session() {  # 0 if a session exists
  local h; h="$(rcx_host "$1")"
  ssh "${RCX_SSH_OPTS[@]}" "$h" "tmux has-session -t $RCX_SESSION 2>/dev/null" >/dev/null 2>&1
}

rcx_sk() {  # rcx_sk <host> <send-keys args...>  : control keys (Enter, C-c, Escape, -l "literal")
  local h; h="$(rcx_host "$1")"; shift
  ssh "${RCX_SSH_OPTS[@]}" "$h" tmux send-keys -t "$RCX_SESSION" "$@" >/dev/null 2>&1
}

rcx_enter() { rcx_sk "$1" Enter; }

# Write a local file to the remote verbatim. Uses `cat >` instead of scp — the account banner the
# remote .bashrc prints on every ssh breaks the scp protocol ("Received message too long").
# The banner only goes to remote stdout while the file gets stdin only, so this is safe.
rcx_put() {  # rcx_put <host> <local-file> <remote-path>   (remote-path may use ~)
  local h; h="$(rcx_host "$1")"
  ssh "${RCX_SSH_OPTS[@]}" "$h" "cat > $3" < "$2" >/dev/null 2>&1
}

# Paste arbitrary text (from a local file) into the pane input verbatim — safe for shell expansion /
# special chars. load-buffer reads the file literally, so $, `, " etc. are not mangled.
rcx_paste_file() {  # rcx_paste_file <host> <local-textfile>
  local h; h="$(rcx_host "$1")"; local f="$2"
  rcx_put "$h" "$f" "~/$RCX_CTRL/.inject.txt" || return 1
  ssh "${RCX_SSH_OPTS[@]}" "$h" \
    "tmux load-buffer -b rcxbuf ~/$RCX_CTRL/.inject.txt && tmux paste-buffer -b rcxbuf -t $RCX_SESSION -d" \
    >/dev/null 2>&1
}

rcx_paste_text() {  # rcx_paste_text <host> <text...>  : paste text via a temp file
  local h="$1"; shift
  local tmp; tmp="$RCX_STATE_DIR/.inject.$(rcx_short "$h").txt"
  printf '%s' "$*" > "$tmp"
  rcx_paste_file "$h" "$tmp"
}

rcx_log() {  # local timestamped log (per host)
  local h; h="$(rcx_short "$1")"; shift
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >> "$RCX_STATE_DIR/$h.log"
}

rcx_event() {  # events the supervisor should see (shared timeline across hosts)
  printf '[%s] %s: %s\n' "$(date '+%m-%d %H:%M:%S')" "$(rcx_short "$1")" "$2" >> "$RCX_STATE_DIR/events.log"
}

rcx_state_file() { printf '%s/%s.state' "$RCX_STATE_DIR" "$(rcx_short "$1")"; }

rcx_state_get() {  # rcx_state_get <host> <key>
  local f; f="$(rcx_state_file "$1")"
  grep -m1 "^$2=" "$f" 2>/dev/null | cut -d= -f2-
}

rcx_state_set() {  # rcx_state_set <host> <key> <value>
  local f; f="$(rcx_state_file "$1")"; touch "$f"
  grep -v "^$2=" "$f" > "$f.t" 2>/dev/null; printf '%s=%s\n' "$2" "$3" >> "$f.t"; mv "$f.t" "$f"
}

# Build the kickoff/resume mission line. codex's /goal sets a long-running objective and keeps
# pursuing it; completion is the file sentinel (~/rcx/DONE). Mission MUST stay a single line.
rcx_mission() {  # rcx_mission start|resume   → one line to stdout
  local body
  if [ "$1" = "resume" ]; then
    body="Resume the autonomous task in ~/rcx/SPEC.md (repo ~/$RCX_REPO_REL). If ~/rcx/DONE already exists, stop immediately. Otherwise read ~/rcx/PROGRESS.md for what is already done and continue from the next unchecked item in ~/rcx/SPEC.md. Mark items [x] in SPEC.md, append progress to ~/rcx/PROGRESS.md, log hard blockers to ~/rcx/BLOCKED.md and keep going with other items. ONLY when every item is checked AND verification passes, run: echo done > ~/rcx/DONE . Do not stop until then."
  else
    body="You are running fully autonomously; no human is watching. Your complete task is in ~/rcx/SPEC.md; work inside the repo ~/$RCX_REPO_REL. Execute it end to end without waiting for confirmation. After finishing each checklist item, mark it [x] in ~/rcx/SPEC.md and append one line to ~/rcx/PROGRESS.md. Make reasonable decisions yourself; never pause to ask. Log hard blockers to ~/rcx/BLOCKED.md and continue with other items. Run the verification commands listed in SPEC.md. ONLY when every item is checked AND verification passes, run: echo done > ~/rcx/DONE . If ~/rcx/DONE already exists, stop. Keep working until done."
  fi
  printf '/goal %s' "$body"
}

# Wait adaptively until codex's TUI input is ready.
#   return 0: ready / 2: blocked on a login/auth gate / 1: timeout
rcx_wait_ready() {  # rcx_wait_ready <host>
  local h="$1" i cap
  for i in $(seq 1 14); do
    cap="$(ssh "${RCX_SSH_OPTS[@]}" "$(rcx_host "$h")" "tmux capture-pane -p -J -t $RCX_SESSION -S -12" 2>/dev/null | rcx_clean)"
    printf '%s' "$cap" | grep -qiE 'OpenAI Codex|Full Access|% left|esc to interrupt|Ready ·' && return 0
    printf '%s' "$cap" | grep -qiE 'Sign in|Not logged in|/login|press enter to (log|sign)|authenticate' && return 2
    sleep 2
  done
  return 1
}
