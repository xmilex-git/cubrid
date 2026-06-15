#!/usr/bin/env bash
# Inject direction/nudge into a running remote claude.
#   rc-steer.sh <host> "<message>" [--interrupt]
#   - just paste the message into the input and Enter (also usable as a "continue" nudge when idle)
#   - --interrupt: press Esc first to stop the current generation, then inject (to change course fast)
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/rc-env.sh"

host="$(rc_host "${1:-}")"; msg="${2:-}"; flag="${3:-}"
[ -z "$host" ] || [ -z "$msg" ] && { echo 'usage: rc-steer.sh <30|32|33> "<message>" [--interrupt]'; exit 2; }
rc_has_session "$host" || { echo "❌ no session on $host"; exit 3; }

if [ "$flag" = "--interrupt" ]; then
  rc_sk "$host" Escape; sleep 0.6
fi
rc_paste_text "$host" "$msg"; sleep 0.8; rc_enter "$host"
rc_log "$host" "steer: ${msg:0:80}"
echo "📨 $host injected: ${msg:0:80}"
