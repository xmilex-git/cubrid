#!/usr/bin/env bash
#
# harvest_weights.sh — derive a per-.sql runtime table from CTP SQL run logs, for
# feeding `ctp_parallel.sh --weights <file>` (time-balanced shards instead of
# count-balanced).
#
# CTP logs each case (sql/bin/run.sh via the Java ConsoleAgent) as:
#   [HH:MM:SS] Testing <abs-scenario>/.../cases/<name>.sql (i/N pct) [OK|NOK]
# There is NO explicit per-case duration, so we approximate each case's cost as the
# delta between consecutive Testing-line timestamps (1-second resolution). That
# delta also absorbs any server restart / crash-recovery the case triggers — which
# IS real shard wall-time, so attributing it to that case is what we want for
# balancing. Limitations (documented, not hidden):
#   * 1s resolution: sub-second cases read as 0 (fine — they sum within a dir).
#   * the last case in each log has no following timestamp (dropped).
#   * a long IDLE gap (paused run, concatenated runs) inflates one case; use --cap.
#   * best signal = the per-SHARD console.log of a real parallel run (cases run
#     back-to-back there); a stale serial log mixes setup gaps in.
# Across multiple logs the MAX per case wins (each case is normally in one shard).
#
# Usage:
#   harvest_weights.sh [--scenario <sql-dir>] [--cap <sec>] [--out <file>] <log|dir> ...
#     --scenario <dir>  scenario root to strip to a relpath. Default: $HOME/cubrid-testcases/sql
#     --cap <sec>       clamp any single per-case delta to <sec> (0 = no cap). Default: 0
#     --out <file>      write here instead of stdout
#     <log|dir> ...     CTP .log files, or dirs (searched for *.log and console.log)
#
# Output: "<sql-relpath>\t<seconds>" (scenario-relative, TAB-separated), sorted.
#
# Copyright (c) 2024 CUBRID test-infra. Apache-2.0.

set -euo pipefail

SCN="${HOME}/cubrid-testcases/sql"
CAP=0
OUT=""
INPUTS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --scenario) SCN="${2:-}"; shift 2 ;;
    --cap)      CAP="${2:-0}"; shift 2 ;;
    --out)      OUT="${2:-}"; shift 2 ;;
    -h|--help)  sed -n '2,40p' "$0"; exit 0 ;;
    -*)         echo "harvest_weights.sh: unknown option: $1" >&2; exit 2 ;;
    *)          INPUTS+=("$1"); shift ;;
  esac
done

[ "${#INPUTS[@]}" -gt 0 ] || { echo "harvest_weights.sh: need at least one log file or dir" >&2; exit 2; }
case "$CAP" in (*[!0-9]*) echo "harvest_weights.sh: --cap must be an integer" >&2; exit 2 ;; esac
SCN="${SCN%/}"

# Expand dirs to the log files inside them.
LOGS=()
for p in "${INPUTS[@]}"; do
  if [ -d "$p" ]; then
    while IFS= read -r f; do LOGS+=("$f"); done < <(find "$p" -type f \( -name '*.log' -o -name 'console.log' \) 2>/dev/null)
  elif [ -f "$p" ]; then
    LOGS+=("$p")
  else
    echo "harvest_weights.sh: not a file or dir (skipped): $p" >&2
  fi
done
[ "${#LOGS[@]}" -gt 0 ] || { echo "harvest_weights.sh: no log files found" >&2; exit 2; }

# Fixed-offset parse (POSIX awk; no gawk-only features). Prefix "[HH:MM:SS] Testing "
# is 19 chars; the case path starts at column 20 and runs up to " (".
TMP="$(mktemp "${TMPDIR:-.}/hw.XXXXXX")"
trap 'rm -f "$TMP"' EXIT

awk -v scn="$SCN" -v cap="$CAP" '
  function rel(p,   r) {
    r=p
    if (index(r, scn "/")==1) r=substr(r, length(scn)+2)
    else sub(/^.*\/sql\//, "", r)
    return r
  }
  FNR==1 { have=0; pt=0; pp="" }                 # reset per input file
  (substr($0,1,1)=="[" && substr($0,10,9)=="] Testing") {
    hh=substr($0,2,2)+0; mm=substr($0,5,2)+0; ss=substr($0,8,2)+0
    t=hh*3600+mm*60+ss
    rest=substr($0,20)
    ix=index(rest," (")
    if (ix<=0) next
    path=substr(rest,1,ix-1)
    if (path !~ /\.sql$/) next
    if (have) {
      dt=t-pt; if (dt<0) dt+=86400               # midnight wrap
      if (cap>0 && dt>cap) dt=cap
      r=rel(pp)
      if (!(r in mx) || dt>mx[r]) mx[r]=dt
    }
    pt=t; pp=path; have=1
  }
  END {
    n=0; tot=0
    for (r in mx) { printf "%s\t%d\n", r, mx[r]; n++; tot+=mx[r] }
    printf "harvest_weights: %d cases, %d s total derived time\n", n, tot > "/dev/stderr"
  }
' "${LOGS[@]}" | LC_ALL=C sort > "$TMP"

if [ -n "$OUT" ]; then
  mv "$TMP" "$OUT"; trap - EXIT
  echo "harvest_weights: wrote $(wc -l < "$OUT") rows -> $OUT" >&2
else
  cat "$TMP"
fi
