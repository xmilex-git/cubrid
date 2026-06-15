#!/usr/bin/env bash
#
# ctp_parallel.sh — one-click parallel CTP SQL regression runner.
#
# Splits the CUBRID CTP SQL suite across N rootless-podman shards (one container
# per shard), each running the real `ctp.sh sql` against a private, pristine copy
# of the scenario tree / build / CTP-conf. The suite is partitioned via per-shard
# exclusions.txt files (the same mechanism CTP itself uses), then results are
# merged into one pass/fail summary.
#
# Isolation is by namespace, NOT by port/SHM reassignment: every shard reuses the
# SAME ports/SHM IDs from sql.conf; podman's net + IPC + mount namespaces keep them
# from colliding. The only things that differ per shard are its exclusions.txt and
# its output directories.
#
# See README.md for the design rationale and the manual podman e2e QA steps.
#
# Copyright (c) 2024 CUBRID test-infra. Apache-2.0.

set -euo pipefail

#####################################################################
# Constants — container-internal mount targets (NOT host paths).
#####################################################################
readonly C_CUBRID="/home/CUBRID"
readonly C_CTP="/home/CTP"
readonly C_DB="/home/CUBRID_DB"
readonly C_SCN="/home/cubrid-testcases/sql"
# Default = a host-matched RUNTIME image built on demand from the bundled
# Containerfile (Rocky 8 / glibc that matches a modern host build). The CI build
# image (cubridci/cubridci:develop) is CentOS 6 / glibc 2.12 and canNOT run a
# binary built on a modern host, so it is NOT the default; pass --image to override.
readonly DEFAULT_IMAGE="ctp-parallel:local"

SELF="$(basename "$0")"
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly CGROUPNS="private"   # rootless on cgroup-v1 hosts fails with the default ns

#####################################################################
# Tiny helpers
#####################################################################
info()  { printf '[ctp-parallel] %s\n' "$*"; }
warn()  { printf '[ctp-parallel] WARN: %s\n' "$*" >&2; }
err()   { printf '[ctp-parallel] ERROR: %s\n' "$*" >&2; }
die()   { err "$*"; exit 1; }

usage() {
  cat <<EOF
$SELF — run the CTP SQL suite in N parallel podman shards.

USAGE:
  $SELF --build <CUBRID_dir> --testcases <repo_root> [options]
  $SELF --dry-run --testcases <repo_root> [options]      # plan/validate only, no podman

REQUIRED (for a real run):
  --build <dir>        A built \$CUBRID directory (copied per shard; absorbs CTP's conf rewrites).
  --testcases <root>   Testcases repo root; scenario is <root>/sql.

OPTIONS:
  --shards <N>         Number of parallel shards. Default: 7 (workload-optimal for the bulk +
                       measured-time split; the heaviest bulk bounds the slowest shard, so >7
                       gains nothing). Capped down only if free RAM can't hold 7.
  --ctp <dir>          CTP_HOME. Default: \$HOME/cubrid-testtools/CTP
  --image <ref>        Container image. Default: $DEFAULT_IMAGE
  --out <dir>          Output / work dir. Default: ./ctp-parallel-out
  --overlay            Use a podman ':O' overlay mount for the build instead of cp -a (D1, experimental).
  DEFAULT split unit = top-level _* directory ("bulk" = CircleCI's sql unit). Each bulk runs
  WHOLE on one shard (never split across shards), so related tests stay co-located exactly as CI
  groups them — this avoids the cross-test/shared-DB interference that finer splits expose. Pair
  with --weights to balance bulks by measured time (each bulk's time = sum of its cases).
  --by-category        Same as the default (explicit): top-level _* bulks.
  --by-dir             Finer: by outermost cases/ dir (~1157 units -> better time balance, but
                       co-locates fewer related tests, so may expose test-isolation failures the
                       bulk grouping avoids).
  --by-case            Finest: per-.sql. UNSAFE for order-sensitive suites; opt in only when the
                       targeted cases are known independent.
  --colocate <file>    Order-sensitivity registry (default: bundled colocate.tsv if present).
                       Each line is a group of cases dirs kept WHOLE in every mode (incl.
                       --by-case); 2+ dirs on a line are also pinned to the SAME shard. See
                       colocate.tsv for the format.
  --no-colocate        Ignore the registry (no keep-whole / co-locate constraints).
  --keep               Do not 'podman rm' the shard containers after the run.
  --weights <file>     Per-case cost table ("<sql-relpath><TAB><seconds>") to balance by measured
                       TIME. DEFAULT: the bundled baseline_weights.tsv (real per-case times) is
                       loaded automatically, so runs are time-balanced out of the box. Pass a file
                       to override (e.g. a fresh harvest from scripts/harvest_weights.sh); cases
                       absent from the table get weight 1.
  --no-weights         Ignore the bundled table and balance by case COUNT instead.
  --locale-dir <dir>   Dir with a prebuilt libcubrid_all_locales.so (+ optional early-exit
                       make_locale.sh). Injected into every shard so CTP skips the slow per-shard
                       locale compile. Falls back to compiling if a build tree already ships the .so.
  --no-webconsole      Do NOT merge per-shard results into \$CTP_HOME/sql/result. By default the
                       run is merged into one schedule dir viewable via 'ctp.sh webconsole start'.
  --merge-only <dir>   Merge an ALREADY-FINISHED run's --out <dir> into \$CTP_HOME/sql/result for
                       webconsole, then exit (no podman/build/testcases needed). Use for runs done
                       with --no-webconsole, or to re-merge an old run dir.
  --label <str>        Tag the merged run (shown in webconsole's 'machine' field) so several runs
                       are easy to tell apart. Default: the --out dir's basename.
  --dry-run            Plan only: discover units, balance, write exclusions/sql.conf/assignment.tsv,
                       run the offline split-validator. Does NOT need podman or --build.
  -h, --help           This help.

EXIT: non-zero if any shard fails, crashes, or a split invariant is violated.
EOF
}

#####################################################################
# Argument parsing
#####################################################################
ARG_BUILD=""
ARG_TC=""
ARG_SHARDS=""
ARG_CTP="${HOME}/cubrid-testtools/CTP"
ARG_IMAGE="$DEFAULT_IMAGE"
ARG_OUT="./ctp-parallel-out"
ARG_OVERLAY=0
ARG_UNIT="category"   # split-unit mode: category (top-level _* "bulk", DEFAULT, = CI's sql unit) | dir | case
ARG_KEEP=0
ARG_WEIGHTS="auto"   # "auto" = bundled baseline_weights.tsv (time-based) | <path> | "none" (count)
ARG_LOCALE_DIR=""
ARG_WEBCONSOLE=1
ARG_COLOCATE="auto"   # "auto" = bundled colocate.tsv if present; a path = that file; "" = disabled
ARG_MERGE_ONLY=""     # path to a finished --out dir to merge into webconsole, then exit
ARG_LABEL=""          # human tag for the merged run (webconsole 'machine' field)
ARG_DRYRUN=0
ARG_VALIDATE_ONLY=0
VO_ASSIGN=""
VO_SQL=""

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --build)       ARG_BUILD="${2:-}"; shift 2 ;;
      --testcases)   ARG_TC="${2:-}"; shift 2 ;;
      --shards)      ARG_SHARDS="${2:-}"; shift 2 ;;
      --ctp)         ARG_CTP="${2:-}"; shift 2 ;;
      --image)       ARG_IMAGE="${2:-}"; shift 2 ;;
      --out)         ARG_OUT="${2:-}"; shift 2 ;;
      --overlay)     ARG_OVERLAY=1; shift ;;
      --by-category) ARG_UNIT="category"; shift ;;
      --by-dir)      ARG_UNIT="dir"; shift ;;
      --by-case)     ARG_UNIT="case"; shift ;;
      --keep)        ARG_KEEP=1; shift ;;
      --weights)     ARG_WEIGHTS="${2:-}"; shift 2 ;;
      --no-weights)  ARG_WEIGHTS="none"; shift ;;
      --locale-dir)  ARG_LOCALE_DIR="${2:-}"; shift 2 ;;
      --colocate)    ARG_COLOCATE="${2:-}"; shift 2 ;;
      --no-colocate) ARG_COLOCATE=""; shift ;;
      --no-webconsole) ARG_WEBCONSOLE=0; shift ;;
      --merge-only)  ARG_MERGE_ONLY="${2:-}"; shift 2 ;;
      --label)       ARG_LABEL="${2:-}"; shift 2 ;;
      --dry-run)     ARG_DRYRUN=1; shift ;;
      # Hidden self-test seam: run ONLY the offline split-validator against the
      # given assignment.tsv + .sql list (used by run_tests.sh on synthetic data).
      --validate-only) ARG_VALIDATE_ONLY=1; VO_ASSIGN="${2:-}"; VO_SQL="${3:-}"; shift 3 ;;
      -h|--help)     usage; exit 0 ;;
      *)             usage; die "unknown argument: $1" ;;
    esac
  done

  if [ "$ARG_VALIDATE_ONLY" -eq 1 ]; then
    [ -r "$VO_ASSIGN" ] || die "--validate-only: assignment file unreadable: $VO_ASSIGN"
    [ -r "$VO_SQL" ]    || die "--validate-only: sql-list file unreadable: $VO_SQL"
    return 0
  fi

  # --merge-only needs only a finished out dir + a CTP_HOME to merge into (no build/testcases).
  if [ -n "$ARG_MERGE_ONLY" ]; then
    [ -d "$ARG_MERGE_ONLY" ] || die "--merge-only dir does not exist: $ARG_MERGE_ONLY"
    [ -d "$ARG_CTP" ]        || die "--merge-only: --ctp dir does not exist: $ARG_CTP"
    [ -d "$ARG_CTP/sql" ]    || die "--merge-only: $ARG_CTP has no sql/ (need a CTP_HOME with webconsole)"
    return 0
  fi

  [ -n "$ARG_TC" ]  || { usage; die "--testcases is required"; }
  [ -d "$ARG_TC" ]  || die "--testcases dir does not exist: $ARG_TC"
  SCN="$ARG_TC/sql"
  [ -d "$SCN" ]     || die "scenario dir not found: $SCN (expected <testcases>/sql)"
  [ -d "$ARG_CTP" ] || die "--ctp dir does not exist: $ARG_CTP"
  [ -r "$ARG_CTP/conf/sql.conf" ] || die "missing CTP template: $ARG_CTP/conf/sql.conf"

  if [ "$ARG_DRYRUN" -eq 0 ]; then
    [ -n "$ARG_BUILD" ] || { usage; die "--build is required for a real run (omit only with --dry-run)"; }
    [ -d "$ARG_BUILD" ] || die "--build dir does not exist: $ARG_BUILD"
    # Resolve symlinks: a CUBRID install is often a symlink (e.g. ~/CUBRID -> .../CUBRID-11.5.x).
    # cp -a of a symlink would create a dangling link in the shard workdir, so copy the target.
    ARG_BUILD="$(readlink -f "$ARG_BUILD")"
    [ -d "$ARG_BUILD" ] || die "--build resolved to a non-directory: $ARG_BUILD"
  fi
  # Normalise SCN to an absolute path with no trailing slash (matches scenarioRootPath in F4).
  SCN="$(cd "$SCN" && pwd)"
}

#####################################################################
# Host preflight — podman must exist for a real run. Runs BEFORE any
# work dirs are created, so a missing podman leaves nothing behind.
#####################################################################
host_preflight() {
  if ! command -v podman >/dev/null 2>&1; then
    err "podman is not installed or not on PATH."
    err "This tool launches one rootless podman container per shard; it cannot run without podman."
    err "Install podman (rootless) and retry, or use --dry-run to validate the split logic only."
    exit 3
  fi
  if ! podman image exists "$ARG_IMAGE" 2>/dev/null; then
    if [ "$ARG_IMAGE" = "$DEFAULT_IMAGE" ] && [ -r "$SELF_DIR/Containerfile" ]; then
      info "image '$ARG_IMAGE' not present; building from $SELF_DIR/Containerfile ..."
      podman build --cgroupns="$CGROUPNS" -t "$ARG_IMAGE" -f "$SELF_DIR/Containerfile" "$SELF_DIR" \
        || die "could not build image '$ARG_IMAGE' from Containerfile."
    else
      info "image '$ARG_IMAGE' not present locally; attempting 'podman pull'..."
      podman pull "$ARG_IMAGE" || die "could not obtain image '$ARG_IMAGE' (pull failed)."
    fi
  fi
  # Rough disk headroom check: N working copies of build+scenario+CTP.
  local avail_kb
  avail_kb="$(df -Pk "$(dirname "$ARG_OUT")" 2>/dev/null | awk 'NR==2{print $4}')"
  [ -n "${avail_kb:-}" ] && info "disk available at out parent: $((avail_kb/1024/1024)) GB"
}

#####################################################################
# Resource sizing — pick N when --shards not given.
#   DEFAULT = 7 (DEFAULT_SHARDS): the workload-optimal count for the default
#   bulk(_*) + measured-time split on the CUBRID sql suite. The slowest shard is
#   bounded by the heaviest single bulk (~323s here, _05_plcsql), so the knee is
#   N* = ceil(total_time / heaviest_bulk) = ceil(2204/323) = 7 — at 7 shards every
#   shard is full (~315-324s) and adding more shards only leaves them idle (no
#   wall-clock gain) while costing extra build copies / RAM. (CircleCI uses 10 only
#   because it splits by name, not time.) Memory-guarded: capped down with a warning
#   if free RAM can't hold 7 shards, to avoid OOM; pass --shards to override.
#####################################################################
readonly DEFAULT_SHARDS=7
PER_SHARD_GB=3
choose_shards() {
  if [ -n "$ARG_SHARDS" ]; then
    case "$ARG_SHARDS" in (*[!0-9]*|"") die "--shards must be a positive integer";; esac
    [ "$ARG_SHARDS" -ge 1 ] || die "--shards must be >= 1"
    NSHARDS="$ARG_SHARDS"
    info "shard count: $NSHARDS (from --shards)"
    return
  fi
  NSHARDS=$DEFAULT_SHARDS
  local free_gb mem_cap
  free_gb="$(free -g 2>/dev/null | awk '/^Mem:/{print $7}')"; [ -z "${free_gb:-}" ] && free_gb=$(( DEFAULT_SHARDS * PER_SHARD_GB ))
  mem_cap=$(( free_gb / PER_SHARD_GB )); [ "$mem_cap" -lt 1 ] && mem_cap=1
  if [ "$mem_cap" -lt "$NSHARDS" ]; then
    warn "default $DEFAULT_SHARDS shards needs ~$(( DEFAULT_SHARDS * PER_SHARD_GB ))GB; only ${free_gb}GB free -> capping to $mem_cap (override with --shards)."
    NSHARDS=$mem_cap
  fi
  info "shard count: $NSHARDS (default; workload-optimal for the bulk + measured-time split)"
}

#####################################################################
# Order-sensitivity registry (colocate.tsv). Produces two WORK files consumed by
# unit discovery / routing / balancing:
#   keepwhole.txt : one cases-dir per line -> routed at cases-dir granularity in
#                   EVERY mode (so --by-case never splits these), kept whole.
#   gids.txt      : "<cases-dir>\t<group-id>" -> dirs sharing a group-id are pinned
#                   to the SAME shard by the balancer.
# Both files always exist (possibly empty) so the awk getline never trips. A
# registered dir absent from the scenario is warned about, not fatal.
#####################################################################
COLO_KEEPWHOLE=""
COLO_GIDS=""
resolve_colocate() {
  COLO_KEEPWHOLE="$WORK/keepwhole.txt"
  COLO_GIDS="$WORK/gids.txt"
  : >"$COLO_KEEPWHOLE"; : >"$COLO_GIDS"
  local src=""
  case "$ARG_COLOCATE" in
    "")   info "colocate: disabled (--no-colocate)."; return 0 ;;
    auto) local bundled="$SELF_DIR/../colocate.tsv"; [ -r "$bundled" ] && src="$bundled" ;;
    *)    [ -r "$ARG_COLOCATE" ] || die "--colocate file not readable: $ARG_COLOCATE"; src="$ARG_COLOCATE" ;;
  esac
  if [ -z "$src" ]; then
    info "colocate: no registry found (bundled colocate.tsv absent); no constraints."
    return 0
  fi
  # Each non-comment line is a group; tokens are cases-dir relpaths (trailing '/'
  # tolerated). Emit "<dir>\tG<lineno>"; line number is a stable per-group id.
  awk '
    /^[[:space:]]*#/ { next }
    { gsub(/\r/,""); n=split($0,t,/[[:space:]]+/)
      for (i=1;i<=n;i++) if (t[i]!="") { d=t[i]; sub(/\/+$/,"",d); print d "\tG" NR }
    }
  ' "$src" > "$COLO_GIDS"
  cut -f1 "$COLO_GIDS" > "$COLO_KEEPWHOLE"
  local missing=0 d
  while IFS= read -r d; do
    [ -n "$d" ] || continue
    [ -d "$SCN/$d" ] || { warn "colocate: registered dir not in scenario (ignored): $d"; missing=$((missing+1)); }
  done < "$COLO_KEEPWHOLE"
  local ndirs ngroups
  ndirs=$(grep -c . "$COLO_KEEPWHOLE" 2>/dev/null || echo 0)
  ngroups=$(cut -f2 "$COLO_GIDS" 2>/dev/null | LC_ALL=C sort -u | grep -c . || echo 0)
  info "colocate: $ndirs dir(s) in $ngroups group(s) from $(basename "$src") — keep-whole applies to --by-case only; multi-dir groups pinned to one shard$([ "$missing" -gt 0 ] && echo "; $missing missing")."
}

#####################################################################
# Resolve the effective weights table (measured per-case seconds for time balancing).
#   auto (default) -> bundled baseline_weights.tsv if present, else count.
#   none           -> count-based (--no-weights).
#   <path>         -> that file (must be readable).
#####################################################################
WEIGHTS_FILE=""
resolve_weights() {
  case "$ARG_WEIGHTS" in
    none|"") WEIGHTS_FILE=""; info "weights: count-based (--no-weights)." ;;
    auto)
      local bundled="$SELF_DIR/../baseline_weights.tsv"
      if [ -r "$bundled" ]; then
        WEIGHTS_FILE="$bundled"
        info "weights: time-based from bundled $(basename "$bundled") ($(awk -F'\t' '{s+=$2}END{printf "%.0f", s}' "$bundled" 2>/dev/null)s over $(wc -l < "$bundled") cases)."
      else
        WEIGHTS_FILE=""; info "weights: no bundled table; count-based."
      fi ;;
    *)
      [ -r "$ARG_WEIGHTS" ] || die "--weights file not readable: $ARG_WEIGHTS"
      WEIGHTS_FILE="$ARG_WEIGHTS"; info "weights: time-based from $ARG_WEIGHTS." ;;
  esac
}

#####################################################################
# Unit discovery.
#   DEFAULT unit = top-level _* directory ("bulk"). This is exactly CircleCI's sql
#   split unit (.circleci/config.yml globs "cubrid-testcases/sql/_*" and ships each
#   match WHOLE to one node). A bulk is INDIVISIBLE — it is never split across shards
#   — so every test inside a _* dir stays co-located in canonical order, exactly as
#   CI groups them. That co-location is what keeps the suite green: finer splits move
#   tests apart and expose cross-test / shared-DB interference (one shard runs ONE
#   database for all its cases, and not every test self-isolates), producing failures
#   that depend on which tests share a shard. Bulk grouping mirrors CI and avoids it.
#   Balance bulks by measured time with --weights (a bulk's weight = sum of its cases'
#   seconds); without weights, by case count. Tradeoff: the heaviest single bulk is a
#   hard floor on the slowest shard, since a bulk cannot be split.
#   --by-dir:      unit = OUTERMOST cases dir (~1157 units; finer => better balance, but
#                  co-locates fewer related tests, so may expose isolation failures).
#   --by-case:     unit = the .sql itself (finest; NOT order-safe; opt in only when the
#                  targeted cases are known independent).
#   colocate.tsv:  applies ONLY to --by-case (keeps listed cases dirs whole / co-located);
#                  in dir/category the cases dir / bulk is already atomic.
# Emits to stdout: "<unit_relpath>\t<weight>" (scenario-relative, no leading '/').
# Honors base exclusions (F4): a .sql removed by the base list is not counted.
#####################################################################
UNITS_FILE=""      # tmp: unit \t sqlcount
SQL_LIST=""        # tmp: surviving .sql relpaths (post base exclusion)
SQL_ALL=""         # tmp: all .sql relpaths
BASE_FILE=""       # the original CTP exclusions.txt (verbatim base list)

discover_units() {
  BASE_FILE="$ARG_CTP/conf/exclusions.txt"
  SQL_ALL="$WORK/sql_all.txt"
  SQL_LIST="$WORK/sql_surviving.txt"
  UNITS_FILE="$WORK/units.tsv"

  # All .sql under a cases dir, as scenario-relative paths with NO leading '/'.
  # CTP's run.sh appends a trailing '/' to the scenario root before computing
  # caseRelativePath = caseFile.substring(rootLen), so the relative path it
  # matches against exclusions has NO leading slash. We must use the same
  # convention or the substring (containPath) match never fires.
  find "$SCN" -type f -name '*.sql' -path '*/cases/*' \
    | sed "s#^${SCN}/##" | LC_ALL=C sort > "$SQL_ALL"
  local total; total=$(wc -l < "$SQL_ALL")
  [ "$total" -gt 0 ] || die "no .sql found under $SCN"

  # Apply base exclusions (F4 containPath) to get the surviving pool.
  apply_base_exclusions "$SQL_ALL" "$SQL_LIST"

  GLOBAL_SQL=$total
  SURVIVING_SQL=$(wc -l < "$SQL_LIST")
  BASE_EXCLUDED=$(( GLOBAL_SQL - SURVIVING_SQL ))

  # Build "<unit>\t<weight>": aggregate the surviving .sql by their unit key and sum
  # weights (measured seconds from --weights, else 1 per .sql; an all-zero unit is
  # clamped to weight 1 so it is still schedulable). The unit key (see unitkey()) is
  # the cases dir by default, the top-level _* dir with --by-category, or the .sql
  # itself with --by-case.
  local wfile="${WEIGHTS_FILE:-/dev/null}"
  awk -F'\t' -v mode="$ARG_UNIT" -v wf="$wfile" -v kw="$COLO_KEEPWHOLE" '
    function unitkey(p,   u, c) {
      # category (DEFAULT): top-level _* "bulk" — atomic, never split across shards.
      if (mode=="category") { u=p; sub(/\/.*/,"",u); return u }
      # outermost cases dir of p (also the keep-whole lookup key).
      c=p; if (match(c,/\/cases\//)) c=substr(c,1,RSTART+RLENGTH-2)
      # keep-whole registry applies ONLY to --by-case (the only mode finer than a cases dir);
      # in dir/category the cases dir / bulk is already whole, so the registry must not pull a
      # cases dir out of its atomic bulk.
      if (mode=="case") return (c in KW) ? c : p
      return c                         # dir: outermost cases dir
    }
    BEGIN {
      while ((getline l < kw) > 0) if (l!="") KW[l]=1
      while ((getline l < wf) > 0) { m=split(l,a,"\t"); if (m>=2) w[a[1]]=a[2]+0 }
    }
    { p=$0; wt=(p in w)?w[p]:1; agg[unitkey(p)]+=wt }
    END { for (u in agg) printf "%s\t%d\n", u, (agg[u]<1?1:agg[u]) }
  ' "$SQL_LIST" | LC_ALL=C sort > "$UNITS_FILE"
}

# apply_base_exclusions <in_sql_list> <out_surviving_list>
# Replicates F4 CommonUtils.containPath byte-for-byte against every base entry.
apply_base_exclusions() {
  local in="$1" out="$2"
  # getLineList keeps every non-blank, trimmed line (comments included; they match nothing).
  awk 'NF{ gsub(/^[ \t]+|[ \t]+$/,""); if(length) print }' "$BASE_FILE" > "$WORK/base_entries.txt" 2>/dev/null || :
  [ -s "$WORK/base_entries.txt" ] || : >"$WORK/base_entries.txt"
  contain_path_filter "$WORK/base_entries.txt" "$in" "$out"
}

# contain_path_filter <entries_file> <paths_file> <surviving_out>
# Writes to <surviving_out> the paths NOT matched by ANY entry, using F4 semantics:
#   entry e -> e2 = (e ends with '/' or '.sql') ? e : e+'/' ; match if index(path,e2)>0
contain_path_filter() {
  local entries="$1" paths="$2" out="$3"
  # Read entries in BEGIN via getline (NOT the NR==FNR idiom): when the entries
  # file is EMPTY, NR==FNR stays true for the paths file too, so every path would
  # be misread as an exclusion entry and nothing would survive. getline keeps the
  # two inputs cleanly separate and yields n=0 (exclude nothing) on an empty file.
  awk -v entries="$entries" '
    BEGIN {
      n=0
      while ((getline e < entries) > 0) {
        if (e=="") continue
        if (substr(e,length(e))!="/" && substr(e,length(e)-3)!=".sql") e=e"/"
        ent[++n]=e
      }
    }
    {
      p=$0; excluded=0
      for (i=1;i<=n;i++) { if (index(p,ent[i])>0) { excluded=1; break } }
      if (!excluded) print p
    }
  ' "$paths" > "$out"
}

#####################################################################
# Greedy LPT balancing — assign units to shards, respecting co-locate groups.
#   A "group" is a co-locate group from the registry (its member units must share
#   a shard) or, for every other unit, the unit itself. Groups are packed weight
#   DESC (tie-break: group-id ASC, deterministic) into the least-loaded shard;
#   then each member unit inherits its group's shard. With no registry every group
#   is a singleton, so this reduces exactly to per-unit LPT.
# Produces: ASSIGN ("unit \t shard"), SHARD_LOAD[i] totals.
#####################################################################
ASSIGN_FILE=""
declare -a SHARD_LOAD
balance_units() {
  ASSIGN_FILE="$WORK/assignment.tsv"
  : >"$ASSIGN_FILE"
  local i
  for (( i=0; i<NSHARDS; i++ )); do SHARD_LOAD[i]=0; done

  # (1) unit -> group-id (default = the unit) and per-group summed weight.
  awk -F'\t' -v gidf="$COLO_GIDS" -v ugf="$WORK/unit_gid.tsv" -v gwf="$WORK/group_w.tsv" '
    BEGIN { while ((getline l < gidf) > 0) { m=split(l,a,"\t"); if (m>=2) gid[a[1]]=a[2] } }
    { u=$1; w=$2+0; g=(u in gid)?gid[u]:u; gw[g]+=w; print u "\t" g > ugf }
    END { for (g in gw) printf "%s\t%d\n", g, gw[g] > gwf }
  ' "$UNITS_FILE"

  # (2) LPT over groups (weight desc, group-id asc) -> group -> shard + #LOAD.
  LC_ALL=C sort -t"$(printf '\t')" -k2,2nr -k1,1 "$WORK/group_w.tsv" \
    | awk -F'\t' -v n="$NSHARDS" '
        BEGIN { for (i=0;i<n;i++) load[i]=0 }
        { g=$1; w=$2+0; best=0; for (i=1;i<n;i++) if (load[i]<load[best]) best=i; load[best]+=w; print g "\t" best }
        END { for (i=0;i<n;i++) printf "#LOAD\t%d\t%d\n", i, load[i] }
      ' > "$WORK/group_shard_raw.tsv"
  grep -v '^#LOAD' "$WORK/group_shard_raw.tsv" > "$WORK/group_shard.tsv"

  # (3) join unit -> group -> shard.
  awk -F'\t' '
    FNR==NR { gs[$1]=$2; next }            # group -> shard
    { print $1 "\t" gs[$2] }               # unit  -> shard
  ' "$WORK/group_shard.tsv" "$WORK/unit_gid.tsv" > "$ASSIGN_FILE"

  # capture loads
  while IFS=$'\t' read -r tag idx load; do
    [ "$tag" = "#LOAD" ] && SHARD_LOAD[idx]=$load
  done < <(grep '^#LOAD' "$WORK/group_shard_raw.tsv")
}

#####################################################################
# Expand the unit->shard assignment into per-shard .sql lists (the files each
# shard will MATERIALIZE) and per-shard .sql counts. Each surviving .sql is
# routed to exactly one shard via its unit, so this is an exact partition.
# Also writes a base-only exclusions.txt per shard (belt-and-suspenders: the
# assigned .sql are materialized, the rest are simply never copied).
#####################################################################
declare -a SHARD_NSQL
expand_shard_sets() {
  local i
  rm -f "$WORK/unmapped.txt"
  for (( i=0; i<NSHARDS; i++ )); do
    : >"$WORK/shard_${i}.sql.txt"
    SHARD_NSQL[i]=0
    cp -f "$BASE_FILE" "$WORK/shard_${i}.exclusions.txt"
  done
  awk -F'\t' -v mode="$ARG_UNIT" -v work="$WORK" -v kw="$COLO_KEEPWHOLE" '
    function unitkey(p,   u, c) {
      if (mode=="category") { u=p; sub(/\/.*/,"",u); return u }
      c=p; if (match(c,/\/cases\//)) c=substr(c,1,RSTART+RLENGTH-2)
      if (mode=="case") return (c in KW) ? c : p
      return c
    }
    BEGIN { while ((getline l < kw) > 0) if (l!="") KW[l]=1 }
    FNR==NR { sh[$1]=$2; next }                      # ASSIGN: unit -> shard
    {
      p=$0; u=unitkey(p); s=sh[u]
      if (s=="") { print p > (work"/unmapped.txt"); next }
      print p > (work"/shard_" s ".sql.txt")
      cnt[s]++
    }
    END { for (s in cnt) print s"\t"cnt[s] }
  ' "$ASSIGN_FILE" "$SQL_LIST" > "$WORK/shard_nsql.txt"
  while IFS=$'\t' read -r s c; do SHARD_NSQL[$s]=$c; done < "$WORK/shard_nsql.txt"
}

#####################################################################
# Split validation (offline; needs only assignment.tsv + the surviving .sql list,
# so it doubles as the --validate-only self-test seam).
#
# A unit U (a dir path) CLAIMS a .sql P iff P==U (per-case units) or P starts with
# U"/" (dir/category units). A clean split requires every surviving .sql to be
# claimed by EXACTLY ONE shard:
#   * 0 claiming units            -> ORPHAN: no shard would run it.
#   * units on >1 distinct shard  -> AMBIGUOUS: a nested cases dir landed on a
#                                    different shard than its outer dir, so the .sql
#                                    would be materialized in more than one shard.
# Proving this for every .sql proves the unit set is prefix-free (order-safe split
# units never overlap) AND covers the pool exactly once. Aborts (exit 4) otherwise.
#####################################################################
validate_split() {
  local nsql; nsql="$(wc -l < "$SQL_LIST")"
  : "${SURVIVING_SQL:=$nsql}"
  info "validating split over $nsql surviving .sql (each must be claimed by exactly one shard) ..."

  # Exact-key routing guard (real/dry-run path only; --validate-only has no WORK
  # tree): a .sql whose unit key is absent from the assignment was routed nowhere.
  if [ -n "${WORK:-}" ] && [ -s "$WORK/unmapped.txt" ]; then
    err "split validation FAILED: $(wc -l < "$WORK/unmapped.txt") surviving .sql not routed to any shard. Head:"
    sed -n '1,20p' "$WORK/unmapped.txt" >&2
    exit 4
  fi

  # Containment proof, independent of how routing was computed. For each .sql, walk
  # its path prefixes (and itself) and match against the assigned units.
  if awk -F'\t' '
        FNR==NR { U[$1]=$2; next }                   # ASSIGN: unit -> shard
        {
          p=$0; nclaim=0; sset=""
          if (p in U) { nclaim++; sset="<" U[p] ">" }
          L=length(p)
          for (k=1;k<=L;k++) {
            if (substr(p,k,1)=="/") {
              cand=substr(p,1,k-1)
              if (cand in U) {
                nclaim++; tag="<" U[cand] ">"
                if (index(sset,tag)==0) sset=sset tag
              }
            }
          }
          if (nclaim==0) { printf "ORPHAN (no assigned unit claims): %s\n", p > "/dev/stderr"; bad++; next }
          ndist=gsub(/>/,">",sset)                    # one ">" per distinct shard tag
          if (ndist>1) { printf "ORPHAN/AMBIGUOUS (claimed by %d units across shards %s): %s\n", nclaim, sset, p > "/dev/stderr"; bad++ }
        }
        END { exit (bad>0)?1:0 }
      ' "$ASSIGN_FILE" "$SQL_LIST"; then
    info "split valid: all $nsql surviving .sql alive in exactly one shard."
  else
    err "split validation FAILED: orphaned or cross-shard-ambiguous .sql (see ORPHAN lines above)."
    exit 4
  fi
}

#####################################################################
# Generate a per-shard sql.conf from the real template:
#   - scenario  -> the container scenario path (no host paths leak)
#   - testcase_exclude_from_file -> ${CTP_HOME}/conf/exclusions.txt (container)
#   - ports / SHM IDs kept verbatim (namespace isolation handles conflicts)
#####################################################################
generate_sql_conf() {
  local out="$1"
  sed -E \
    -e "s#^[[:space:]]*scenario=.*#scenario=${C_SCN}#" \
    -e "s#^[[:space:]]*testcase_exclude_from_file=.*#testcase_exclude_from_file=\${CTP_HOME}/conf/exclusions.txt#" \
    "$ARG_CTP/conf/sql.conf" > "$out"
}

#####################################################################
# Emit the plan artifacts into --out (always; used by --dry-run and real runs).
#####################################################################
emit_plan() {
  mkdir -p "$OUT"
  cp -f "$ASSIGN_FILE" "$OUT/assignment.tsv"
  cp -f "$UNITS_FILE"  "$OUT/units.tsv"
  local i
  : >"$OUT/plan.tsv"
  printf '# shard\tsql_count\tweight\n' >>"$OUT/plan.tsv"
  for (( i=0; i<NSHARDS; i++ )); do
    printf '%d\t%d\t%d\n' "$i" "${SHARD_NSQL[i]:-0}" "${SHARD_LOAD[i]:-0}" >>"$OUT/plan.tsv"
    mkdir -p "$OUT/shard_${i}"
    cp -f "$WORK/shard_${i}.exclusions.txt" "$OUT/shard_${i}/exclusions.txt"
    cp -f "$WORK/shard_${i}.sql.txt" "$OUT/shard_${i}/assigned_sql.txt" 2>/dev/null || :
    generate_sql_conf "$OUT/shard_${i}/sql.conf"
  done
  info "plan written to $OUT (assignment.tsv, units.tsv, plan.tsv, shard_*/{exclusions.txt,sql.conf})"
}

print_plan_summary() {
  info "split summary:"
  printf '  global .sql:        %d\n' "$GLOBAL_SQL"
  printf '  base-excluded .sql: %d\n' "$BASE_EXCLUDED"
  printf '  surviving .sql:     %d\n' "$SURVIVING_SQL"
  local modelabel
  case "$ARG_UNIT" in
    dir)      modelabel="cases-dir" ;;
    category) modelabel="bulk(_*)" ;;
    case)     modelabel="per-case" ;;
    *)        modelabel="$ARG_UNIT" ;;
  esac
  printf '  units:              %d  (mode: %s, weight: %s)\n' "$(wc -l < "$UNITS_FILE")" \
    "$modelabel" \
    "$([ -n "$WEIGHTS_FILE" ] && echo measured-time || echo count)"
  # awk line/group counts (always exit 0 and emit exactly one integer, even on an
  # empty file — unlike `grep -c`, which prints 0 AND exits 1, doubling under `||`).
  local kwn=0 grpn=0
  [ -r "$COLO_KEEPWHOLE" ] && kwn=$(awk 'END{print NR+0}' "$COLO_KEEPWHOLE" 2>/dev/null)
  [ -r "$COLO_GIDS" ] && grpn=$(awk -F'\t' '{c[$2]++} END{m=0; for(g in c) if(c[g]>1) m++; print m+0}' "$COLO_GIDS" 2>/dev/null)
  printf '  colocate:           %d dir(s), %d multi-dir group(s) pinned  (keep-whole active only with --by-case)\n' "${kwn:-0}" "${grpn:-0}"
  printf '  shards:             %d\n' "$NSHARDS"
  printf '  %-7s %-10s %s\n' "shard" "sql" "weight$([ -n "$WEIGHTS_FILE" ] && echo '(s)')"
  local i
  for (( i=0; i<NSHARDS; i++ )); do
    printf '  %-7d %-10d %d\n' "$i" "${SHARD_NSQL[i]:-0}" "${SHARD_LOAD[i]:-0}"
  done
}

#####################################################################
# Per-shard working-copy construction (host side).
#####################################################################
build_shard_workdir() {
  local i="$1" d="$OUT/shard_${i}"
  mkdir -p "$d"
  info "shard $i: building working copies under $d ..."

  # build -> CUBRID (writable copy absorbs CTP's conf rewrites; F3)
  if [ "$ARG_OVERLAY" -eq 1 ]; then
    info "shard $i: --overlay set; build mounted via podman :O overlay (no copy)."
  else
    cp -a "$ARG_BUILD" "$d/CUBRID"
    # Locale speedup (D6): CTP keeps need_make_locale=yes, but make_locale is the
    # single slowest startup step (~60-90s of gcc, repeated in EVERY shard). Ship a
    # prebuilt libcubrid_all_locales.so + an early-exit make_locale.sh so CTP's
    # make_locale finds the .so already present and returns immediately instead of
    # recompiling. The locale is thus compiled at most ONCE (offline), not per shard.
    if [ -n "$LOCALE_SO" ]; then
      cp -f "$LOCALE_SO" "$d/CUBRID/lib/libcubrid_all_locales.so"
      cp -f "$LOCALE_SCRIPT" "$d/CUBRID/bin/make_locale.sh"
      chmod +x "$d/CUBRID/bin/make_locale.sh" 2>/dev/null || :
    fi
  fi

  # testcases -> private scenario copy that MATERIALIZES ONLY this shard's .sql.
  # Pass A: dir structure + answers + everything EXCEPT test .sql / stale results.
  # Pass B: copy in only the .sql assigned to this shard.
  # Result: CTP finds exactly this shard's cases (no exclude filter needed), the
  # host tree is untouched (F5), and there is no giant exclusions list / O(n^2) match.
  mkdir -p "$d/scenario"
  if command -v rsync >/dev/null 2>&1; then
    rsync -a --delete --exclude='*.sql' --exclude='*.result' --exclude='*.log' "$SCN/" "$d/scenario/"
    rsync -a --files-from="$WORK/shard_${i}.sql.txt" "$SCN/" "$d/scenario/"
  else
    cp -a "$SCN/." "$d/scenario/"
    find "$d/scenario" -type f \( -name '*.result' -o -name '*.log' \) -delete
    # prune .sql NOT assigned to this shard
    ( cd "$d/scenario" && find . -type f -name '*.sql' -path '*/cases/*' | sed 's#^\./##' \
        | grep -vxF -f "$WORK/shard_${i}.sql.txt" | tr '\n' '\0' | xargs -0 -r rm -f )
  fi

  # CTP -> per-shard copy (drop stale run artifacts), then wire conf + exclusions
  mkdir -p "$d/CTP"
  if command -v rsync >/dev/null 2>&1; then
    rsync -a \
      --exclude='.output_*.log' --exclude='.script_cont_*' \
      --exclude='sql/result/*' --exclude='sql/log/*' \
      "$ARG_CTP/" "$d/CTP/"
  else
    cp -a "$ARG_CTP/." "$d/CTP/"
    rm -rf "$d/CTP"/.output_*.log "$d/CTP"/.script_cont_* "$d/CTP"/sql/result/* "$d/CTP"/sql/log/* 2>/dev/null || :
  fi
  generate_sql_conf "$d/CTP/conf/sql.conf"
  cp -f "$WORK/shard_${i}.exclusions.txt" "$d/CTP/conf/exclusions.txt"

  # fresh per-shard CUBRID_DATABASES
  mkdir -p "$d/CUBRID_DB"
  : >"$d/CUBRID_DB/databases.txt"
  mkdir -p "$d/out"
}

#####################################################################
# Decide how cub_server core dumps can be captured out of each shard.
# The kernel's core_pattern is a single global knob (not namespaced), but the
# path is resolved in the crashing process's mount-ns, so:
#   - absolute path  -> bind-mount the shard's cores/ over that dir (cores land on
#                       the host automatically). [CORE_MODE=path]
#   - relative       -> cores land in the server's cwd (under the mounted CUBRID*/
#                       CTP copies); collect them after the run.          [relative]
#   - pipe (|handler)-> the host's coredump handler gets them; we cannot capture
#                       per-shard, only warn.                              [pipe]
#####################################################################
CORE_MODE="none"
CORE_DIR=""
setup_core_capture() {
  local pat; pat="$(cat /proc/sys/kernel/core_pattern 2>/dev/null)"
  case "$pat" in
    \|*) CORE_MODE="pipe"
         err "WARNING: core_pattern pipes to a handler ('$pat'); per-shard cores cannot be"
         err "         captured. Cores (if any) go to the host coredump store." ;;
    /*)  CORE_MODE="path"; CORE_DIR="$(dirname "$pat")"
         info "core capture: bind-mounting each shard's cores/ over '$CORE_DIR' (pattern '$pat')." ;;
    ?*)  CORE_MODE="relative"
         info "core capture: core_pattern is relative ('$pat'); will collect cores from shard copies." ;;
    *)   CORE_MODE="none"
         err "WARNING: core_pattern is empty; cores may be disabled on this host." ;;
  esac
}

#####################################################################
# Launch one shard container (detached, NOT --rm).
#####################################################################
SHM_SIZE="2g"
launch_shard() {
  local i="$1" d="$OUT/shard_${i}" name="ctp_shard_$$_${i}"
  SHARD_NAMES[i]="$name"
  mkdir -p "$d/cores"
  local -a mounts=(
    -v "$d/CTP:${C_CTP}:rw"
    -v "$d/scenario:${C_SCN}:rw"
    -v "$d/CUBRID_DB:${C_DB}:rw"
  )
  if [ "$ARG_OVERLAY" -eq 1 ]; then
    mounts+=( -v "$ARG_BUILD:${C_CUBRID}:O" )
  else
    mounts+=( -v "$d/CUBRID:${C_CUBRID}:rw" )
  fi
  # Core-dump capture: the kernel's core_pattern is global but is resolved inside
  # the crashing process's mount-ns. If it is an absolute path, bind-mount the
  # shard's cores/ dir over that directory so a cub_server core lands on the host
  # automatically (and outside $CUBRID/$CTP_HOME, where CTP's clean_log_cores can't
  # delete it). See setup_core_capture for the relative/pipe fallbacks.
  if [ "$CORE_MODE" = "path" ]; then
    mounts+=( -v "$d/cores:${CORE_DIR}:rw" )
  fi
  info "shard $i: launching container $name (image $ARG_IMAGE) ..."
  # No --network=host, no published ports: each shard's localhost is private.
  # --ipc=private: private SysV SHM space so the fixed SHM IDs never collide.
  # --ulimit core=-1: unlimited core size so a real cub_server core is not truncated.
  podman run -d --name "$name" \
    --ipc=private \
    --cgroupns="$CGROUPNS" \
    --shm-size="$SHM_SIZE" \
    --ulimit core=-1 \
    -e "CUBRID=${C_CUBRID}" -e "CTP_HOME=${C_CTP}" -e "CUBRID_DATABASES=${C_DB}" \
    -e "TZ=Asia/Seoul" -e "LC_ALL=en_US" \
    "${mounts[@]}" \
    "$ARG_IMAGE" >/dev/null
}

#####################################################################
# Wait, collect, aggregate.
#####################################################################
declare -a SHARD_NAMES
declare -a SHARD_RC
wait_shards() {
  local i
  for (( i=0; i<NSHARDS; i++ )); do
    local name="${SHARD_NAMES[i]}" d="$OUT/shard_${i}"
    info "waiting on shard $i ($name) ..."
    SHARD_RC[i]="$(podman wait "$name" 2>/dev/null || echo 255)"
    podman logs "$name" > "$d/console.log" 2>&1 || :
    info "shard $i finished rc=${SHARD_RC[i]}"
  done
}

# Preserve artifacts BEFORE cleanup; then rm (unless --keep).
declare -a SHARD_CORES
collect_shards() {
  local i
  for (( i=0; i<NSHARDS; i++ )); do
    local d="$OUT/shard_${i}"
    cp -f "$WORK/shard_${i}.exclusions.txt" "$d/exclusions.txt" 2>/dev/null || :
    # CTP summary/logs already live in the per-shard CTP copy on the host.
    [ -d "$d/CTP/sql/result" ] && cp -a "$d/CTP/sql/result" "$d/out/" 2>/dev/null || :
    [ -d "$d/CTP/sql/log" ]    && cp -a "$d/CTP/sql/log"    "$d/out/" 2>/dev/null || :
    # Core dumps. path mode: already on the host in $d/cores via the bind-mount.
    # relative mode: cores landed in the server cwd inside the mounted CUBRID*/CTP
    # copies; sweep them into $d/cores before they are lost. (CTP only deletes cores
    # under $CUBRID/$CTP_HOME, so sweep those plus CUBRID_DB.)
    mkdir -p "$d/cores"
    if [ "$CORE_MODE" = "relative" ]; then
      find "$d/CUBRID" "$d/CUBRID_DB" "$d/CTP" -type f -name 'core*' 2>/dev/null \
        -exec sh -c 'f="$1"; [ "$(file -b "$f" 2>/dev/null | grep -c core)" -gt 0 ] && mv "$f" "$2/" || :' _ {} "$d/cores" \; 2>/dev/null || :
    fi
    SHARD_CORES[i]=$(find "$d/cores" -type f -name 'core*' 2>/dev/null | wc -l)
    [ "${SHARD_CORES[i]}" -gt 0 ] && err "shard $i: ${SHARD_CORES[i]} core dump(s) preserved in $d/cores"
  done
  cp -f "$ASSIGN_FILE" "$OUT/assignment.tsv" 2>/dev/null || :
  if [ "$ARG_KEEP" -eq 0 ]; then
    for (( i=0; i<NSHARDS; i++ )); do podman rm "${SHARD_NAMES[i]}" >/dev/null 2>&1 || :; done
  else
    info "--keep set: leaving containers in place."
  fi
}

# Parse "Fail/Success/Total" from a shard's CTP output. Echoes "fail success total".
parse_shard_result() {
  local d="$1" src=""
  # CTP writes a summary; search console + any result summary files.
  for cand in "$d/console.log" "$d"/CTP/sql/result/*summary* "$d"/out/result/*summary*; do
    [ -f "$cand" ] && src="$cand $src"
  done
  [ -n "$src" ] || { echo "0 0 0"; return; }
  # CTP prints its summary as capitalized "Key:Value" with no space, e.g.
  #   Fail:10
  #   Success:129
  #   Total:139
  # Split on ':'/'=', lowercase+trim the key, and pull the digits from the value.
  # Last matching block wins (console.log is parsed last, so it is authoritative).
  # shellcheck disable=SC2086
  awk -F'[:=]' '
    { key=tolower($1); gsub(/[ \t]/,"",key); val=$2; gsub(/[^0-9]/,"",val) }
    key=="fail"    && val!="" { f=val }
    key=="success" && val!="" { s=val }
    key=="total"   && val!="" { t=val }
    END { printf "%d %d %d\n", f+0, s+0, t+0 }
  ' $src 2>/dev/null
}

aggregate() {
  local i tot_fail=0 tot_succ=0 tot_total=0 tot_cores=0 any_crash=0
  info "==================== AGGREGATE ===================="
  printf '  %-7s %-8s %-8s %-8s %-8s %-8s %s\n' "shard" "rc" "fail" "success" "total" "cores" "expected"
  for (( i=0; i<NSHARDS; i++ )); do
    local d="$OUT/shard_${i}" f s t exp cores
    read -r f s t <<<"$(parse_shard_result "$d")"
    exp="${SHARD_NSQL[i]:-0}"
    cores="${SHARD_CORES[i]:-0}"
    tot_fail=$((tot_fail+f)); tot_succ=$((tot_succ+s)); tot_total=$((tot_total+t)); tot_cores=$((tot_cores+cores))
    local note=""
    [ "${SHARD_RC[i]}" != "0" ] && { note="CRASHED"; any_crash=1; }
    [ "$cores" -gt 0 ] && note="${note:+$note,}CORE"
    [ "$t" != "$exp" ] && note="${note:+$note,}TOTAL!=EXPECTED"
    printf '  %-7d %-8s %-8d %-8d %-8d %-8d %s %s\n' "$i" "${SHARD_RC[i]}" "$f" "$s" "$t" "$cores" "$exp" "$note"
  done
  printf '  %-7s %-8s %-8d %-8d %-8d %-8d %d\n' "ALL" "-" "$tot_fail" "$tot_succ" "$tot_total" "$tot_cores" "$SURVIVING_SQL"

  # Split invariant: Sigma CTP Total == surviving == global - base_excluded
  local fail=0
  if [ "$tot_total" -ne "$SURVIVING_SQL" ]; then
    err "INVARIANT VIOLATED: sum(CTP Total)=$tot_total != surviving=$SURVIVING_SQL"
    fail=1
  fi
  if [ "$SURVIVING_SQL" -ne "$(( GLOBAL_SQL - BASE_EXCLUDED ))" ]; then
    err "INVARIANT VIOLATED: surviving=$SURVIVING_SQL != global-base=$(( GLOBAL_SQL - BASE_EXCLUDED ))"
    fail=1
  fi
  [ "$any_crash" -ne 0 ] && { err "one or more shards crashed."; fail=1; }
  [ "$tot_cores" -ne 0 ] && { err "$tot_cores core dump(s) captured (see shard_*/cores/)."; fail=1; }
  [ "$tot_fail" -ne 0 ] && { err "$tot_fail test failure(s) across all shards."; fail=1; }

  if [ "$fail" -ne 0 ]; then
    err "RESULT: FAILED"
    return 1
  fi
  info "RESULT: PASSED (fail=0, total=$tot_total over $NSHARDS shards)"
  return 0
}

#####################################################################
# Merge the per-shard CTP result dirs into ONE schedule dir under
# $CTP_HOME/sql/result so `ctp.sh webconsole start` shows the whole parallel run
# as a single entry whose failures are all browsable (D7).
#
# CTP makes each schedule dir self-contained for FAILED cases: it co-locates the
# failing <case>.sql/.result/.answer inside the schedule's category tree, and the
# webconsole's failure view simply walks the schedule dir for *.sql. So merging is
# a UNION of every shard's schedule sql/ subtree plus one summed main.info at the
# root (findAllTestResults stops at the first main.info, so only the merged run is
# listed; per-shard main.info are intentionally NOT copied).
#####################################################################
merge_results() {
  [ "$ARG_WEBCONSOLE" -eq 1 ] || { info "webconsole merge skipped (--no-webconsole)."; return 0; }
  local resroot="$ARG_CTP/sql/result"
  if [ ! -d "$ARG_CTP/sql" ]; then
    warn "webconsole merge: $ARG_CTP/sql not found; skipping."
    return 0
  fi

  # Build/version label + a representative cubrid_rel from any shard's main.info.
  local ver="" rel="" os="linux" i src sched
  for (( i=0; i<NSHARDS; i++ )); do
    local mi; mi="$(find "$OUT/shard_${i}/CTP/sql/result" -name main.info 2>/dev/null | head -1)"
    if [ -n "$mi" ]; then
      ver="$(awk -F: '/^build:/{print $2; exit}' "$mi")"
      rel="$(grep -m1 '^cubrid_rel:' "$mi" | cut -d: -f2-)"
      os="$(awk -F: '/^os:/{print $2; exit}' "$mi")"
      break
    fi
  done
  [ -n "$ver" ] || ver="unknown"

  local y m stamp lbl lblsan
  lbl="${ARG_LABEL:-$(basename "$OUT")}"
  lblsan="$(printf '%s' "$lbl" | tr -c 'A-Za-z0-9._-' '_')"
  y="y$(date +%Y)"; m="m$(date +%-m)"; stamp="$(date +%s)"
  sched="schedule_${os}_sql_64bit_parallel_${stamp}_${lblsan}_${ver}"
  local dest="$resroot/$y/$m/$sched"
  mkdir -p "$dest/sql"

  # Union every shard's schedule sql/ subtree (carries each shard's failed-case
  # artifacts + summary files). Distinct failures live in distinct category leaves,
  # so the union is lossless for the failure view.
  local sum_succ=0 sum_fail=0 sum_total=0 sum_exec=0 sum_time=0
  for (( i=0; i<NSHARDS; i++ )); do
    src="$(find "$OUT/shard_${i}/CTP/sql/result" -type d -name 'schedule_*' 2>/dev/null | head -1)"
    [ -n "$src" ] || { warn "webconsole merge: shard $i has no schedule dir; skipping it."; continue; }
    [ -d "$src/sql" ] && cp -an "$src/sql/." "$dest/sql/" 2>/dev/null || :
    local mi="$src/main.info"
    if [ -r "$mi" ]; then
      sum_succ=$(( sum_succ + $(awk -F: '/^success:/{print $2+0; exit}' "$mi") ))
      sum_fail=$(( sum_fail + $(awk -F: '/^fail:/{print $2+0; exit}' "$mi") ))
      sum_total=$(( sum_total + $(awk -F: '/^total:/{print $2+0; exit}' "$mi") ))
      sum_exec=$(( sum_exec + $(awk -F: '/^execute_case:/{print $2+0; exit}' "$mi") ))
      sum_time=$(( sum_time + $(awk -F: '/^totalTime:/{print $2+0; exit}' "$mi") ))
    fi
  done

  # One summed main.info at the merged root -> appears as a single run in the list.
  {
    printf 'build:%s\n' "$ver"
    printf 'version:64bit\n'
    printf 'os:%s\n' "$os"
    printf 'category:sql\n'
    printf 'elapse_time:%d\n' "$sum_time"
    printf 'success:%d\n' "$sum_succ"
    printf 'fail:%d\n' "$sum_fail"
    printf 'total:%d\n' "$sum_total"
    printf 'execute_case:%d\n' "$sum_exec"
    printf 'totalTime:%d\n' "$sum_time"
    printf 'end_time:%s\n' "$(date '+%Y-%m-%d %H:%M:%S')"
    printf 'result_path:%s\n' "$dest"
    printf '%s\n' "${rel:+cubrid_rel:$rel}"
    printf 'user:\n'
    printf 'machine:ctp-parallel/%s(%d shards)\n' "$lbl" "$NSHARDS"
  } > "$dest/main.info"

  info "webconsole: merged $NSHARDS shards -> $dest (success=$sum_succ fail=$sum_fail total=$sum_total)"
  info "webconsole: view with  CTP_HOME=$ARG_CTP $ARG_CTP/bin/ctp.sh webconsole start  (then open http://<host>:8888 )"
}

#####################################################################
# Resolve the prebuilt locale lib + early-exit make_locale.sh to inject per shard
# (D6). Source priority: (1) --locale-dir, (2) a libcubrid_all_locales.so already
# in the build tree. The early-exit make_locale.sh is taken from --locale-dir if
# present, else the skill-bundled copy under locale/. The 18MB .so itself is NOT
# bundled in the skill; without it shards compile the locale themselves (slow).
#####################################################################
LOCALE_SO=""
LOCALE_SCRIPT=""
resolve_locale() {
  local bundled="$SELF_DIR/../locale/make_locale.sh"
  if [ -n "$ARG_LOCALE_DIR" ]; then
    [ -d "$ARG_LOCALE_DIR" ] || die "--locale-dir does not exist: $ARG_LOCALE_DIR"
    [ -r "$ARG_LOCALE_DIR/libcubrid_all_locales.so" ] \
      || die "--locale-dir has no libcubrid_all_locales.so: $ARG_LOCALE_DIR"
    LOCALE_SO="$(cd "$ARG_LOCALE_DIR" && pwd)/libcubrid_all_locales.so"
    if [ -r "$ARG_LOCALE_DIR/make_locale.sh" ]; then
      LOCALE_SCRIPT="$(cd "$ARG_LOCALE_DIR" && pwd)/make_locale.sh"
    else
      LOCALE_SCRIPT="$bundled"
    fi
  elif [ -r "$ARG_BUILD/lib/libcubrid_all_locales.so" ]; then
    LOCALE_SO="$ARG_BUILD/lib/libcubrid_all_locales.so"
    LOCALE_SCRIPT="$bundled"
  fi
  if [ -n "$LOCALE_SO" ]; then
    [ -r "$LOCALE_SCRIPT" ] || die "early-exit make_locale.sh missing: $LOCALE_SCRIPT"
    info "locale: injecting prebuilt $(basename "$LOCALE_SO") + early-exit make_locale.sh per shard (skips per-shard compile)."
  else
    info "locale: no prebuilt locale lib; shards will run make_locale (slow). Pass --locale-dir <dir with libcubrid_all_locales.so> to skip it."
  fi
}

#####################################################################
# Main
#####################################################################
WORK=""
OUT=""
cleanup() { [ -n "${WORK:-}" ] && rm -rf "$WORK" 2>/dev/null || :; }

main() {
  parse_args "$@"

  # Hidden self-test seam: validate a supplied split and exit.
  if [ "$ARG_VALIDATE_ONLY" -eq 1 ]; then
    WORK="$(mktemp -d)"
    trap cleanup EXIT
    ASSIGN_FILE="$VO_ASSIGN"
    SQL_LIST="$VO_SQL"
    SURVIVING_SQL="$(wc -l < "$SQL_LIST")"
    validate_split
    return $?
  fi

  # --merge-only: merge a finished run's out dir into webconsole and exit (no podman).
  if [ -n "$ARG_MERGE_ONLY" ]; then
    OUT="$(cd "$ARG_MERGE_ONLY" && pwd)"
    NSHARDS="$(find "$OUT" -maxdepth 1 -type d -name 'shard_*' 2>/dev/null | wc -l)"
    [ "$NSHARDS" -ge 1 ] || die "--merge-only: no shard_* dirs under $OUT"
    ARG_WEBCONSOLE=1
    info "merge-only: merging $NSHARDS shard result dir(s) from $OUT into $ARG_CTP/sql/result ..."
    merge_results
    return 0
  fi

  # podman preflight FIRST (real runs only) — before any work dirs exist.
  if [ "$ARG_DRYRUN" -eq 0 ]; then
    host_preflight
  fi

  choose_shards

  OUT="$ARG_OUT"
  mkdir -p "$OUT"
  OUT="$(cd "$OUT" && pwd)"
  WORK="$(mktemp -d "${OUT}/.work.XXXXXX")"
  trap cleanup EXIT

  resolve_colocate
  resolve_weights
  info "discovering units under $SCN ..."
  discover_units
  balance_units
  expand_shard_sets
  validate_split
  emit_plan
  print_plan_summary

  if [ "$ARG_DRYRUN" -eq 1 ]; then
    info "--dry-run: plan + validation complete; not launching containers."
    return 0
  fi

  setup_core_capture
  resolve_locale
  declare -ga SHARD_NAMES SHARD_RC
  local i
  for (( i=0; i<NSHARDS; i++ )); do build_shard_workdir "$i"; done
  for (( i=0; i<NSHARDS; i++ )); do launch_shard "$i"; done
  wait_shards
  collect_shards
  local agg_rc=0
  aggregate || agg_rc=$?
  merge_results || :
  return "$agg_rc"
}

main "$@"
