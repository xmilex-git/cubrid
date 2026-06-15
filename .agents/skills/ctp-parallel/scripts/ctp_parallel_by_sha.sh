#!/usr/bin/env bash
#
# ctp_parallel_by_sha.sh — run the parallel CTP SQL suite for a SPECIFIC pair of
# commits: a CUBRID engine SHA and a cubrid-testcases SHA. Reproducible "give me
# two SHAs -> N-shard parallel result" entry point.
#
# It is a thin wrapper around ctp_parallel.sh:
#   1. HARD-GATE preflight: refuses to start unless podman, git, the repos, the
#      CTP tree, and the build toolchain are all present.
#   2. testcases SHA -> a detached git worktree (cheap).
#   3. cubrid SHA    -> built with the repo's own build.sh (NOT justfile/presets,
#      which only exist on xmilex_base-derived branches) into a per-SHA install
#      dir (cached: same SHA is not rebuilt). ~/CUBRID is never touched.
#   4. hands the built dir + checked-out scenario to ctp_parallel.sh.
#
# Copyright (c) 2024 CUBRID test-infra. Apache-2.0.

set -euo pipefail

SELF="$(basename "$0")"
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
ORCH="$SELF_DIR/ctp_parallel.sh"

# Disk-backed cache/worktree area (NEVER /tmp). Override with --workdir.
WORKROOT="${HOME}/.ctp-parallel"

# Defaults
CUBRID_REPO="${HOME}/dev/cubrid"
TC_REPO="${HOME}/cubrid-testcases"
CTP_DIR="${HOME}/cubrid-testtools/CTP"
MODE="release"
CUBRID_SHA=""
TC_SHA=""
OUT=""
declare -a PASSTHRU=()

err() { printf '[by-sha] ERROR: %s\n' "$*" >&2; }
info(){ printf '[by-sha] %s\n' "$*"; }
die() { err "$*"; exit 1; }

usage() {
  cat <<EOF
$SELF — run the parallel CTP SQL suite for a specific (cubrid SHA, testcases SHA).

Usage:
  $SELF --cubrid-sha <sha> --testcases-sha <sha> [options] [-- <ctp_parallel.sh args>]

Required:
  --cubrid-sha <sha>       Commit in the CUBRID engine repo to build & test.
  --testcases-sha <sha>    Commit in the cubrid-testcases repo to run.

Options:
  --cubrid-repo <path>     CUBRID engine git repo.    [default: $CUBRID_REPO]
  --testcases-repo <path>  cubrid-testcases git repo. [default: $TC_REPO]
  --ctp <path>             CTP tool dir.              [default: $CTP_DIR]
  --mode release|debug     build.sh mode.             [default: $MODE]
  --workdir <path>         Build cache + worktrees.   [default: $WORKROOT]
  --out <path>             Passed through to ctp_parallel.sh --out.
  -h, --help               This help.

Any args after '--' (or any unrecognized --flag) are passed through to
ctp_parallel.sh (e.g. --shards 10, --weights <file>, --keep, --by-category).

Example:
  $SELF --cubrid-sha 9f75499 --testcases-sha 97073ae --shards 10 --weights w.tsv
EOF
}

#####################################################################
# Arg parsing — known flags consumed here; everything else -> PASSTHRU.
#####################################################################
while [ $# -gt 0 ]; do
  case "$1" in
    --cubrid-sha)      CUBRID_SHA="${2:-}"; shift 2 ;;
    --testcases-sha)   TC_SHA="${2:-}"; shift 2 ;;
    --cubrid-repo)     CUBRID_REPO="${2:-}"; shift 2 ;;
    --testcases-repo)  TC_REPO="${2:-}"; shift 2 ;;
    --ctp)             CTP_DIR="${2:-}"; shift 2 ;;
    --mode)            MODE="${2:-}"; shift 2 ;;
    --workdir)         WORKROOT="${2:-}"; shift 2 ;;
    --out)             OUT="${2:-}"; shift 2 ;;
    -h|--help)         usage; exit 0 ;;
    --)                shift; while [ $# -gt 0 ]; do PASSTHRU+=("$1"); shift; done ;;
    *)                 PASSTHRU+=("$1"); shift ;;
  esac
done

[ -n "$CUBRID_SHA" ] || { usage; die "--cubrid-sha is required"; }
[ -n "$TC_SHA" ]     || { usage; die "--testcases-sha is required"; }
case "$MODE" in release|debug) ;; *) die "--mode must be release or debug" ;; esac

#####################################################################
# HARD-GATE preflight. Collect every problem, print a checklist, and refuse to
# do ANY work (no worktrees, no build) unless everything passes.
#####################################################################
preflight() {
  local ok=1
  pass(){ printf '  [ OK ] %s\n' "$1"; }
  bad(){  printf '  [FAIL] %s\n' "$1" >&2; ok=0; }

  info "preflight checks (all must pass to proceed):"

  # container runtime
  if command -v podman >/dev/null 2>&1; then pass "podman present ($(command -v podman))"
  else bad "podman not installed — this tool runs one rootless container per shard"; fi

  # git
  command -v git >/dev/null 2>&1 && pass "git present" || bad "git not installed"

  # build toolchain (build.sh needs these; no justfile/cmake-presets assumed)
  command -v cmake >/dev/null 2>&1 && pass "cmake present ($(cmake --version 2>/dev/null | head -1))" \
    || bad "cmake not installed (build.sh needs it)"
  if command -v ninja >/dev/null 2>&1 || command -v make >/dev/null 2>&1; then
    pass "build generator present ($(command -v ninja || command -v make))"
  else bad "neither ninja nor make found"; fi
  if command -v cc >/dev/null 2>&1 || command -v gcc >/dev/null 2>&1; then
    pass "C/C++ compiler present"
  else bad "no C compiler (cc/gcc) found"; fi
  if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then pass "JAVA_HOME ok ($JAVA_HOME)"
  elif command -v java >/dev/null 2>&1; then pass "java present (JAVA_HOME unset; will derive)"
  else bad "no JAVA_HOME and no java on PATH (build.sh + PL engine need a JDK)"; fi

  # orchestrator
  [ -x "$ORCH" ] && pass "orchestrator present ($ORCH)" || bad "orchestrator missing/not executable: $ORCH"

  # CUBRID engine repo + build.sh
  if git -C "$CUBRID_REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    pass "cubrid repo is a git work tree ($CUBRID_REPO)"
    [ -x "$CUBRID_REPO/build.sh" ] && pass "cubrid build.sh present" || bad "cubrid build.sh missing in $CUBRID_REPO"
  else bad "cubrid repo not found / not a git work tree: $CUBRID_REPO"; fi

  # testcases repo
  if git -C "$TC_REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    pass "testcases repo is a git work tree ($TC_REPO)"
  else bad "testcases repo not found / not a git work tree: $TC_REPO"; fi

  # CTP tree
  [ -r "$CTP_DIR/conf/sql.conf" ] && pass "CTP tree present ($CTP_DIR)" \
    || bad "CTP tree missing or no conf/sql.conf: $CTP_DIR"

  [ "$ok" -eq 1 ] || { err "preflight FAILED — install/locate the missing items above and retry."; exit 2; }
  info "preflight passed."
}

# Verify a SHA exists in a repo; try a fetch once if not.
resolve_sha() {  # resolve_sha <repo> <sha> <label>
  local repo="$1" sha="$2" label="$3"
  if git -C "$repo" cat-file -e "${sha}^{commit}" 2>/dev/null; then return 0; fi
  info "$label SHA $sha not present in $repo; fetching..."
  git -C "$repo" fetch --all --tags --quiet 2>/dev/null || :
  git -C "$repo" cat-file -e "${sha}^{commit}" 2>/dev/null \
    || die "$label SHA '$sha' not found in $repo (even after fetch)."
}

#####################################################################
# Main
#####################################################################
preflight

resolve_sha "$CUBRID_REPO" "$CUBRID_SHA" "cubrid"
resolve_sha "$TC_REPO"     "$TC_SHA"     "testcases"
# canonical short ids for cache/worktree naming
CUBRID_ID="$(git -C "$CUBRID_REPO" rev-parse --short=12 "${CUBRID_SHA}^{commit}")"
TC_ID="$(git -C "$TC_REPO" rev-parse --short=12 "${TC_SHA}^{commit}")"

mkdir -p "$WORKROOT/builds" "$WORKROOT/wt"
INSTALL_DIR="$WORKROOT/builds/${MODE}/CUBRID-${CUBRID_ID}"
TC_WT="$WORKROOT/wt/tc-${TC_ID}"
CUBRID_WT="$WORKROOT/wt/cubrid-${CUBRID_ID}"

cleanup() {
  # keep the cubrid INSTALL (cache); drop the throwaway worktrees.
  [ -n "${TC_WT:-}" ]     && git -C "$TC_REPO"     worktree remove --force "$TC_WT" 2>/dev/null || :
  [ -n "${CUBRID_WT:-}" ] && git -C "$CUBRID_REPO" worktree remove --force "$CUBRID_WT" 2>/dev/null || :
}
trap cleanup EXIT

# 1) testcases worktree at the requested SHA (cheap).
info "checking out testcases @ $TC_ID -> $TC_WT"
git -C "$TC_REPO" worktree remove --force "$TC_WT" 2>/dev/null || :
git -C "$TC_REPO" worktree add --detach --quiet "$TC_WT" "$TC_SHA"
[ -d "$TC_WT/sql" ] || die "testcases @ $TC_ID has no sql/ dir (is this the right repo/SHA?)"

# 2) cubrid build at the requested SHA, cached by SHA. Built with build.sh.
if [ -x "$INSTALL_DIR/bin/cubrid" ]; then
  info "cubrid @ $CUBRID_ID already built (cache hit): $INSTALL_DIR"
else
  info "building cubrid @ $CUBRID_ID ($MODE) via build.sh -> $INSTALL_DIR (this can take a while)"
  rm -rf "$INSTALL_DIR"; mkdir -p "$INSTALL_DIR"
  git -C "$CUBRID_REPO" worktree remove --force "$CUBRID_WT" 2>/dev/null || :
  git -C "$CUBRID_REPO" worktree add --detach --quiet "$CUBRID_WT" "$CUBRID_SHA"
  local_java=()
  [ -n "${JAVA_HOME:-}" ] && local_java=(-j "$JAVA_HOME")
  ( cd "$CUBRID_WT" && ./build.sh -m "$MODE" -p "$INSTALL_DIR" "${local_java[@]}" build ) \
    || die "build.sh failed for cubrid @ $CUBRID_ID"
  [ -x "$INSTALL_DIR/bin/cubrid" ] || die "build produced no $INSTALL_DIR/bin/cubrid"
  info "build complete: $INSTALL_DIR"
fi

# 3) hand off to the orchestrator.
declare -a args=( --build "$INSTALL_DIR" --testcases "$TC_WT" --ctp "$CTP_DIR" )
[ -n "$OUT" ] && args+=( --out "$OUT" )
args+=( "${PASSTHRU[@]}" )
info "running: ctp_parallel.sh ${args[*]}"
# The orchestrator exits non-zero on test failures/cores/crashes — that is a valid
# result to propagate, not a wrapper error, so don't let set -e abort on it.
rc=0
"$ORCH" "${args[@]}" || rc=$?
info "done (cubrid=$CUBRID_ID testcases=$TC_ID mode=$MODE rc=$rc)"
exit "$rc"
