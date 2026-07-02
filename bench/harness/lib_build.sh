#!/usr/bin/env bash
# lib_build.sh - build-freshness helpers for the Tapeset/BufFile harness
# (redesign G003, issue #68; written for issue #93's gate/checklist slice).
#
# Read-only: like cubrid-deps-check, this never invokes a build itself -- it
# only reports whether the installed binary looks stale against the source
# tree and prints the exact command to fix it (the cubrid-build skill's
# `just build <mode>`, which lives in the sibling tooling repo, not here).
set -euo pipefail

HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${HARNESS_DIR}/../.." && pwd)

# binary_is_stale <mode: debug|release> [source_file ...]
# Prints a warning and returns 1 if the installed cub_server predates any of
# the given source files (defaults to the qfile_buffile/qfile_tape/
# query_manager sources this issue's counters live in); returns 0 if fresh or
# if the binary can't be found (nothing to compare against).
binary_is_stale()
{
  local mode=$1
  shift
  local sources=("$@")
  if [[ ${#sources[@]} -eq 0 ]]; then
    sources=(
      "${REPO_ROOT}/src/query/qfile_buffile.cpp"
      "${REPO_ROOT}/src/query/qfile_buffile.hpp"
      "${REPO_ROOT}/src/query/qfile_tape.cpp"
      "${REPO_ROOT}/src/query/qfile_tape.hpp"
      "${REPO_ROOT}/src/query/query_manager.c"
      "${REPO_ROOT}/src/storage/page_buffer.c"
      "${REPO_ROOT}/src/storage/page_buffer.h"
    )
  fi

  local install_root="${HOME}/${mode}/CUBRID-11.5.develop"
  local server_bin="${install_root}/bin/cub_server"
  if [[ ! -x "${server_bin}" ]]; then
    echo "lib_build: no ${mode} cub_server installed at ${server_bin} -- nothing to compare" >&2
    return 0
  fi

  local src stale=0
  for src in "${sources[@]}"; do
    [[ -f "${src}" ]] || continue
    if [[ "${src}" -nt "${server_bin}" ]]; then
      echo "lib_build: ${src} is newer than ${server_bin}" >&2
      stale=1
    fi
  done

  if [[ "${stale}" -ne 0 ]]; then
    echo "lib_build: rebuild with: WORKSPACE=${REPO_ROOT} just build ${mode}  (cubrid-build skill, run from the tooling repo)" >&2
    return 1
  fi
  return 0
}
