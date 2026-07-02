#!/usr/bin/env bash
# preflight.sh - read-only environment check before running gate_tapeset_scan.sh
# (redesign G003, issue #68; written for issue #93's gate/checklist slice).
#
# Mirrors cubrid-deps-check: prints [OK]/[MISS]/[WARN] and echoes fix
# suggestions, but never mutates anything and never runs its own suggestions.
set -uo pipefail

HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "${HARNESS_DIR}/lib.sh"
# shellcheck source=lib_build.sh
source "${HARNESS_DIR}/lib_build.sh"

# gate_tapeset_scan.sh forces the debug install (the 4 selftests are
# `#if !defined (NDEBUG)`-only, so a release CUBRID would silently no-op);
# check the same one here rather than lib.sh's release-oriented resolution.
export CUBRID="${HARNESS_CUBRID_DEBUG:-${HOME}/debug/CUBRID-11.5.develop}"

status=0

check_ok()   { echo "[OK]   $1"; }
check_miss() { echo "[MISS] $1"; status=1; }
check_warn() { echo "[WARN] $1"; }

# 1. cub_server present for the mode this run will use.
MODE=${HARNESS_MODE:-debug}
if [[ -x "${HOME}/${MODE}/CUBRID-11.5.develop/bin/cub_server" ]]; then
  check_ok "${MODE} cub_server installed (${HOME}/${MODE}/CUBRID-11.5.develop)"
  stale_msg=$(binary_is_stale "${MODE}" 2>&1 >/dev/null) || check_warn "$(tr '\n' ' ' <<<"${stale_msg}")"
else
  check_miss "${MODE} cub_server not installed -- run: WORKSPACE=${REPO_ROOT} just build ${MODE} (cubrid-build skill)"
fi

# 2. stored_procedure=no (PL server boot workaround; see issue #93 runtime
#    verification note and the tooling repo's `just conf` campaign recipe).
CONF="${CUBRID}/conf/cubrid.conf"
if [[ -f "${CONF}" ]] && grep -qE '^[[:space:]]*stored_procedure[[:space:]]*=[[:space:]]*no' "${CONF}"; then
  check_ok "stored_procedure=no in ${CONF} (PL server boot workaround applied)"
else
  check_warn "stored_procedure=no not found in ${CONF} -- if server boot fails with 'PL server can not be started', apply: WORKSPACE=${REPO_ROOT} just conf (tooling repo, commit c3e8a58) or add it to this DB's conf section directly"
fi

# 3. CUBRID_DATABASES / DB registered.
if [[ -n "${CUBRID_DATABASES:-}" && -f "${CUBRID_DATABASES}/databases.txt" ]]; then
  if grep -q "^${DB_NAME}[[:space:]]" "${CUBRID_DATABASES}/databases.txt"; then
    check_ok "${DB_NAME} registered in ${CUBRID_DATABASES}/databases.txt"
  else
    check_miss "${DB_NAME} not found in ${CUBRID_DATABASES}/databases.txt"
  fi
else
  check_miss "CUBRID_DATABASES unset or databases.txt missing"
fi

# 4. server-control wrapper present (never call raw `cubrid server` -- it
#    hangs when its output is piped).
if [[ -x "${SERVER_CTL}" ]]; then
  check_ok "cubrid-server-control wrapper present (${SERVER_CTL})"
else
  check_miss "cubrid-server-control wrapper not found at ${SERVER_CTL}"
fi

exit "${status}"
