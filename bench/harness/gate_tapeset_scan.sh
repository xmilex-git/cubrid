#!/usr/bin/env bash
# gate_tapeset_scan.sh - in-server judgment for the BUFFILE/HELDTAPE/TAPEREAD/
# PRODUCER selftests (redesign G003, issue #68; gating wired by issue #93).
#
# Before issue #93, qmgr_initialize ran these 4 selftests but discarded their
# return codes (query_manager.c, "SELFTEST result=%d" logged and forgotten) --
# a FAIL could never be detected short of reading server stderr by hand. This
# script restarts the harness DB with all 4 selftests enabled on the SERVER
# process's environment (per house rule: csql client env has no effect) and
# greps the resulting er_log for a `result=0` PASS line per selftest and for
# the absence of the `SELFTEST FAIL` marker qmgr_initialize now emits.
#
# Usage: DB_NAME=wmg003 ./gate_tapeset_scan.sh   (defaults to lib.sh's DB_NAME)
set -euo pipefail

HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "${HARNESS_DIR}/lib.sh"

# The 4 selftests only exist behind `#if !defined (NDEBUG)`; against a
# release install the env vars below would be silently ignored and this gate
# would report a false PASS (nothing ran). Force the debug install
# regardless of what lib.sh's release-oriented CUBRID resolution picked.
export CUBRID="${HARNESS_CUBRID_DEBUG:-${HOME}/debug/CUBRID-11.5.develop}"
export PATH="${CUBRID}/bin:${PATH}"
if [[ ! -x "${CUBRID}/bin/cub_server" ]]; then
  echo "FAIL: no debug cub_server at ${CUBRID} -- build one first (see preflight.sh)" >&2
  exit 1
fi

echo "gate_tapeset_scan: preflight..."
if ! "${HARNESS_DIR}/preflight.sh"; then
  echo "FAIL: preflight check reported a [MISS] -- see above" >&2
  exit 1
fi

SELFTESTS=(BUFFILE HELDTAPE TAPEREAD PRODUCER)

latest_er_log()
{
  local latest="${CUBRID}/log/server/${DB_NAME}_latest.err"
  if [[ -e "${latest}" ]]; then
    echo "${latest}"
    return 0
  fi
  # fall back to the newest timestamped log if _latest.err doesn't exist yet
  # on this CUBRID build.
  find "${CUBRID}/log/server" -maxdepth 1 -name "${DB_NAME}_*.err" -printf '%T@ %p\n' 2>/dev/null \
    | sort -rn | head -1 | cut -d' ' -f2-
}

echo "gate_tapeset_scan: restarting ${DB_NAME} with BUFFILE/HELDTAPE/TAPEREAD/PRODUCER selftests enabled..."
for t in "${SELFTESTS[@]}"; do
  export "CUBRID_${t}_SELFTEST=1"
done
restart_server
for t in "${SELFTESTS[@]}"; do
  unset "CUBRID_${t}_SELFTEST"
done

LOG=$(latest_er_log)
if [[ -z "${LOG}" || ! -f "${LOG}" ]]; then
  echo "FAIL: could not find a server er_log for ${DB_NAME} under ${CUBRID}/log/server" >&2
  exit 1
fi
echo "gate_tapeset_scan: judging ${LOG}"

fail=0
for t in "${SELFTESTS[@]}"; do
  if ! grep -q "${t}_SELFTEST result=0 " "${LOG}"; then
    echo "FAIL: ${t}_SELFTEST did not report result=0 in ${LOG}" >&2
    fail=1
  fi
  if grep -q "${t}_SELFTEST FAIL" "${LOG}"; then
    echo "FAIL: ${t}_SELFTEST FAIL marker present in ${LOG}" >&2
    fail=1
  fi
done

if [[ "${fail}" -ne 0 ]]; then
  exit 1
fi
echo "PASS: BUFFILE/HELDTAPE/TAPEREAD/PRODUCER selftests all result=0 in ${LOG}"
