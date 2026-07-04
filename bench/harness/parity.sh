#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

query_arg=${1:?usage: parity.sh <query.sql>}
if [[ -f "${query_arg}" ]]; then
  query="${query_arg}"
elif [[ -f "${HARNESS_DIR}/${query_arg}" ]]; then
  query="${HARNESS_DIR}/${query_arg}"
else
  query="${HARNESS_DIR}/queries/${query_arg}"
fi

serial="${RESULTS_DIR}/parity.serial.$(basename "${query}").out"
parallel="${RESULTS_DIR}/parity.parallel.$(basename "${query}").out"
serial_sorted="${RESULTS_DIR}/parity.serial.$(basename "${query}").sorted"
parallel_sorted="${RESULTS_DIR}/parity.parallel.$(basename "${query}").sorted"
proof="${RESULTS_DIR}/parity.$(basename "${query}").proof.txt"

write_query_sql()
{
  local output=$1
  {
    echo ";plan detail"
    echo ";trace on"
    cat "${query}"
  } >"${output}"
}

prove_param()
{
  local label=$1
  {
    echo "===== ${label} ====="
    echo "cubrid_rel=${CUBRID_REL:-<unknown>}"
    echo "data_buffer_size=$(paramdump_value data_buffer_size || true)"
    echo "parallelism=$(paramdump_value parallelism || true)"
    echo "max_parallel_workers=$(paramdump_value max_parallel_workers || true)"
    echo "CUBRID_WM_SCAN_NEW=${CUBRID_WM_SCAN_NEW:-<unset>}"
    echo "CUBRID_WM_SORT_NEW=${CUBRID_WM_SORT_NEW:-<unset>}"
    echo "CUBRID_WM_HASHJOIN_NEW=${CUBRID_WM_HASHJOIN_NEW:-<unset>}"
  } | tee -a "${proof}"
}

# WM-gate opt-in (redesign #78/#92): a caller who wants the NEW-engagement
# counter assert must explicitly export one of these three (any value,
# including "0") -- an unset var means "don't care", so the guard stays
# a no-op for callers who only want the classic serial==parallel md5 check.
wm_gate_requested()
{
  [[ -n "${CUBRID_WM_SCAN_NEW+x}${CUBRID_WM_SORT_NEW+x}${CUBRID_WM_HASHJOIN_NEW+x}" ]]
}

wm_gate_engaged()
{
  local var val
  for var in CUBRID_WM_SCAN_NEW CUBRID_WM_SORT_NEW CUBRID_WM_HASHJOIN_NEW; do
    val="${!var:-}"
    [[ "${val:0:1}" == "1" ]] && return 0
  done
  return 1
}

# NEW-engagement anti-tautology guard (evidence: harness eval §4, review
# report issue69-81 recommendation ①): a gate that is merely *requested* to be
# ON proves nothing by itself (a rejected gate silently falls back to OLD).
# Prove the NEW path actually ran via the backing-kind census counters
# (PSTAT_QF_NEW_BACKED_CREATE / PSTAT_QF_OLD_TOUCH_ON_NEW, list_file.c) —
# require a delta across the leg's query, not just a nonzero absolute value.
check_wm_engagement()
{
  local label=$1
  local before=$2
  local after=$3
  local new_before new_after old_before old_after new_delta old_delta

  new_before=$(extract_stat "${before}" "Num_qfile_new_backed_create")
  new_after=$(extract_stat "${after}" "Num_qfile_new_backed_create")
  old_before=$(extract_stat "${before}" "Num_qfile_old_touch_on_new")
  old_after=$(extract_stat "${after}" "Num_qfile_old_touch_on_new")
  new_delta=$(( ${new_after:-0} - ${new_before:-0} ))
  old_delta=$(( ${old_after:-0} - ${old_before:-0} ))
  echo "${label}: new_backed_create_delta=${new_delta} old_touch_on_new_delta=${old_delta}" | tee -a "${proof}"

  if [[ "${old_delta}" -ne 0 ]]; then
    echo "PARITY GUARD FAIL (${label}): OLD mechanism touched a NEW-backed list (old_touch_on_new_delta=${old_delta})" | tee -a "${proof}"
    exit 1
  fi

  wm_gate_requested || return 0
  if wm_gate_engaged; then
    if [[ "${new_delta}" -le 0 ]]; then
      echo "PARITY GUARD FAIL (${label}): WM gate requested ON but no NEW-backed list was created (new_backed_create_delta=${new_delta})" | tee -a "${proof}"
      exit 1
    fi
  fi
}

result_rows()
{
  awk '
    /^=== <Result of SELECT Command/ { in_result=1; next }
    in_result && /^=== Auto Trace ===/ { in_result=0; next }
    in_result && /^[[:space:]]*$/ { next }
    in_result && /^=+/ { next }
    in_result && /^[[:space:]]*[0-9]+ rows? selected/ { in_result=0; next }
    in_result && /selected\.|Committed\.|Execute OK\./ { next }
    in_result && /^  / { print }
  ' "$1"
}

: >"${proof}"
serial_sql="${RESULTS_DIR}/parity.serial.sql"
parallel_sql="${RESULTS_DIR}/parity.parallel.sql"
write_query_sql "${serial_sql}"
write_query_sql "${parallel_sql}"

serial_stat_before="${RESULTS_DIR}/parity.$(basename "${query}").serial.statdump.before.txt"
serial_stat_after="${RESULTS_DIR}/parity.$(basename "${query}").serial.statdump.after.txt"
parallel_stat_before="${RESULTS_DIR}/parity.$(basename "${query}").parallel.statdump.before.txt"
parallel_stat_after="${RESULTS_DIR}/parity.$(basename "${query}").parallel.statdump.after.txt"

set_server_conf_param parallelism 1
set_server_conf_param max_parallel_workers 1
restart_server
prove_param serial
statdump_snapshot >"${serial_stat_before}"
csql_file "${serial_sql}" >"${serial}" 2>&1
statdump_snapshot >"${serial_stat_after}"

# Bidirectional guard (CONFIRMED item 2/3): the serial leg must not itself
# engage parallel workers, else serial==parallel degenerates to
# serial==serial and proves nothing.
if grep -qE 'parallel workers: ([1-9]|[0-9]{2,})' "${serial}"; then
  echo "PARITY GUARD FAIL: serial run engaged parallel workers (contaminated baseline)" | tee -a "${proof}"
  echo "  inspect ;trace on output: ${serial}" | tee -a "${proof}"
  exit 1
fi
check_wm_engagement serial "${serial_stat_before}" "${serial_stat_after}"

set_server_conf_param parallelism 8
set_server_conf_param max_parallel_workers 8
restart_server
prove_param parallel
statdump_snapshot >"${parallel_stat_before}"
csql_file "${parallel_sql}" >"${parallel}" 2>&1
statdump_snapshot >"${parallel_stat_after}"

# Passthrough-tautology guard (evidence I-2/I-11): the parallel run MUST actually
# engage parallel workers (>=2), else serial==parallel is trivially serial==serial
# and proves nothing. Parallelism shows ONLY in ;trace on, never in ;plan detail.
# NOTE (residual limitation, out of scope per issue #92 판정): this only proves
# reservation-time engagement (xasl->executed_parallelism); it cannot by itself
# distinguish a per-operator serial fallback from an operator that never
# attempted parallelism (query_dump.c's curr/gstats/ostats->parallel_num lines
# print only when >0, so a fallback and a not-attempted operator both print
# nothing). The check_wm_engagement() counters below cover the NEW-backing
# half of "did it really run"; the fallback-vs-not-attempted trace ambiguity
# remains open (would need a `;plan detail` cross-check, not built here).
if ! grep -qE 'parallel workers: ([2-9]|[0-9]{2,})' "${parallel}"; then
  echo "PARITY GUARD FAIL: parallel run engaged NO parallel workers (>=2)" | tee -a "${proof}"
  echo "  query shape (e.g. aggregate-wrapped DISTINCT) may have serialized the operator;" | tee -a "${proof}"
  echo "  inspect ;trace on output: ${parallel}" | tee -a "${proof}"
  exit 1
fi
echo "parallel_engaged=$(grep -oE 'parallel workers: [0-9]+' "${parallel}" | sort -u | paste -sd, -)" | tee -a "${proof}"
check_wm_engagement parallel "${parallel_stat_before}" "${parallel_stat_after}"

result_rows "${serial}" | sort >"${serial_sorted}"
result_rows "${parallel}" | sort >"${parallel_sorted}"
serial_md5=$(md5sum "${serial_sorted}" | awk '{ print $1 }')
parallel_md5=$(md5sum "${parallel_sorted}" | awk '{ print $1 }')
echo "serial_md5=${serial_md5}" | tee -a "${proof}"
echo "parallel_md5=${parallel_md5}" | tee -a "${proof}"
[[ "${serial_md5}" == "${parallel_md5}" ]]
