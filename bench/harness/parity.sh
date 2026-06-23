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
    cat "${query}"
  } >"${output}"
}

prove_param()
{
  local label=$1
  {
    echo "===== ${label} ====="
    echo "data_buffer_size=$(paramdump_value data_buffer_size || true)"
    echo "parallelism=$(paramdump_value parallelism || true)"
    echo "max_parallel_workers=$(paramdump_value max_parallel_workers || true)"
  } | tee -a "${proof}"
}

result_rows()
{
  awk '
    /^=== <Result of SELECT Command/ { in_result=1; next }
    in_result && /^[[:space:]]*$/ { next }
    in_result && /^=+/ { next }
    in_result && /^[[:space:]]*[0-9]+ rows selected/ { in_result=0; next }
    in_result && /selected\.|Committed\.|Execute OK\./ { next }
    in_result && /^  / { print }
  ' "$1"
}

: >"${proof}"
serial_sql="${RESULTS_DIR}/parity.serial.sql"
parallel_sql="${RESULTS_DIR}/parity.parallel.sql"
write_query_sql "${serial_sql}"
write_query_sql "${parallel_sql}"

set_server_conf_param parallelism 1
set_server_conf_param max_parallel_workers 1
restart_server
prove_param serial
csql_file "${serial_sql}" >"${serial}" 2>&1

set_server_conf_param parallelism 8
set_server_conf_param max_parallel_workers 8
restart_server
prove_param parallel
csql_file "${parallel_sql}" >"${parallel}" 2>&1

result_rows "${serial}" | sort >"${serial_sorted}"
result_rows "${parallel}" | sort >"${parallel_sorted}"
serial_md5=$(md5sum "${serial_sorted}" | awk '{ print $1 }')
parallel_md5=$(md5sum "${parallel_sorted}" | awk '{ print $1 }')
echo "serial_md5=${serial_md5}" | tee -a "${proof}"
echo "parallel_md5=${parallel_md5}" | tee -a "${proof}"
[[ "${serial_md5}" == "${parallel_md5}" ]]
