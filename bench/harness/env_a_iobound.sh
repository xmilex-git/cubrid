#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

query=${HARNESS_QUERY:-"${HARNESS_DIR}/queries/A_parallel_hj.sql"}
label=${1:-env_a_iobound}
out="${RESULTS_DIR}/${label}.$(date +%s).out"
before="${RESULTS_DIR}/${label}.before.stat"
after="${RESULTS_DIR}/${label}.after.stat"

# Env A uses a small CUBRID data buffer rather than cgroup mutation: data_buffer_size=512M
# is the stock small-buffer mechanism available in this tree and forces temp/data pressure on SF10.
echo "data_buffer_size=$(paramdump_value data_buffer_size || true)" >&2
statdump_snapshot >"${before}"
timing=$(run_timed_sql "${query}" "${out}")
statdump_snapshot >"${after}"
"${HARNESS_DIR}/collect_metrics.sh" "${label}" "${before}" "${after}" >"${RESULTS_DIR}/${label}.metrics.tsv"
echo -e "label\twall_ms\trc\tout_file"
echo -e "${label}\t${timing}"
