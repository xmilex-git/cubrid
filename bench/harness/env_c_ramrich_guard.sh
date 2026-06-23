#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

query=${HARNESS_QUERY:-"${HARNESS_DIR}/queries/C_driver.sql"}
label=${1:-env_c_ramrich_guard}
before="${RESULTS_DIR}/${label}.before.stat"
after="${RESULTS_DIR}/${label}.after.stat"
concurrency=${HARNESS_C_CONCURRENCY:-8}
tmp="${RESULTS_DIR}/${label}.k${concurrency}.$(date +%s)"
mkdir -p "${tmp}"

# Env C is the RAM-rich regression guard: 16G data buffer, parallelism 8, and
# eight concurrent clients. run_3way.sh applies the config before restart; this
# runner proves the live values before measuring.
data_buffer=$(paramdump_value data_buffer_size || true)
parallelism=$(paramdump_value parallelism || true)
max_parallel_workers=$(paramdump_value max_parallel_workers || true)
echo "data_buffer_size=${data_buffer}" >&2
echo "parallelism=${parallelism}" >&2
echo "max_parallel_workers=${max_parallel_workers}" >&2
[[ "${data_buffer}" == 16.0G* || "${data_buffer}" == 16G* ]]
[[ "${parallelism}" == 8* ]]
[[ "${max_parallel_workers}" == 8* ]]

statdump_snapshot >"${before}"
start=$(now_ns)
for i in $(seq 1 "${concurrency}"); do
  (run_timed_sql "${query}" "${tmp}/${i}.out" >"${tmp}/${i}.timing" 2>&1 || true) &
done
wait
end=$(now_ns)
statdump_snapshot >"${after}"
wall_ms=$(elapsed_ms "${start}" "${end}")
failures=$(awk '$2 != 0 { n++ } END { print n+0 }' "${tmp}"/*.timing)
"${HARNESS_DIR}/collect_metrics.sh" "${label}" "${before}" "${after}" >"${RESULTS_DIR}/${label}.metrics.tsv"
echo -e "label\twall_ms\trc\tout_file"
echo -e "${label}\t${wall_ms}\t${failures}\t${tmp}"
[[ "${failures}" == 0 ]]
