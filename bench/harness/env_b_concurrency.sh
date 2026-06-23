#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

query=${HARNESS_QUERY:-"${HARNESS_DIR}/queries/B_concurrency_sort.sql"}
label=${1:-env_b_concurrency}
ks=${HARNESS_KS:-"64 128"}
echo -e "label\tk\tqps\tp50_ms\tp95_ms\trc_failures"
for k in ${ks}; do
  tmp="${RESULTS_DIR}/${label}.k${k}.$(date +%s)"
  mkdir -p "${tmp}"
  start=$(now_ns)
  for i in $(seq 1 "${k}"); do
    (run_timed_sql "${query}" "${tmp}/${i}.out" >"${tmp}/${i}.timing" 2>&1 || true) &
  done
  wait
  end=$(now_ns)
  wall_ms=$(elapsed_ms "${start}" "${end}")
  awk '{ print $1 }' "${tmp}"/*.timing | sort -n >"${tmp}/latencies"
  p50=$(awk '{ a[NR]=$1 } END { i=int((NR+1)*0.50); if (i<1)i=1; print a[i] }' "${tmp}/latencies")
  p95=$(awk '{ a[NR]=$1 } END { i=int((NR+1)*0.95); if (i<1)i=1; if (i>NR)i=NR; print a[i] }' "${tmp}/latencies")
  fails=$(awk '$2 != 0 { n++ } END { print n+0 }' "${tmp}"/*.timing)
  qps=$(awk -v k="${k}" -v ms="${wall_ms}" 'BEGIN { printf "%.6f", k / (ms / 1000.0) }')
  HARNESS_QPS="${qps}" HARNESS_P50_MS="${p50}" HARNESS_P95_MS="${p95}" "${HARNESS_DIR}/collect_metrics.sh" "${label}.k${k}" >"${RESULTS_DIR}/${label}.k${k}.metrics.tsv"
  echo -e "${label}\t${k}\t${qps}\t${p50}\t${p95}\t${fails}"
  rm -rf "${tmp}"
done
