#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

label=${1:-sample}
before=${2:-}
after=${3:-}

metric_delta()
{
  local name=$1
  local pattern=$2
  local b=0 a=0
  [[ -n "${before}" && -s "${before}" ]] && b=$(extract_stat "${before}" "${pattern}" || echo 0)
  [[ -n "${after}" && -s "${after}" ]] && a=$(extract_stat "${after}" "${pattern}" || echo 0)
  awk -v a="${a:-0}" -v b="${b:-0}" 'BEGIN { print a - b }'
}

echo -e "label\tmetric\tvalue\tnote"
echo -e "${label}\tqps\t${HARNESS_QPS:-NA}\tcomputed by env runner when applicable"
echo -e "${label}\tp50_ms\t${HARNESS_P50_MS:-NA}\tcomputed by env runner when applicable"
echo -e "${label}\tp95_ms\t${HARNESS_P95_MS:-NA}\tcomputed by env runner when applicable"
echo -e "${label}\tPERF_PAGE_TEMP_delta\t$(metric_delta PERF_PAGE_TEMP 'PERF_PAGE_TEMP|temp.*page')\tfrom cubrid statdump if exposed"
echo -e "${label}\tLRU_victim_mutex_wait_delta\t$(metric_delta LRU_victim_mutex_wait 'victim.*mutex|lru.*wait')\tfrom cubrid statdump if exposed"
echo -e "${label}\tNum_data_page_victim_candidate_delta\t$(metric_delta Num_data_page_victim_candidate 'Num_data_page_victim_candidate')\tbuffer victim-candidate activity exposed by cubrid statdump"
echo -e "${label}\tdata_page_ioreads_delta\t$(metric_delta data_page_ioreads 'data.*ioread|page.*read')\tfrom cubrid statdump if exposed"
echo -e "${label}\tNum_workmem_degrades\t0\tplaceholder until P1a instrumentation exists"
echo -e "${label}\trawfd_overflow_bytes\tNA\tP1b/P2 placeholder"
echo -e "${label}\trawfd_overflow_files\tNA\tP1b/P2 placeholder"
echo -e "${label}\trawfd_fd_in_use\tNA\tP1b/P2 placeholder"
echo -e "${label}\trawfd_reaper_deletions\tNA\tP1b/P2 placeholder"
echo -e "${label}\trawfd_boot_sweep_counts\tNA\tP1b/P2 placeholder"
echo -e "${label}\trawfd_read_cache_hit\tNA\tP4 placeholder"
echo -e "${label}\trawfd_read_cache_miss\tNA\tP4 placeholder"
echo -e "${label}\trawfd_decrypt_cpu\tNA\tP4/TDE placeholder"
echo -e "${label}\tseparate_class_a_materialize\tNA\tP4/P5 placeholder"
echo -e "${label}\tseparate_class_a_prime_materialize\tNA\tP4/P5 placeholder"
echo -e "${label}\tseparate_class_b_materialize\tNA\tP4/P5 placeholder"
