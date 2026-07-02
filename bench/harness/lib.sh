#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${HARNESS_DIR}/../.." && pwd)
DB_NAME=${DB_NAME:-tpch_sf10}
DB_USER=${DB_USER:-dba}
DB_PATH=${CUBRID_DATABASES:-${HOME}/databases}
RESULTS_DIR=${RESULTS_DIR:-"${HARNESS_DIR}/results"}
BASELINES_DIR=${BASELINES_DIR:-"${HARNESS_DIR}/baselines"}
mkdir -p "${RESULTS_DIR}" "${BASELINES_DIR}"

# issue #106: whether the caller explicitly opted into a worktree build must
# be captured before the case below fills in a BUILD_NAME-derived default --
# otherwise every run "explicitly" sets BUILD_WORKTREE and the worktree
# _install is preferred unconditionally (the #91 stale-binary false FAIL).
BUILD_WORKTREE_EXPLICIT=${BUILD_WORKTREE+x}

case "${BUILD_NAME:-redesign}" in
  baseline) BUILD_WORKTREE=${BUILD_WORKTREE:-"${HOME}/dev/cubrid-wm-baseline"} ;;
  asbuilt) BUILD_WORKTREE=${BUILD_WORKTREE:-"${HOME}/dev/cubrid-wm-asbuilt"} ;;
  redesign) BUILD_WORKTREE=${BUILD_WORKTREE:-"${HOME}/dev/cubrid-workmem"} ;;
  *) BUILD_WORKTREE=${BUILD_WORKTREE:-"${REPO_ROOT}"} ;;
esac

SERVER_CTL=${SERVER_CTL:-"${BUILD_WORKTREE}/.agents/skills/cubrid-server-control/scripts/cubrid-server-ctl.sh"}
if [[ ! -x "${SERVER_CTL}" ]]; then
  SERVER_CTL="${REPO_ROOT}/.agents/skills/cubrid-server-control/scripts/cubrid-server-ctl.sh"
fi
if [[ ! -x "${SERVER_CTL}" ]]; then
  # The agent tooling (incl. this skill) was extracted out of the CUBRID
  # checkout into a standalone repo (workspace commit b981c3b) and no longer
  # ships inside BUILD_WORKTREE/REPO_ROOT. Fall back to the sibling tooling repo.
  SERVER_CTL="${CUBRID_TOOLING_REPO:-${HOME}/dev/workspace}/.agents/skills/cubrid-server-control/scripts/cubrid-server-ctl.sh"
fi

export CUBRID_DATABASES="${DB_PATH}"
# issue #106: the active install ($CUBRID, default ~/CUBRID) wins by default.
# A worktree's build_x86_64_release/_install is only used when the caller
# explicitly opted in via BUILD_WORKTREE=... -- an unset BUILD_WORKTREE must
# never silently pick up a stale worktree binary over the just-built active
# one.
if [[ -n "${BUILD_WORKTREE_EXPLICIT}" && -x "${BUILD_WORKTREE}/build_x86_64_release/_install/CUBRID/bin/csql" ]]; then
  export CUBRID="${BUILD_WORKTREE}/build_x86_64_release/_install/CUBRID"
else
  export CUBRID=${CUBRID:-"${HOME}/CUBRID"}
fi
export PATH="${CUBRID}/bin:${PATH}"

# issue #106: record which binary this run actually resolved to, and warn
# (never hard-fail -- #98 is running this harness concurrently elsewhere and
# must not be broken by a false staleness read) if it looks older than the
# source tree. CUBRID_REL is exported so callers (e.g. parity.sh's proof)
# can stamp the exact binary identity they tested against.
CUBRID_REL=$(cubrid_rel 2>/dev/null | grep -m1 .) || CUBRID_REL="<cubrid_rel unavailable>"
export CUBRID_REL
LIB_BUILD_SH="${HARNESS_DIR}/lib_build.sh"
if [[ -f "${LIB_BUILD_SH}" ]]; then
  # shellcheck source=lib_build.sh
  source "${LIB_BUILD_SH}"
  if declare -f binary_is_stale_at >/dev/null; then
    binary_is_stale_at "${CUBRID}" >&2 || true
  fi
fi

csql_cmd()
{
  csql -u "${DB_USER}" "${DB_NAME}" "$@" </dev/null
}

csql_file()
{
  local sql_file=$1
  csql_cmd -i "${sql_file}"
}

server_ctl()
{
  "${SERVER_CTL}" "$@" "${DB_NAME}"
}

restart_server()
{
  server_ctl restart
}

ensure_harness_server_conf()
{
  local conf="${CUBRID}/conf/cubrid.conf"
  local target_max_clients=${HARNESS_MAX_CLIENTS:-200}
  [[ -f "${conf}" && -w "${conf}" ]] || return 0

  local current
  current=$(awk -F= '$1 == "max_clients" { gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2; exit }' "${conf}")
  if [[ -n "${current}" && "${current}" =~ ^[0-9]+$ && "${current}" -ge "${target_max_clients}" ]]; then
    return 0
  fi

  local tmp="${conf}.harness.$$"
  awk -v target="${target_max_clients}" '
    BEGIN { done = 0 }
    $1 == "max_clients" && $0 !~ /^[ \t]*#/ { print "max_clients=" target; done = 1; next }
    { print }
    END { if (!done) print "max_clients=" target }
  ' "${conf}" >"${tmp}"
  mv "${tmp}" "${conf}"
}

set_server_conf_param()
{
  local key=$1
  local value=$2
  local conf="${CUBRID}/conf/cubrid.conf"
  if [[ ! -f "${conf}" ]]; then
    echo "set_server_conf_param: conf file not found: ${conf} (cannot set ${key}=${value})" >&2
    return 1
  fi
  if [[ ! -w "${conf}" ]]; then
    echo "set_server_conf_param: conf file not writable: ${conf} (cannot set ${key}=${value})" >&2
    return 1
  fi

  local tmp="${conf}.harness.$$"
  awk -F= -v key="${key}" -v value="${value}" '
    BEGIN { done = 0 }
    $1 == key && $0 !~ /^[ \t]*#/ { print key "=" value; done = 1; next }
    { print }
    END { if (!done) print key "=" value }
  ' "${conf}" >"${tmp}"
  mv "${tmp}" "${conf}"
}

configure_env_server_conf()
{
  local env_name=$1
  ensure_harness_server_conf

  case "${env_name}" in
    a)
      set_server_conf_param data_buffer_size "${HARNESS_A_DATA_BUFFER_SIZE:-512M}"
      set_server_conf_param parallelism "${HARNESS_A_PARALLELISM:-8}"
      set_server_conf_param max_parallel_workers "${HARNESS_A_MAX_PARALLEL_WORKERS:-8}"
      ;;
    b)
      set_server_conf_param data_buffer_size "${HARNESS_B_DATA_BUFFER_SIZE:-16G}"
      set_server_conf_param parallelism "${HARNESS_B_PARALLELISM:-8}"
      set_server_conf_param max_parallel_workers "${HARNESS_B_MAX_PARALLEL_WORKERS:-8}"
      ;;
    c)
      set_server_conf_param data_buffer_size "${HARNESS_C_DATA_BUFFER_SIZE:-16G}"
      set_server_conf_param parallelism "${HARNESS_C_PARALLELISM:-8}"
      set_server_conf_param max_parallel_workers "${HARNESS_C_MAX_PARALLEL_WORKERS:-8}"
      ;;
    positioned)
      set_server_conf_param data_buffer_size "${HARNESS_POSITIONED_DATA_BUFFER_SIZE:-16G}"
      set_server_conf_param parallelism "${HARNESS_POSITIONED_PARALLELISM:-8}"
      set_server_conf_param max_parallel_workers "${HARNESS_POSITIONED_MAX_PARALLEL_WORKERS:-8}"
      ;;
  esac
}

now_ns()
{
  date +%s%N
}

elapsed_ms()
{
  local start_ns=$1
  local end_ns=$2
  awk -v s="${start_ns}" -v e="${end_ns}" 'BEGIN { printf "%.3f", (e - s) / 1000000.0 }'
}

run_timed_sql()
{
  local sql_file=$1
  local out_file=${2:-/dev/null}
  local start end rc
  start=$(now_ns)
  if csql_file "${sql_file}" >"${out_file}" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  end=$(now_ns)
  printf "%s\t%s\t%s\n" "$(elapsed_ms "${start}" "${end}")" "${rc}" "${out_file}"
  return "${rc}"
}

median()
{
  awk '{ a[NR]=$1 } END { if (NR == 0) exit 1; asort(a); mid=int((NR+1)/2); if (NR % 2) print a[mid]; else printf "%.6f\n", (a[mid]+a[mid+1])/2 }'
}

mean_sigma2()
{
  awk '{ n++; x[NR]=$1; s+=$1 } END { if (n == 0) exit 1; mean=s/n; for (i=1;i<=n;i++){ d=x[i]-mean; ss+=d*d } sigma=(n>1?sqrt(ss/(n-1)):0); printf "%.6f\t%.6f\t%.6f\t%.6f\n", mean, sigma, mean - 2*sigma, mean + 2*sigma }'
}

paramdump_value()
{
  local key=$1
  cubrid paramdump "${DB_NAME}" </dev/null 2>/dev/null | awk -F= -v k="${key}" '$1 ~ k { gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2; exit }'
}

statdump_snapshot()
{
  cubrid statdump "${DB_NAME}" </dev/null 2>/dev/null || true
}

extract_stat()
{
  local stat_file=$1
  local pattern=$2
  awk -v p="${pattern}" 'tolower($0) ~ tolower(p) { for (i=NF;i>=1;i--) if ($i ~ /^-?[0-9]+([.][0-9]+)?$/) { print $i; exit } }' "${stat_file}"
}
