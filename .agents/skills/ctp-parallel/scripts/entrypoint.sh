#!/bin/sh
#
# entrypoint.sh — runs INSIDE each shard container.
#
# The orchestrator (ctp_parallel.sh) mounts the four per-shard working copies to
# these fixed container paths, then this entrypoint sources the build, runs an
# in-container preflight (with the D5 relocation guard), and execs the real CTP.
#
# Every shard reuses the SAME ports/SHM IDs from sql.conf; isolation is by podman
# namespaces (net + IPC + mount), so there is nothing per-shard to configure here.
#
# Copyright (c) 2024 CUBRID test-infra. Apache-2.0.

set -eu

export CUBRID=/home/CUBRID
export CTP_HOME=/home/CTP
export CUBRID_DATABASES=/home/CUBRID_DB
export TZ=Asia/Seoul
export LC_ALL=en_US

# JAVA_HOME is required by ctp.sh (F1); derive it from `java` if the image left it unset.
if [ -z "${JAVA_HOME:-}" ]; then
  if command -v java >/dev/null 2>&1; then
    JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
    export JAVA_HOME
  else
    echo "PREFLIGHT: JAVA_HOME unset and no java on PATH" >&2
    exit 10
  fi
fi

# Set the CUBRID runtime env directly. This build-tree install ships no
# .cubrid.sh, and sourcing one would bake the original (host) build path; since
# the build is mounted at /home/CUBRID we point everything at the mount instead.
PATH="$CTP_HOME/bin:$CUBRID/bin:$JAVA_HOME/bin:$PATH"
LD_LIBRARY_PATH="$CUBRID/lib:$CUBRID/cci/lib:$JAVA_HOME/jre/lib/amd64:$JAVA_HOME/jre/lib/amd64/server:${LD_LIBRARY_PATH:-}"
# CTP loads the CUBRID JDBC driver at runtime from the inherited CLASSPATH
# (sql/bin/run.sh runs ConsoleAgent with -classpath "$CLASSPATH:...", and the
# driver jar is NOT bundled in $CTP_HOME/sql/lib). On a host this is set by the
# user's shell profile; here we set it explicitly because we deliberately do not
# source .cubrid.sh. Without it, ConsoleDAO throws ClassNotFoundException
# (cubrid.sql.CUBRIDOID) and CTP runs 0 cases (silent empty pass).
CLASSPATH=".:${CLASSPATH:-}"
for _j in "$CUBRID"/jdbc/*.jar; do
  [ -r "$_j" ] && CLASSPATH="$_j:$CLASSPATH"
done
export PATH LD_LIBRARY_PATH CLASSPATH

command -v cubrid >/dev/null 2>&1 || { echo "PREFLIGHT: cubrid not on PATH" >&2; exit 13; }
case "$(command -v cubrid)" in
  "$CUBRID"/bin/*) : ;;
  *) echo "PREFLIGHT: cubrid resolves to '$(command -v cubrid)', not \$CUBRID/bin" >&2; exit 13 ;;
esac

[ -w "$CUBRID/conf" ]      || { echo "PREFLIGHT: $CUBRID/conf not writable" >&2; exit 14; }
[ -w "$CUBRID_DATABASES" ] || { echo "PREFLIGHT: $CUBRID_DATABASES not writable" >&2; exit 14; }
locale -a 2>/dev/null | grep -qi 'en_US' || { echo "PREFLIGHT: en_US locale missing in image" >&2; exit 15; }
[ -d /home/cubrid-testcases/sql ] || { echo "PREFLIGHT: scenario /home/cubrid-testcases/sql missing" >&2; exit 16; }
[ -r "$CTP_HOME/conf/sql.conf" ] && [ -r "$CTP_HOME/conf/exclusions.txt" ] \
  || { echo "PREFLIGHT: $CTP_HOME/conf/{sql.conf,exclusions.txt} missing" >&2; exit 17; }
# CTP compiles its JDBC helper classes with `javac -cp $CUBRID/jdbc/cubrid_jdbc.jar`
# (sql/bin/run.sh). A build made without the cubrid-jdbc submodule (or with a
# build-only target that skips JDBC packaging) ships no jdbc jar, so javac fails
# with 32 "cannot find symbol" errors and CTP silently runs 0 cases. Fail loudly.
[ -r "$CUBRID/jdbc/cubrid_jdbc.jar" ] \
  || { echo "PREFLIGHT: $CUBRID/jdbc/cubrid_jdbc.jar missing — the --build tree has no JDBC driver. Init the submodule (git submodule update --init cubrid-jdbc) and build a full install (e.g. build.sh with jdbc packaging), or copy cubrid_jdbc.jar into \$CUBRID/jdbc/." >&2; exit 18; }

# Allow core dumps for crash diagnosis (best effort).
ulimit -c unlimited 2>/dev/null || :

echo "PREFLIGHT OK: CUBRID=$CUBRID CTP_HOME=$CTP_HOME JAVA_HOME=$JAVA_HOME shard scenario ready."
exec ctp.sh sql -c "$CTP_HOME/conf/sql.conf"
