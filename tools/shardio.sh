#!/bin/bash
# Host side of the shard IO concurrency test (lab/shardio.py runs inside the loadgen container).
#
#   tools/shardio.sh run --scenario issue [--rate 1000 ...]   apply the scenario, run it, put everything back
#   tools/shardio.sh list | readings | reset                   passed straight to lab/shardio.py
#
# history.shardIOConcurrency is read when history takes ownership of a shard, so a new value needs a history
# restart. That, the SQL pool size (maxConns) and the Postgres CPU cap are things the loadgen container cannot do,
# so this script does them around the run:
#   1. write history.shardIOConcurrency into a marked block of config/dynamicconfig/docker.yaml (lab.shardio prepare)
#   2. recreate history with SQL_MAX_CONNS (pool size) and, if the scenario says so, cap Postgres' CPU
#   3. run the test
#   4. always: remove the block, uncap Postgres, recreate history with the default pool (back to the lab default)
set -euo pipefail
cd "$(dirname "$0")/.."
DC="docker compose"
PY=($DC run --rm --no-deps -T --entrypoint python loadgen -m lab.shardio)

if [ "${1:-}" != "run" ]; then
  exec "${PY[@]}" "$@"
fi
shift
scenario=""
args=("$@")
while [ $# -gt 0 ]; do
  case "$1" in --scenario) scenario="$2"; shift 2 ;; --scenario=*) scenario="${1#*=}"; shift ;; *) shift ;; esac
done
[ -n "$scenario" ] || { echo "usage: tools/shardio.sh run --scenario <name>   (tools/shardio.sh list)"; exit 2; }

eval "$("${PY[@]}" env --scenario "$scenario" | tail -1)"     # SIO DB_CPUS MAX_CONNS

restore() {
  echo "--- restoring the lab: dynamic-config block removed, Postgres uncapped, history back to maxConns 20"
  "${PY[@]}" reset >/dev/null 2>&1 || true
  chaos/chaos.sh cpu postgres 0 >/dev/null 2>&1 || true
  $DC up -d --force-recreate --no-deps history >/dev/null 2>&1 || true
}
trap restore EXIT

echo "--- scenario $scenario: history.shardIOConcurrency=$SIO, maxConns=$MAX_CONNS, Postgres CPU cap=${DB_CPUS}"
"${PY[@]}" prepare --scenario "$scenario"
echo "--- recreating history so every shard picks up the new value (playbook Step 6)"
SQL_MAX_CONNS="$MAX_CONNS" $DC up -d --force-recreate --no-deps history
if [ "$DB_CPUS" != "0" ]; then
  chaos/chaos.sh cpu postgres "$DB_CPUS"
fi
$DC run --rm --no-deps --name loadgen --entrypoint python loadgen -m lab.shardio run "${args[@]}"
