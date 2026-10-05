#!/usr/bin/env bash
# Fault injection for the Temporal lab.
#
#   chaos/chaos.sh <command> [args]      (run from anywhere; it cd's to the project root)
#
# Every action is (1) printed, (2) appended to results/chaos.log with a timestamp, and
# (3) posted to Grafana as an annotation tagged "chaos" -> red vertical markers on every
# dashboard panel. Use that log as the timeline column of your Failure Atlas.
#
# PROCESS FAULTS (service = frontend | history | matching | internal-worker |
#                 sdk-worker | sdk-worker-wf | sdk-worker-act | postgres | toxiproxy)
#   kill <svc> [restart_after_s]      SIGKILL every replica (a crash: no graceful drain)
#   kill-one <svc> [restart_after_s]  SIGKILL just the first replica
#   stop <svc> [restart_after_s]      SIGTERM (graceful shutdown, 30s grace)
#   start <svc>                       start stopped containers
#   pause <svc> [secs]                freeze all processes (cgroup freezer): a hung process /
#                                     long GC pause. Sockets stay open but nothing answers.
#   unpause <svc>
#   rolling-restart <svc> [wait_s]    restart replicas one by one (default wait 15s)
#   scale <svc> <n>                   docker compose up -d --scale <svc>=<n>
#
# RESOURCE FAULTS
#   cpu <svc> <cpus>                  cap CPU (e.g. 0.5); "0" removes the cap
#   mem <svc> <MB>                    cap memory (may OOM-kill it: that's the experiment)
#
# NETWORK FAULTS
#   partition <svc> [secs]            disconnect from the lab network, reconnect later with the
#                                     SAME IP. Use for sdk-worker*/loadgen. For server services
#                                     prefer `pause` (they advertise their IP on the ring).
#
# DATABASE FAULTS (via toxiproxy, live, no restarts)
#   db-latency <ms> [jitter_ms]       add latency to every DB response
#   db-bandwidth <KB/s>               throttle DB response bandwidth
#   db-blackhole [up|down|both]       swallow data, keep connections open (default: down =
#                                     writes may commit but the client never hears back)
#   db-cut                            refuse/kill all DB connections (proxy disabled)
#   db-reset-peer                     reset connections mid-stream (TCP RST)
#   db-heal                           remove all toxics, re-enable the proxy
#   db-pause [secs]                   freeze the postgres container
#   db-kill [restart_after_s]         SIGKILL postgres (data survives: it's on a volume)
#
#   status                            containers, toxics, live CPU/mem
set -euo pipefail
cd "$(dirname "$0")/.."

TOXI=${TOXIPROXY_URL:-http://localhost:8474}
GRAFANA=${GRAFANA_URL:-http://localhost:3000}
NET=${LAB_NETWORK:-lab_default}
LOG=results/chaos.log
mkdir -p results

note() {  # note "text"  -> stdout + log + grafana annotation
  local ts; ts=$(date '+%F %T')
  echo "[$ts] $*"
  echo "$ts | $*" >> "$LOG"
  curl -fsS -m 2 -X POST "$GRAFANA/api/annotations" -H 'Content-Type: application/json' \
    -d "{\"text\":\"$*\",\"tags\":[\"chaos\"]}" >/dev/null 2>&1 || true
}

ids() {  # running container ids of a compose service
  local out; out=$(docker compose ps -q "$1" 2>/dev/null || true)
  [ -n "$out" ] || { echo "no running containers for service '$1'" >&2; exit 1; }
  echo "$out"
}
ids_all() { docker compose ps -a -q "$1" 2>/dev/null; }

countdown() { local s=$1; while [ "$s" -gt 0 ]; do printf '\r  ... %ss ' "$s"; sleep 1; s=$((s-1)); done; printf '\r              \r'; }

toxic_set() {  # toxic_set <name> <json-body>   (idempotent: replaces an existing toxic of that name)
  curl -fsS -X DELETE "$TOXI/proxies/postgres/toxics/$1" >/dev/null 2>&1 || true
  curl -fsS -X POST "$TOXI/proxies/postgres/toxics" -H 'Content-Type: application/json' -d "$2" >/dev/null \
    || { echo "toxiproxy not reachable at $TOXI (is it up?)" >&2; exit 1; }
}

cmd=${1:-help}; shift || true
case "$cmd" in
  kill|kill-one)
    svc=${1:?service}; after=${2:-}
    targets=$(ids "$svc"); [ "$cmd" = kill-one ] && targets=$(echo "$targets" | head -1)
    note "SIGKILL $svc ($(echo "$targets" | wc -w) container(s))"
    docker kill -s KILL $targets >/dev/null
    if [ -n "$after" ]; then countdown "$after"; docker start $targets >/dev/null; note "restarted $svc after ${after}s"; fi ;;
  stop)
    svc=${1:?service}; after=${2:-}; targets=$(ids "$svc")
    note "SIGTERM (graceful) $svc"
    docker stop -t 30 $targets >/dev/null
    if [ -n "$after" ]; then countdown "$after"; docker start $targets >/dev/null; note "restarted $svc after ${after}s"; fi ;;
  start)
    svc=${1:?service}; note "start $svc"; docker start $(ids_all "$svc") >/dev/null ;;
  pause)
    svc=${1:?service}; secs=${2:-}; targets=$(ids "$svc")
    note "PAUSE (freeze) $svc${secs:+ for ${secs}s}"
    docker pause $targets >/dev/null
    if [ -n "$secs" ]; then countdown "$secs"; docker unpause $targets >/dev/null; note "UNPAUSE $svc"; fi ;;
  unpause)
    svc=${1:?service}; note "UNPAUSE $svc"; docker unpause $(ids "$svc") >/dev/null ;;
  rolling-restart)
    svc=${1:?service}; wait_s=${2:-15}
    for c in $(ids "$svc"); do
      note "rolling restart: $svc $(docker inspect -f '{{.Name}}' "$c")"
      docker restart -t 30 "$c" >/dev/null
      countdown "$wait_s"
    done ;;
  scale)
    svc=${1:?service}; n=${2:?replicas}; note "scale $svc=$n"
    docker compose up -d --no-recreate --scale "$svc=$n" "$svc" ;;
  cpu)
    svc=${1:?service}; cpus=${2:?cpus}; note "CPU cap $svc = $cpus"
    docker update --cpus "$cpus" $(ids "$svc") >/dev/null ;;
  mem)
    svc=${1:?service}; mb=${2:?MB}; note "MEM cap $svc = ${mb}MB"
    docker update --memory "${mb}m" --memory-swap "${mb}m" $(ids "$svc") >/dev/null ;;
  partition)
    svc=${1:?service}; secs=${2:-}
    for c in $(ids "$svc"); do
      ip=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$c")
      note "PARTITION $svc $(docker inspect -f '{{.Name}}' "$c") (ip $ip) disconnected from $NET"
      docker network disconnect "$NET" "$c"
      if [ -n "$secs" ]; then
        countdown "$secs"
        docker network connect --alias "$svc" --ip "$ip" "$NET" "$c"
        note "HEAL partition $svc reconnected with ip $ip"
      else
        echo "  reconnect later with: docker network connect --alias $svc --ip $ip $NET $c"
      fi
    done ;;
  db-latency)
    ms=${1:?latency ms}; jitter=${2:-0}; note "DB latency +${ms}ms (jitter ${jitter}ms)"
    toxic_set lat "{\"name\":\"lat\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$ms,\"jitter\":$jitter}}" ;;
  db-bandwidth)
    kb=${1:?KB/s}; note "DB bandwidth limited to ${kb}KB/s"
    toxic_set bw "{\"name\":\"bw\",\"type\":\"bandwidth\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"rate\":$kb}}" ;;
  db-blackhole)
    dir=${1:-down}; note "DB blackhole ($dir): connections stay open, data is swallowed"
    if [ "$dir" = down ] || [ "$dir" = both ]; then
      toxic_set hole_down '{"name":"hole_down","type":"timeout","stream":"downstream","toxicity":1.0,"attributes":{"timeout":0}}'; fi
    if [ "$dir" = up ] || [ "$dir" = both ]; then
      toxic_set hole_up '{"name":"hole_up","type":"timeout","stream":"upstream","toxicity":1.0,"attributes":{"timeout":0}}'; fi ;;
  db-cut)
    note "DB CUT: proxy disabled, all connections dropped"
    curl -fsS -X POST "$TOXI/proxies/postgres" -H 'Content-Type: application/json' -d '{"enabled":false}' >/dev/null ;;
  db-reset-peer)
    note "DB reset_peer: TCP RST on every connection"
    toxic_set rst '{"name":"rst","type":"reset_peer","stream":"downstream","toxicity":1.0,"attributes":{"timeout":0}}' ;;
  db-heal)
    note "DB HEAL: toxics removed, proxy enabled"
    for t in lat bw hole_down hole_up rst; do curl -fsS -X DELETE "$TOXI/proxies/postgres/toxics/$t" >/dev/null 2>&1 || true; done
    curl -fsS -X POST "$TOXI/proxies/postgres" -H 'Content-Type: application/json' -d '{"enabled":true}' >/dev/null ;;
  db-pause)
    secs=${1:-}; note "DB PAUSE (freeze postgres)${secs:+ for ${secs}s}"
    docker pause $(ids postgres) >/dev/null
    if [ -n "$secs" ]; then countdown "$secs"; docker unpause $(ids postgres) >/dev/null; note "DB UNPAUSE"; fi ;;
  db-kill)
    after=${1:-}; note "DB SIGKILL postgres"
    docker kill -s KILL $(ids postgres) >/dev/null
    if [ -n "$after" ]; then countdown "$after"; docker start $(ids_all postgres) >/dev/null; note "DB restarted after ${after}s"; fi ;;
  status)
    docker compose ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}\t{{.Ports}}' | cut -c1-140
    echo; echo "toxiproxy:"; curl -fsS "$TOXI/proxies/postgres" 2>/dev/null || echo "  (unreachable)"
    echo; docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' 2>/dev/null | grep -E 'NAME|^lab-' || true ;;
  help|-h|--help|"")
    sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//' ;;
  *)
    echo "unknown command '$cmd' (try: $0 help)" >&2; exit 2 ;;
esac
