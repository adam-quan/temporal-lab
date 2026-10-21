# Temporal Internals Lab (Python SDK)

A self-hosted Temporal cluster you can hurt, plus the Python workers and load generator to hurt it with.
Every Temporal service is its own container; Postgres sits behind a fault-injection proxy; Prometheus + Grafana scrape
the server **and** the SDK workers **and** the load generator. The experiments are in `experiments/EXPERIMENTS.md`.

## Status: read this first

I could not run Docker or install the Temporal SDK in the environment where this was written. What that means:

| Verified here | Not verified (you will be the first to run it) |
|---|---|
| All Python compiles; YAML/JSON configs parse; the server config template renders to a valid config | `docker compose up` end to end |
| The **load generator** (open-loop scheduler, multi-process aggregation, percentiles, error labelling, lag warning, JSONL output) ran against a stub client | Anything talking to a real Temporal server or the real `temporalio` package |
| Dashboard JSON: 34 panels, no overlaps; Makefile and `chaos.sh` parse and their error paths work | Metric/dynamic-config names on *your* versions; shard hash (`probe shard`) vs the server |
| Image env vars and entrypoint were checked against the upstream v1.32.0 config template | Docker Hub tags in `.env`, the `temporal-sql-tool` / `temporal operator ...` flags |

So: **run `make up && make smoke` first.** `smoke` starts one of every workflow type against the real SDK and server and
prints PASS/FAIL per case. Expect to fix a small number of first-run issues; see Troubleshooting.

## Quickstart

```sh
make up          # build the Python image, start postgres, temporal (4 services), UI, prometheus, grafana, ...
make smoke       # 12 checks. All PASS = the lab works
make urls
make load ARGS="ramp --shape noop --rates 10,50,100 --step-seconds 30"
python3 tools/curve.py results/*.jsonl        # throughput/latency table with the knee marked
```
Needs Docker with Compose v2 and ~4 GB of free RAM. Stop with `make down`; wipe everything with `make nuke`.
The host ports below are the defaults; change them in `.env` (`*_HOST_PORT*`) if another local Temporal stack uses them.

| What | URL |
|---|---|
| Temporal UI (namespace `lab`) | http://localhost:8080 |
| Grafana (dashboard "Temporal Lab", anonymous admin) | http://localhost:3000 |
| Prometheus (targets, alerts) | http://localhost:9090 |
| Toxiproxy API | http://localhost:8474 |
| Frontend gRPC | localhost:7233 (7233-7243 when scaled) |

## Architecture

```
 loadgen ──────► frontend ──► history ◄──► matching ◄────── sdk-worker (xN, python)
 (lab.load)          │           │  ▲           │             polls workflow + activity tasks
                     │           │  └─ internal-worker
                     └───────────┴───────┬───────┘
                                         ▼
                                    toxiproxy ──► postgres   (temporal + temporal_visibility DBs)
                                     :8474 API     (SQL visibility: no Elasticsearch)

 prometheus scrapes:  server services :8000 (DNS service discovery, so --scale works)
                      sdk workers :9000    loadgen :9001-9004    cadvisor    postgres-exporter
 chaos/chaos.sh -> docker (kill/pause/cpu/mem/network) + toxiproxy API (DB latency/blackhole/cut) + Grafana annotations
```

## Layout

```
docker-compose.yml   the whole lab              .env                 versions + worker tuning knobs
Makefile             ergonomic commands         chaos/chaos.sh       fault injection
config/temporal/     server config template     config/dynamicconfig hot-reloaded server tunables (5 s poll)
config/prometheus/   scrape config + SLO alerts config/grafana/      provisioning + generated dashboard
lab/                 Python: models, activities, workflows, worker, load, probe, hotshard / workerperf / historytasks / persistenceqps / shardio (tests)
tools/               gen_dashboard.py, curve.py, grafana_shots.py (panel screenshots), shardio.sh
experiments/         EXPERIMENTS.md, atlas.md, hot-shard.md, worker-performance.md, history-task-processing.md, history-persistence-qps.md, shard-io-concurrency.md
results/             ramp output (*.jsonl) and chaos.log
```

## The Python side

**Workflow shapes** (`python -m lab.load ramp --shape X`; all parameters overridable with `--params '{json}'`):

| Shape | Workflow | Stresses | Useful params |
|---|---|---|---|
| `noop` | start -> complete | start API, shard write rate | `--payload-bytes` |
| `chain` | N sequential activities | matching, dispatch, history growth. Also slow / hanging / flaky / heartbeating / big-result activities | `n`, `spec.*`, `call.*` |
| `fanout` | N parallel activities (failures counted, not raised) | pending-activity limit, WFT size, replay cost, retry storms | `n`, `max_parallel` |
| `timer` | sleep random(min,max) s | timer queue, fire bursts | `min_s`, `max_s` |
| `signal` | signal-with-start to `--keys` IDs | signal contention; **`--keys 1` = hot key** | `--keys`, `--payload-bytes` |
| `longlived` | activity + timer loop | history growth, continue-as-new | `iterations`, `can_mode` never/suggested/every |
| `parent` | N children | cross-shard traffic, child start rate | `n`, `max_parallel`, `parent_close_policy` |
| `anatomy` | activity, timer, signal wait, activity (+query, update) | Phase 1 dissection; prints a replay probe | `timer_s` |
| `poison` | raise / raise_after_activity / cpu / fail | workflow-task failure loops, deadlock detector | `mode` |
| `evolving` | A,B,wait,C with code variants v1/v2/v3 | non-determinism and patching | `make deploy-variant V=...` |

`spec` = what an activity does (`work_ms`, `cpu_ms`, `heartbeat_every_ms`, `response_bytes`, `fail_rate`, `fail_first_attempts`).
`call` = how it is invoked (`activity` work_async|work_sync, timeouts, retry policy, `task_queue`). See `lab/models.py`.

**Load generator honesty.** It is open-loop: operation k of a step is scheduled at `start + k/rate` regardless of server speed,
and latencies are measured from that intended time (no coordinated omission). `lag p99` in the table is the generator's own
scheduling delay: if it exceeds ~250 ms the generator (or `--max-inflight`) is the bottleneck, not Temporal. Use `--processes N`.
The Temporal SDK retries some errors (e.g. `RESOURCE_EXHAUSTED`) internally, so rate limiting first shows up as *latency*.

**Worker knobs** (set in `.env` or your shell, then `make workers`; empty = SDK default):

| Env (.env name) | Worker argument | What it changes | Watch |
|---|---|---|---|
| `MAX_CONCURRENT_ACTIVITIES` (`W_MAX_CONCURRENT_ACTIVITIES`) | `max_concurrent_activities` | activity slots | `slots_available`, activity schedule->start |
| `MAX_CONCURRENT_WORKFLOW_TASKS` | `max_concurrent_workflow_tasks` | workflow task slots | same, workflow |
| `*_TASK_POLLS` | `max_concurrent_*_task_polls` | pollers | `num_pollers`, empty polls |
| `POLLER_AUTOSCALE=1` | `PollerBehaviorAutoscaling` | pollers adapt (server >= 1.28) | `num_pollers` |
| `TUNER=resource` | `WorkerTuner.create_resource_based` | slots follow CPU/mem targets (mutually exclusive with `MAX_CONCURRENT_*`) | `slots_used` (not `available`) |
| `MAX_CACHED_WORKFLOWS` | `max_cached_workflows` | sticky cache size | cache hit/miss, replay latency |
| `ACTIVITY_THREADS` | `ThreadPoolExecutor(max_workers)` | sync-activity concurrency (must be >= max activities) | CPU, slots |
| `STICKY_QUEUE_TIMEOUT_S` | `sticky_queue_schedule_to_start_timeout` | failover delay after a worker dies | WFT schedule->start |
| `MAX_ACTIVITIES_PER_SECOND`, `MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND` | same | dispatch rate limits | activity schedule->start |
| `DEBUG_MODE=1` | `debug_mode` | disables the SDK deadlock detector | WFT timeouts |
| `EVOLVING_VARIANT` | (code path) | which `EvolvingWorkflow` version this worker runs | `WorkflowTaskFailed` |

Two worker layouts: one combined fleet (`sdk-worker`, queue `lab-tq`) or split fleets (`make split`: `sdk-worker-wf` on `lab-wf`,
`sdk-worker-act` on `lab-act`; then add `--task-queue lab-wf --activity-queue lab-act` to load commands).

## Chaos

`chaos/chaos.sh help` lists everything: kill / kill-one / stop / pause / rolling-restart / scale / cpu / mem / partition, and DB faults
through toxiproxy (latency, bandwidth, blackhole, cut, reset-peer, heal). Every action is timestamped in `results/chaos.log` and
drawn as a red marker on every Grafana panel.

## Design decisions worth knowing

* **SQL visibility, no Elasticsearch**: lighter, and enough for `LabRunId` filtering and counts. Visibility is eventually consistent; `--count-after-s` waits before counting.
* **Everything goes through toxiproxy**, so DB faults are live, and the postgres-exporter connects directly so it keeps working during them.
* **`numHistoryShards` defaults to 4** so shard effects appear quickly. It is fixed at first boot: `make reset SHARDS=512` rebuilds.
* **Server containers compute their own IP at start** (`hostname -i`) for the membership ring, which is what lets `--scale history=3` work.
* **Restart policy is `no`** so *you* decide when a crashed service comes back (`RESTART_POLICY=on-failure` in `.env` mimics an orchestrator).
* **Workflows are started by type name with dict params**; the worker's data converter builds the dataclasses. That keeps the generator import-free and every experiment a JSON change.
* **Histogram units differ**: SDK latencies are milliseconds, server latencies are seconds. Panel titles say which.

## Troubleshooting (likely first-run issues)

| Symptom | Look at / try |
|---|---|
| `manifest unknown` / pull fails | Image tags in `.env` may not exist (I could not verify them). Pick existing tags; keep `TEMPORAL_VERSION` equal for server and admin-tools. |
| `db-init` fails | `docker compose logs db-init`. Tool flags or the `SQL_PASSWORD` env var may differ by version: `docker compose run --rm db-init temporal-sql-tool --help`. |
| `ns-init` loops | `docker compose logs ns-init`. CLI syntax may differ: `docker compose run --rm ns-init temporal operator namespace create --help`. Or create namespace `lab` in the UI, then register search attributes `LabRunId` and `LabShape` (Keyword), or run loads with `--no-search-attrs`. |
| A server service exits at start | `docker compose logs history`. Look for config template errors or membership/bind errors (`hostname -i` output). |
| `Worker()` raises `TypeError: unexpected keyword` | Your `temporalio` version lacks that option; delete the corresponding env knob in `lab/worker.py` (`docker compose run --rm --entrypoint pip loadgen show temporalio`). |
| compose rejects `7233-7243:7233` | Change the frontend port mapping to `"7233:7233"` (you then cannot scale frontend on the host). |
| Empty Grafana panels | `make metric-names PATTERN=<part>` and fix the query in `tools/gen_dashboard.py`, `make dashboard`. Server metric names vary by release. The container panels need cAdvisor, which often cannot see containers on Docker Desktop (macOS/Windows): use `docker stats` there. |
| Dynamic config edit does nothing | Wrong key for your version (look for warnings in `docker compose logs frontend history matching`; check `common/dynamicconfig/constants.go`). |
| `probe shard` disagrees with `tdbg` | The hash assumption (`farm.Fingerprint32(nsID + "_" + wfID) % shards + 1`) is wrong for your version, or `pyfarmhash` differs from Go's farm. Trust `tdbg`. |
| Generator prints `lag p99` warnings | It is saturated: `--processes N`, raise `--max-inflight`, or lower the rate. |
