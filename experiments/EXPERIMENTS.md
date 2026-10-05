# Experiments: the plan, as commands

Numbering follows the original plan (Phase 0-5, chaos experiments #1-#18). For every experiment:
**write your prediction in `atlas.md` first**, run it, then fill in the rest. Dashboard: http://localhost:3000.

```sh
# shell helpers used throughout (paste once per terminal)
alias lg='docker compose run --rm --name loadgen loadgen'       # load generator:  lg ramp ... | lg smoke
alias t='docker compose exec admin-tools temporal'              # temporal CLI (namespace lab preset)
alias ch='chaos/chaos.sh'                                       # fault injection; also logs + annotates Grafana
```

Workflow IDs are `<run-id>-<shape>-<process>-<seq>`, so with `--run-id foo` the first workflow of the `noop` shape is `foo-noop-0-0`.
`make history WF=<id>` prints the annotated event history. `make count RUN=<run-id>` shows how a run ended.

**Reset between experiments** (worker knobs back to defaults, faults healed, stragglers gone):
`ch db-heal; make workers; make unsplit; make cleanup RUN=<id>`.

---------------------------------------------------------------------------------------------------

## Phase 0: is the lab alive?

```sh
make up && make smoke           # 12 checks; every one should PASS before you trust anything else
make metric-names PATTERN=schedule_to_start    # do the dashboard's metric names exist in YOUR versions?
```
If a dashboard panel is empty, fix the query in `tools/gen_dashboard.py` (`make dashboard`) using the real names.

## Phase 1: anatomy of one workflow

```sh
lg ramp --shape anatomy --rate 1 --duration 1 --run-id anat --params '{"timer_s": 5}'
make history WF=anat-anatomy-0-0                       # parked at "waiting-for-signal"
t workflow query  -w anat-anatomy-0-0 --type stage
t workflow update execute -w anat-anatomy-0-0 --name add_note --input '"hello"'
t workflow signal -w anat-anatomy-0-0 --name proceed
make history WF=anat-anatomy-0-0                       # complete history, with who-produced-each-event notes
t workflow show -w anat-anatomy-0-0 --output json      # the raw thing
```

1. **Map events.** Read the `history` output line by line against the `^` notes. Questions: why is there a `WorkflowTaskScheduled/Started/Completed` triple *before* `ActivityTaskScheduled`? Which events did the worker create (commands) and which did the server?
2. **Path of a start.** `make shard WF=anat-anatomy-0-0 SHARDS=4`, then confirm with `make cli-tdbg ARGS="workflow describe -w anat-anatomy-0-0"` (look for the shard id; if they disagree, my hash assumption is wrong for your version: tell me). Count persistence operations per activity round trip: run `lg ramp --shape chain --rate 20 --duration 30 --params '{"n":1}'` then again with `{"n":2}` and compare `sum(increase(persistence_requests[1m]))` per starts; the difference per extra activity is the answer to the checkpoint question.
3. **Sticky queues.** `make scale SVC=sdk-worker N=2`, start ~20 anatomy workflows with `timer_s: 20`, then `ch kill-one sdk-worker`. Workflows that were cached on the dead worker stall until the sticky schedule-to-start timeout (default 10 s; change with `W_STICKY_QUEUE_TIMEOUT_S` + `make workers`). Find one in `make history` with `WorkflowTaskTimedOut` followed by a `WorkflowTaskScheduled` on a normal queue (`task_queue=lab-tq` instead of `STICKY`).
4. **Replay.** `docker compose logs sdk-worker | grep replay-probe` shows `replaying=False` lines. Then `docker compose restart sdk-worker` (cache gone), `t workflow signal ... --name proceed`, grep again: the *earlier* steps print again with `replaying=True`. That duplicate print is why workflow code must be deterministic and why `workflow.logger` suppresses replay logs.
5. **Timers.** `lg ramp --shape timer --rate 1 --duration 1 --params '{"min_s":60,"max_s":60}' --run-id tm`; in `make history WF=tm-timer-0-0` compare `+ms` of `TimerStarted` and `TimerFired` with 60000.

## Phase 2: load profiles (ramps)

Rates are starts/sec. Each step holds `--step-seconds`. Use `--processes 2` (or more) above ~500/s and **check the `lag p99` column**: if it is > 250 ms you are measuring the generator.
After a ramp: `python3 tools/curve.py results/<run-id>.jsonl` marks the knee.

```sh
# Noop: start-API throughput, shard write rate
lg ramp --shape noop   --rates 10,50,100,500,1000,2000 --step-seconds 120 --processes 2 --run-id noop1
# Activity chain: matching, dispatch, history growth
lg ramp --shape chain  --rates 10,50,100,200,500 --step-seconds 120 --params '{"n":5,"spec":{"work_ms":50}}' --run-id chain1
# Fan-out: repeat with n = 100, 1000, 5000. 5000 exceeds the pending-activity limit: predict what happens first.
lg ramp --shape fanout --rates 1,2,5,10 --step-seconds 60 --params '{"n":1000,"max_parallel":0}' --run-id fan1000
lg ramp --shape fanout --rates 1,2,5,10 --step-seconds 60 --params '{"n":1000,"max_parallel":100}' --run-id fan1000b   # bounded
# Timer storm: ~100k sleeping workflows (1000/s x 100 s). Watch task_latency_queue and timer bursts.
lg ramp --shape timer  --rates 100,500,1000 --step-seconds 100 --params '{"min_s":60,"max_s":300}' --run-id timers
# Signal firehose: many signals to a few workflows.   Hot key: all to ONE workflow id.
lg ramp --shape signal --keys 20 --rates 100,500,1000,2000 --step-seconds 60 --run-id sig20
lg ramp --shape signal --keys 1  --rates 100,500,1000,2000 --step-seconds 60 --run-id hot1
lg ramp --shape noop   --keys 1  --rates 50,200,500 --step-seconds 60 --run-id hotstart   # start the same id repeatedly
# Long-lived: histories growing
lg ramp --shape longlived --rate 5 --duration 120 --params '{"iterations":3000,"sleep_ms":100,"can_mode":"never"}' --run-id ll
# Child fan-out: cross-shard traffic
lg ramp --shape parent --rates 1,2,5 --step-seconds 60 --params '{"n":1000,"max_parallel":200}' --run-id kids
```
For each step record: throughput vs target, rpc/e2e percentiles (table), schedule-to-start (dashboard row 2), **and the first container that hits its CPU ceiling** (dashboard row 4, `Container CPU`). Clean up leftovers: `make cleanup RUN=timers`.

***Example***

`lg` is the shell alias from the experiments guide: `docker compose run --rm --name loadgen loadgen`. So this runs the load generator container, which starts Temporal workflows at increasing rates and measures how the cluster copes.

****The flags****

- **`ramp --shape chain`**: start `ChainWorkflow` executions. Each one runs N activities one after another.
- **`--rates 10,50,100,200,500`**: five steps, at 10, 50, 100, 200, and 500 workflow starts per second.
- **`--step-seconds 120`**: hold each step for 2 minutes, so the ramp takes about 10 minutes in total.
- **`--params '{"n":5,"spec":{"work_ms":50}}'`**: each workflow runs 5 sequential activities, and each activity simulates 50 ms of I/O wait. These are actually the defaults for the `chain` shape, so the flag changes nothing here. It's a template you can edit.
- **`--run-id chain1`**: names the run. Workflow IDs become `chain1-chain-0-0`, `chain1-chain-0-1`, and so on. Each workflow is tagged with the search attribute `LabRunId=chain1`, which is how `make count RUN=chain1` and `make cleanup RUN=chain1` find them.

****What happens****

- It starts about 103,000 workflows in total: (10+50+100+200+500) × 120.
- Starts are scheduled at fixed times, whether or not the server keeps up.
- Because `chain` waits for results by default, it also measures end-to-end latency from the intended start time to the workflow result.
- The top step is roughly 2,500 activity executions per second.
- At the end it prints a per-step table with target vs. achieved rate, p50/p99/p99.9 latencies, errors, and generator `lag p99`.
- It then writes `results/chain1.jsonl` and counts workflows by status in visibility.
- Afterwards, `python3 tools/curve.py results/chain1.jsonl` marks the knee, the first step where latency jumps or throughput falls short.

****What to watch for****

- At 500 starts/s, each workflow lives about 0.3–0.5 s, so roughly 150–250 workflows are in flight. That probably exceeds the worker's default 100 activity slots.
- Predict where it breaks before you run it.
- Compare your prediction against the activity schedule-to-start panel and `slots_available` on the dashboard.

This run uses one generator process. If `lag p99` goes above about 250 ms at the high steps, add `--processes 2`, or you'll be measuring the generator instead of Temporal.


## Phase 3: find and move bottlenecks

Worker knobs are env vars: prefix the command, then `make workers`. Reset by running `make workers` without them.

**A. Worker side**
```sh
# 1. starve activity slots. Predict slots_available and activity schedule->start; server panels stay green.
W_MAX_CONCURRENT_ACTIVITIES=5 make workers
lg ramp --shape chain --rates 20,50,100 --step-seconds 60 --run-id starve
# 2. starve pollers (SDK may require >= 2), then compare with 50 and with W_POLLER_AUTOSCALE=1 (server >= 1.28)
W_MAX_CONCURRENT_WORKFLOW_TASK_POLLS=2 W_MAX_CONCURRENT_ACTIVITY_TASK_POLLS=2 make workers
# 3. tiny sticky cache: the classic hidden cost. Watch cache miss rate, replay latency, then throughput.
W_MAX_CACHED_WORKFLOWS=5 make workers
lg ramp --shape longlived --rate 20 --duration 90 --params '{"iterations":300,"sleep_ms":200}' --run-id cache5
make workers    # default cache (1000) and repeat the same command as cache1000
# 4. CPU-bound workflow code. First with the SDK's deadlock detector (fails the task after ~2 s):
lg ramp --shape poison --rate 1 --duration 5 --params '{"mode":"cpu"}' --run-id spin
docker compose logs sdk-worker | grep -i deadlock
#    then with the detector off, so the SERVER's workflow-task timeout does the work:
W_DEBUG_MODE=1 make workers
lg ramp --shape poison --rate 1 --duration 5 --params '{"mode":"cpu"}' --wft-timeout-s 2 --run-id spin2
make history WF=spin2-poison-0-0     # WorkflowTaskTimedOut
```
Bonus (Python-specific): sync vs async activities and the GIL. Compare `--params '{"call":{"activity":"work_sync"},"spec":{"cpu_ms":20}}'` against `"work_async"` with the same `cpu_ms` at 100/s: the async variant blocks the worker's event loop, so *workflow tasks and polls on the same process* slow down too (watch workflow-task schedule-to-start).

**B. Server side**
```sh
# 1. task queue partitions: edit config/dynamicconfig/docker.yaml (uncomment the two numTaskqueue* keys, value 1), wait 5 s
#    Compare against value 4 (default) and 16. Use make tq to see pollers/backlog.
lg ramp --shape noop --rates 500,1000,2000 --step-seconds 60 --processes 2
# 2. throttle the DB and follow the causal chain (dashboard row 3, top to bottom, plus the ch log timeline)
ch cpu postgres 0.5
ch db-latency 20          # then 100, then 500
lg ramp --shape chain --rates 50,100 --step-seconds 90 --run-id slowdb
ch db-heal; ch cpu postgres 0
# 3. hot key: more history hosts do not help. Explain in terms of shard ownership.
make scale SVC=history N=4
lg ramp --shape signal --keys 1    --rates 500,1000 --step-seconds 60 --run-id hotK1
lg ramp --shape signal --keys 1000 --rates 500,1000 --step-seconds 60 --run-id hotK1000
make shard WF=hotK1-signal-k0 SHARDS=4     # one shard, one owner, one CPU-bound history replica
python3 -m lab.probe shard-dist --pattern 'order-{i}' --count 100000 --shards 512   # ID scheme skew (needs pyfarmhash locally)
# 3b. hot shard, end to end (reproduce -> playbook detection -> verified fixes): experiments/hot-shard.md
make hotshard ARGS="run --kind restart"                            # PASS = hot shard detected AND neighbours slowed
make hotshard ARGS="run --kind restart --expect healthy"           # after a fix from the guide
# 4. shard count
make reset SHARDS=1     # then noop ramp; then make reset SHARDS=512 and repeat the identical ramp
```

**C. Limits and back-pressure**
```sh
# 1. edit dynamicconfig: frontend.namespaceRPS (per host) or frontend.globalNamespaceRPS = 200
lg ramp --shape noop --rates 100,200,400,800 --step-seconds 60 --run-id rps
#    dashboard row 1: temporal_request_failure {status_code="RESOURCE_EXHAUSTED"}, and rpc p99 rising BEFORE errors surface
#    (the SDK retries RESOURCE_EXHAUSTED itself). Does your generator back off or amplify? (--max-inflight bounds it.)
# 2. ceilings: the knee from `python3 tools/curve.py results/noop*.jsonl` at SHARDS=4 vs 512, 1 vs 3 history replicas
```

## Phase 4: chaos

Run each **with load applied** (start a ramp in one terminal, inject in another). Every `ch` call is logged in `results/chaos.log` and marked on Grafana.

| # | Fault and workload | Watch |
|---|---|---|
| 1 | Long heartbeating activity: `lg ramp --shape chain --rate 5 --duration 60 --no-await --params '{"n":1,"spec":{"work_ms":30000,"heartbeat_every_ms":1000},"call":{"activity":"work_sync","start_to_close_s":300,"heartbeat_timeout_s":5}}'` then `ch kill sdk-worker 5`. Repeat with `heartbeat_timeout_s` removed and `heartbeat_every_ms` 0: predict the retry time. | activity attempts in `make history`, retry gap |
| 2 | `make scale SVC=history N=3`; chain load at 100/s; `ch kill-one history 30` | membership panel, `ShardOwnershipLost`, throughput recovery time, in-flight workflow results |
| 3 | chain load; `ch kill matching 20`; `make tq` before/after | backlog drain, lost tasks (there should be none) |
| 4 | noop load; `ch kill frontend 15`. Then `make scale SVC=frontend N=2` and repeat (client-side DNS balancing) | client failures, rpc p99 |
| 5 | `make scale SVC=frontend N=2`, same for `matching` and `history`; chain load; `ch rolling-restart history 20`, then matching, frontend. Success = `make count RUN=<id>` has zero Failed/Terminated | count by status |
| 6 | `ch db-pause 30`, then 120, then 600 under chain load | when history starts erroring, thundering herd on resume |
| 7 | `ch db-latency 500` | cascade: persistence -> history -> frontend -> client timeouts |
| 8 | no replicated DB in the lab; nearest analogues: `ch db-kill 10`, `ch db-cut`, `ch db-reset-peer` (then `ch db-heal`) | shard ownership stability, error types |
| 9 | Long activities (see #1) and `ch partition sdk-worker 5`, then `20` (shorter / longer than the 10 s workflow task timeout) | `WorkflowTaskTimedOut`, duplicate activity execution, worker logs on reporting a stale attempt |
| 10 | `make scale SVC=history N=2`; chain load; `ch partition history 40` (cuts one history host off completely; not exactly "DB only", the lab has one shared DB path) | shard range-id fencing: writes from the old owner must be rejected |
| 11 | **Non-determinism.** See below | `temporal_workflow_task_execution_failed`, `WorkflowTaskFailed` |
| 12 | **Poison pill.** `lg ramp --shape poison --rate 10 --duration 10 --params '{"mode":"raise"}' --run-id pz` (also `raise_after_activity`, and `fail` for contrast). Then add `--exec-timeout-s 60`. | workflows stay Running; task retries with backoff; history does **not** grow per retry, so use `t workflow describe -w pz-poison-0-0` for the pending task attempt count |
| 13 | **Retry storm.** `lg ramp --shape fanout --rate 2 --duration 30 --run-id storm --params '{"n":200,"call":{"initial_interval_ms":100,"backoff":1.0,"max_interval_s":1,"schedule_to_close_s":60},"spec":{"fail_rate":1.0}}'`. Compare with default backoff (`"backoff":2.0,"max_interval_s":100`). | `service_requests` for activity RPCs, `temporal_activity_execution_failed`, server CPU |
| 14 | **Hanging activity.** `--params '{"n":1,"spec":{"work_ms":600000},"call":{"start_to_close_s":900}}'`, `ch kill sdk-worker 5`: how long until it is retried? Fix: add `"heartbeat_every_ms":1000` to spec and `"heartbeat_timeout_s":5` to call | time to detection |
| 15 | **Unbounded history.** Lower `limit.historyCount.warn/error/suggestContinueAsNew` in dynamicconfig, then `--shape longlived --params '{"iterations":20000,"sleep_ms":0,"can_mode":"never"}'`; repeat with `"can_mode":"suggested"` and `"every"` | warnings in `docker compose logs history`, terminal failure, replay latency |
| 16 | **Oversized payloads.** `lg ramp --shape noop --rate 1 --duration 1 --payload-bytes 3000000` (over 2 MB blob limit) and `5000000` (over 4 MB gRPC). Then a big activity result: `--shape chain --params '{"n":1,"spec":{"response_bytes":3000000}}'` | where the error appears and how visible it is (client error vs stuck workflow) |
| 17 | **Pending/signal limits.** `--shape fanout --params '{"n":3000}'` (pending activities), `--shape parent --params '{"n":3000}'` (pending children), `--shape signal --keys 1 --rate 500 --duration 60` (signals per execution). Check exact limits for your version. | what fails first, and whether the workflow or the task fails |
| 18 | **Clock skew.** `docker compose run -d --name skewed --no-deps --entrypoint faketime sdk-worker -f "+90s" python -m lab.worker`, run chain load, compare behaviors that depend on the worker's clock (local timers, heartbeat throttling) with those the server owns (timeouts, timers). Expect mostly "nothing" and explain why. | |

**#11 Non-determinism, step by step** (v1 = A,B,wait,C; v2 = B,A,wait,C; v3 = v2 behind `workflow.patched`):
```sh
make deploy-variant V=v1
lg ramp --shape evolving --rate 5 --duration 10 --run-id evo --no-await     # runs A,B then parks waiting for a signal
make deploy-variant V=v2                                                     # "deploy" the reordered code
t workflow signal --query 'LabRunId="evo"' --name proceed --yes             # wakes them: replay hits B where history has A
make history WF=evo-evolving-0-0                                             # WorkflowTaskFailed; workflow still Running
# recover, three ways (try each on a fresh 'evo' run):
make deploy-variant V=v1                                                     # (a) roll back: stuck tasks succeed on their next retry
t workflow reset --query 'LabRunId="evo"' --type FirstWorkflowTask --reason fix --yes   # (b) reset (check flags: t workflow reset --help)
make deploy-variant V=v3                                                     # (c) patch: old histories replay as A,B; new runs execute B,A
```
Question for the atlas: why does (c) fix executions that were *already* broken under v2?

## Phase 5: recovery and operations

```sh
# Bulk repair: use the reset from #11(b). Verify with: make count RUN=evo
# Drain a backlog: stop workers, queue ~100k workflow tasks, then add workers and time the drain.
docker compose stop sdk-worker
lg ramp --shape noop --rates 1000 --step-seconds 100 --processes 2 --no-await --run-id backlog
make tq                         # ApproximateBacklogCount
make scale SVC=sdk-worker N=1   # then 2, 4, 8: drain rate per worker count; what stops scaling linearly?
# Latency SLO alerts: http://localhost:9090/alerts (rules in config/prometheus/rules.yml).
#   Make WorkflowTaskScheduleToStartHigh fire with W_MAX_CONCURRENT_WORKFLOW_TASKS=2, then make it stop firing by fixing the cause, not the threshold.
# Worker deploy while long-running workflows are active: repeat #11 with several `make deploy-variant` steps;
#   for Worker Versioning (deployments) read the SDK docs, it needs server-side support enabled.
```

## Deliverables
`experiments/atlas.md`: the Failure Atlas table (pre-filled with every experiment above), the capacity-model template, and the runbook template.
