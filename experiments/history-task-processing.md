# History task processing: produce the write-reject loop, investigate it, fix it

A runnable version of the playbook
[History Task Processing — Tuning and Troubleshooting](https://github.com/tsurdilo/temporal-server-operations/blob/main/playbooks/history-task-processing-tuning.md)
and its alert runbook
[87 — History Write-Reject Loop](https://github.com/tsurdilo/temporal-server-operations/blob/main/observability/alerts/server/runbooks/87-history-write-reject-loop.md).

The test is `lab/historytasks.py` (`make historytasks ARGS="..."`). It gives the history service a small database
budget, drops a backlog of history tasks on it, reads the playbook's panels from Prometheus, and prints a verdict.
One scenario reproduces the loop, one applies the playbook's fix, and three reproduce the ways the fix goes wrong.

## Background, in four lines

Every workflow step becomes a **history task**: saved in the database, loaded back, then executed by the history
service. Executing a task reads the workflow's state (`GetWorkflowExecution`, only on a cache miss) and writes it
back (`UpdateWorkflowExecution`). **Nothing limits how fast tasks start by default.** When the persistence rate
limiter refuses a task's write, the pod throws the cached state away, so the retry must read it back first. Those
reads spend the budget the writes need, so more writes are refused: **the write-reject loop**.

## What the test does

```
 settle (75 s)           burst                                     drain
 ── settings applied ──► 400 ParentWorkflows x 8 children ───────► until every parent completed
    probe workflow every 2 s, start to finish (does unrelated work still move?)
```

1. **Settings.** The test writes a marked block at the end of `config/dynamicconfig/docker.yaml`: always
   `history.persistenceMaxQPS: 300` (the "database budget"), plus the scenario's task-scheduler settings. It removes
   the block when the run ends. If a run is killed, `make historytasks ARGS=reset` removes it.
2. **Settle.** The limiters re-read their values within about a minute. The test also checks that the cluster is
   idle and prints the `Get*Tasks` loading rate next to the playbook's idle floor.
3. **Burst.** 400 parents (started at 100/s), each starting 8 children: 3,600 executions. The children are started **by history
   tasks**, not by the client, so this is a backlog of task processing (the playbook's own test shape).
4. **Drain.** The run lasts until every parent has completed. If no parent finishes for 3 minutes the test stops
   waiting, reports the stall and terminates the leftovers, so the next run starts clean. The test's own worker
   (its own task queue) runs the workflows, so the lab's `sdk-worker` is not involved.

| Scenario | Settings (on top of the 300/s budget) | Playbook section | PASS means |
|---|---|---|---|
| `loop` | none: the default | 3. the write-reject loop | indicator peak > 5 with > 10 refused calls/s |
| `calibrate` | none; a slow trickle of the same work | 5.2 step 2: calls per task | measured without refusals; writes `results/historytasks-calibration.json` |
| `fix` | limiter on, shadow off, `GlobalNamespaceMaxQPS` = 75% and `GlobalMaxQPS` = 100% of `budget ÷ calls per task` | 5.3 step 3 | nothing refused, loop flat, scheduler throttling > 0, backlog drained |
| `shadow` | limiter on, limits set, **shadow mode left on** | 4.2 trap one | throttling is *counted* yet the loop still runs |
| `zero` | limiter on, shadow off, **limits left at 0** | 4.2 trap two | nothing throttled and the loop still runs |
| `toolow` | limiter on, shadow off, namespace limit at 15% | 5.4 too low | nothing refused, but tasks wait more than 1 s at the scheduler |

Exit codes: `0` = PASS, `1` = FAIL, `2` = INVALID (the host slept during the run, see section 4). Each run writes
`results/<run-id>.json`.

---

## 0. Set up

```sh
make up && make smoke
docker compose build sdk-worker        # only if lab/ changed since the last `make up`
```

**Keep the machine awake.** A run takes 10–17 minutes; if a laptop sleeps, Docker freezes and every timer and backoff
freezes with it. Run with `caffeinate -dimsu make historytasks ARGS="..."` and the lid open. The test detects a
paused host and reports `RESULT: INVALID` instead of numbers.

**Start from an idle cluster.** If the lab has been down for longer than the namespace retention (24 hours), the
first thing it does on start-up is delete every expired workflow. That is a history task backlog of its own
(`TimerActiveTaskDeleteHistoryEvent`); here it was ~147,000 executions, ~20 minutes. The test warns when it sees
it. Wait for it to finish, or every number will include it.

---

## 1. Produce the issue

```sh
make historytasks ARGS="run --scenario loop"
```

About 17 minutes. A real run on an idle cluster (server 1.32.0, 512 shards, one history pod, local Postgres):

```
baseline: Get*Tasks loading 6.5/s (idle floor for 512 shards ~29/s), history DB calls reached 15.1/s, refused 0.0/s

burst: 400 ParentWorkflows x 8 children = 3600 executions, started at 100.0/s
  t=   24s parents done 0/400    loop indicator  47.0  refused 177.2/s  tasks completed 18.2/s  attempts  77.8/s
  t=   84s parents done 29/400   loop indicator   4.2  refused  70.3/s  tasks completed 66.7/s  attempts 152.7/s
  t=  205s parents done 166/400  loop indicator   1.4  refused   0.0/s  tasks completed  3.4/s  attempts   6.9/s  <- idle
  t=  325s parents done 243/400  loop indicator   2.0  refused  22.8/s  tasks completed 66.2/s  attempts  93.0/s
  t=  385s parents done 292/400  loop indicator   1.2  refused   0.0/s  tasks completed  1.9/s  attempts   4.9/s  <- idle
  t=  445s parents done 292/400  loop indicator   1.4  refused   0.0/s  tasks completed  1.9/s  attempts   2.8/s  <- idle
  t=  506s parents done 346/400  loop indicator   2.4  refused  38.7/s  tasks completed 66.8/s  attempts 127.1/s
  t=  566s ... t= 686s           (idle again: 359/400 for two minutes)

backlog     400 parents x 8 children: drained in 901 s (4.0 executions/s), 1 parents UNFINISHED (900 s cap)
loop        indicator (cleared / cache miss) avg 3.3  peak 13.2   state cleared 26.5/s   genuine cache misses 8.1/s
refused     21.9/s history DB calls refused (PersistenceLimit); top: GetCurrentClusterMetadata 9.6,
            GetWorkflowExecution 9.0, RangeCompleteReplicationTasks 1.6, UpdateWorkflowExecution 0.9
reached     GetWorkflowExecution attempted 16.8/s, refused 9.0/s, reached the DB 7.8/s
api calls   refused over the window (Resource Exhausted with Cause, PersistenceLimit): RespondWorkflowTaskCompleted,history
            1961, RecordChildExecutionCompleted,history 1197, RespondWorkflowTaskCompleted,frontend 1023,
            StartWorkflowExecution,history 231, ScheduleWorkflowTask,history 212
tasks       completed 24.3/s   attempts 48.9/s ('History Task Throughput' counts attempts)   attempts/task p99 16.7
scheduler   throttled 0.0/s
database    calls per completed task 5.19   UpdateWorkflowExecution p99 9.6 ms (flat = the limiter, not the DB)
alerts      86 (refused > 10/s): 9 consecutive min -> would not fire;  87 (ratio > 5 and refused > 10/s): 2 consecutive
            min -> would not fire  (both need 10; window 15 min)
probe       settle unrelated workflow: start->result p50 36 ms  p95 51 ms
probe       burst  unrelated workflow: start->result p50 34 ms  p95 4159 ms  (1 never finished)

RESULT: PASS (expect=loop: the write-reject loop surfaced: indicator peak > 5 with > 10 refused calls/s)
```

What to notice:

* **The loop forms within seconds.** In the first 24 s the indicator was 47: state was thrown away 47 times faster
  than it was genuinely missing from the cache. (An earlier run reached 55–87.)
* **Twice as many attempts as completed tasks.** 48.9 attempts/s for 24.3 completions/s; the slowest 1% of tasks
  needed 17 attempts.
* **Then it turns into waves, with the database idle in between.** Bursts of ~66 tasks/s alternate with gaps of
  1–2 minutes at ~2 tasks/s and **zero** refusals, while a hundred or more workflows wait. See section 4.
* **Unrelated work stalls.** A one-step probe workflow normally finishes in 36 ms; during the burst its p95 was
  4.2 s, and one never finished.
* **API calls are refused too, but retried out of sight.** *Resource Exhausted with Cause* counted about 3,000 refused
  `RespondWorkflowTaskCompleted` (the worker finishing workflow tasks), 1,197 `RecordChildExecutionCompleted` and
  231 `StartWorkflowExecution` (history starting the children). The SDK and history retried them, so the test's own
  client saw no error; the damage shows up as executions not moving. This is the playbook's 1.2 sign "clients are
  being refused while a backlog is worked through". It happened here even though client calls get a higher-priority
  share of the budget (3.4): with one history pod and a 300/s budget, that share was not enough.

## 2. Show and investigate it

For a live look, run the scenario and keep a second terminal on the readings:

```sh
make historytasks ARGS="run --scenario loop"          # terminal 1
make historytasks ARGS="indicator --window 60"        # terminal 2, repeat while the burst runs
```

Grafana (http://localhost:3000, folder *Temporal Lab*) → **Temporal Server Dashboard v2.25.0**, which carries every
panel the playbook names. Pick namespace `lab`. Follow the playbook's own order:

### Step 1: is it the loop? (playbook 3.3)

**Panel:** *Write-Reject Loop Indicator (cleared / cache miss)*, Persistence group.

```promql
sum(rate(workflow_context_cleared[$__rate_interval])) / sum(rate(cache_miss{cache_type="mutablestate"}[$__rate_interval]))
```

| Reading | Meaning | This run |
|---|---|---|
| below ~1 | normal | ~1.5 at idle (both numbers tiny: read the two lines, not just the ratio) |
| 1–2 | some refused writes | |
| above ~5 | the loop is running | **47** in the first 24 s, peak 13.2 on the 1-minute view |

The panel's two companion lines matter as much as the ratio: state **cleared** at 26.5/s on average (far more at the
start) while genuine cache misses stayed at 8.1/s. A cache that is too small shows the opposite (misses high).
Resizing `history.hostLevelCacheMaxSize` would not help.

### Step 2: who is being refused? (playbook 1.2, 3.3)

**Panel:** *Rejected Database Calls by Operation and Scope*.

```promql
sum(rate(persistence_errors_resource_exhausted{namespace="$namespace"}[$__rate_interval])) by (operation, resource_exhausted_cause, resource_exhausted_scope)
```

`GetWorkflowExecution` (the re-reads) leads among workflow operations, ten times ahead of `UpdateWorkflowExecution`
(the writes that started it): **9.0/s against 0.9/s**. The reads are the consequence; the refused writes are the
cause, because an attempt whose read is refused never reaches its write.

On this lab the overall top entry was **`GetCurrentClusterMetadata`** (9.6/s), which is not workflow work at all.
It is a **lab artifact**: the lab sets `system.forceSearchAttributesCacheRefreshOnRead: true`, which makes every
search-attribute read (every visibility task, every start) fetch cluster metadata from the database, and a refused
fetch is retried on the next read. That setting is off by default; ignore this line when comparing with production.

If the top entries are `GetTransferTasks` / `GetTimerTasks` / `GetVisibilityTasks` instead, that is task
**loading** being throttled, a different and later step (playbook 7).

### Step 3: attempts versus what reached the database (playbook 3.5)

**Panels:** *Rejected Database Calls by Operation and Scope* against *Database Calls That Reached the Database*.

| `GetWorkflowExecution` | This run |
|---|---|
| attempted | 16.8/s |
| refused | 9.0/s |
| **reached the database** | **7.8/s** |

More than half of the reads you would react to on *Persistence Requests* never touched Postgres. Size everything
from what reached the store.

### Step 4: are tasks finishing? (playbook 2.4, 5.3)

**Panel:** *History Task Throughput* (`task_requests`). **Careful:** in 1.32 `task_requests` is recorded on every
**attempt**, not once per finished task. Under the loop it overstated throughput by 2x here (48.9 attempts/s for
24.3 completions/s). The test uses `task_attempt_count` for completions: that histogram is recorded when a task
finishes, and its distribution is the number of attempts each task needed (p99 **16.7** here):

```promql
sum(rate(task_attempt_count{service_name="history"}[1m]))                                    # tasks completed/s
histogram_quantile(0.99, sum by (le) (rate(task_attempt_bucket{service_name="history"}[5m])))   # attempts per task
sum(rate(task_errors_throttled{service_name="history"}[1m]))                                 # attempts refused/s
```

(`task_attempt` is also recorded on every attempt beyond the 30th, so tasks that need more than 30 attempts are
counted more than once. Here p99 was 17, so the effect is negligible.)

### Step 5: is the database actually struggling? (runbook 87, "what it is not")

**Panel:** *Persistence Latencies*. `UpdateWorkflowExecution` p99 stayed at **9.6 ms**, the same as in calibration.
Flat latency means the limiter turned the calls away before Postgres saw them. Not a slow database.

### Step 6: do clients feel it? (playbook 3.4)

**Panel:** *Resource Exhausted with Cause*, filtered to `PersistenceLimit`. Here: ~3,000 refused
`RespondWorkflowTaskCompleted`, 1,197 `RecordChildExecutionCompleted`, 231 `StartWorkflowExecution`, all retried
silently by the SDK and by history. The user-visible symptom is executions not moving: the probe workflow's p95 went
from 51 ms to 4.2 s.

### Step 7: would the alerts have fired? (playbook 8.1)

Alert 87's expression:

```promql
((sum(rate(workflow_context_cleared[5m])) > 0) / (sum(rate(cache_miss{cache_type="mutablestate"}[5m])) > 0))
  and (sum(rate(persistence_errors_resource_exhausted{service_name="history",resource_exhausted_cause="PersistenceLimit"}[5m])) > 10)
```

Minute by minute over this run:

| Alert | Its expression, per minute | Would it fire? |
|---|---|---|
| **86** (refusals > 10/s, by scope and cause, 10 min) | `System` scope: **72, 53, 57, 63, 61, 32, 22, 12.5**, 8.7, 8.7, 3.5, ... | **no**: 9 consecutive minutes, one short |
| **87** (ratio > 5 **and** refusals > 10/s, 10 min) | 5-minute ratio: **6.5**, 4.5, 4.7, 4.4, 4.5, 2.7 ... 1.5 | **no**: above 5 for 2 minutes |

Neither alert fired, although the 1-minute panel read 47 at the start and work was visibly stuck. Alert 87's
refusal half held, but its 5-minute ratio sank below 5 within two minutes: once the loop turns into waves (section 4),
state is cleared in bursts and the 5-minute average dilutes it. (An earlier run of the same scenario put alert 86 at
exactly 10 minutes, so it is borderline.) The playbook's threshold of 5 was measured on a cluster where the loop
stayed continuous. **Read the panel, not just the alerts**, whenever a backlog is draining slowly.

### Confirm what is in force (playbook 4.3)

```sh
docker compose logs history | grep "Quota changed"
```

```
"msg":"Quota changed","component":"persistence","scope":"host","current-quota":9000,"new-quota":300
```

That line is the only place the enforced value appears; it needs the history log level at `info` (the lab's
default). The task-scheduler limits log the same way with `component=task-scheduler`. It has one blind spot,
described under trap two below.

## 3. Fix it

The playbook's answer is **not** to raise the persistence limit (that lets more of the same loop through). It is to
**start fewer tasks**, with the task scheduler's rate limits, which are off by default. The test walks the playbook's
four steps (section 5), then reproduces both of its traps (section 4.2).

### Step 1–2: measure calls per task (playbook 5.2)

```sh
make historytasks ARGS="calibrate"
```

Runs the same parent/child work as a slow trickle (60 parents at one every 2 s) under the same 300/s budget, so
nothing is refused, and divides what reached the database by the tasks that completed:

```
calls per task = 116.3 DB calls reached / 25.1 tasks completed = 4.63
budget 300 / 4.63 = 65 tasks/s  ->  history.taskSchedulerGlobalNamespaceMaxQPS 48 (75%), history.taskSchedulerGlobalMaxQPS 64
```

The playbook measured 3.69 on its cluster and says to use your own: this lab's figure is 4.63 (two calibrations
gave 4.93 and 4.63). Two things matter for getting it right:

* **Measure in ordinary work, not during the loop.** The loop run reported 5.08 calls per task, inflated by retries.
* **Divide by completed tasks, not by *History Task Throughput*.** That panel (`task_requests`) counts attempts;
  calibration counted 32.3 attempts/s for 25.1 completions/s even with nothing refused. The test uses
  `task_attempt_count`.

The numbers are written to `results/historytasks-calibration.json`; the `fix`, `shadow` and `toolow` scenarios read
them from there.

### Step 3: set the limits, with shadow mode off (playbook 5.3)

```sh
make historytasks ARGS="run --scenario fix"
```

```yaml
history.taskSchedulerEnableRateLimiter:
  - value: true
history.taskSchedulerEnableRateLimiterShadowMode:
  - value: false
history.taskSchedulerGlobalNamespaceMaxQPS:
  - value: 48          # 75% of 300 / 4.63
history.taskSchedulerGlobalMaxQPS:
  - value: 64          # 300 / 4.63
```

Same burst, same 300/s budget, the two limits from calibration:

```
  t=   24s parents done 0/400    loop indicator 9.1  refused 124.1/s  tasks completed 26.6/s  scheduler throttled 196.9/s
  t=   84s parents done 33/400   loop indicator 1.5  refused   0.0/s  tasks completed 45.5/s  scheduler throttled 197.1/s
  t=  205s parents done 149/400  loop indicator 1.5  refused   0.0/s  tasks completed 45.8/s  scheduler throttled 246.7/s
  t=  385s parents done 314/400  loop indicator 1.5  refused   0.0/s  tasks completed 43.1/s  scheduler throttled 256.9/s
  t=  506s parents done 398/400  loop indicator 1.5  refused   0.0/s  tasks completed 20.5/s  scheduler throttled 231.3/s

RESULT: PASS (expect=healthy: task attempts refused <= 2/s, no loop, scheduler throttling > 0, backlog drained)
```

| | `loop` (no limits) | `fix` | |
|---|---|---|---|
| backlog drained in | 901 s | **520 s** | **1.7x faster**, with the same database budget |
| executions completed | 4.0/s | **6.9/s** | |
| tasks completed | 24.3/s, in waves | **~45/s, steady**, pinned at the 48/s limit | |
| idle gaps with work pending | 1–2 min each | **none** | |
| attempts per task, p99 | 16.7 | **3.2** | the same work is no longer done many times |
| refused task attempts | 10.2/s | **1.6/s** | |
| `GetWorkflowExecution` refused | 9.0/s | **1.5/s** | the re-reads have gone |
| loop indicator | peak 13.2 | **1.5 flat** after the first 30 s | |
| scheduler throttled | 0 | **235/s** | tasks wait instead of failing: the setting working |

That is the playbook's "what tuned looks like": throttling above zero, refusals near zero, backlog draining. Three
things it did **not** fix, all worth knowing:

* **The first 30 seconds.** Refusals hit 124/s at the start of the burst, and alert 86's expression stayed above 10/s
  for 5 minutes. Those were mostly **API** calls: 100 parent starts a second plus the worker's
  `RespondWorkflowTaskCompleted` (~3,300 refused over the run) are client traffic over the budget, and the task
  scheduler does not limit client traffic. That is the playbook's 3.5 row "client traffic is itself over the limit",
  a decision for the persistence QPS limits, not for these settings.
* **`GetCurrentClusterMetadata` refusals (6.2/s).** A lab artifact of `system.forceSearchAttributesCacheRefreshOnRead`
  (step 2 above), not task work.
* **Unrelated workflows in the same namespace.** The probe's p95 stayed at 4.5 s (4.2 s without limits): the limit is
  per namespace, so the probe queues behind the burst. To protect one workload from another, give them different
  namespaces and set `taskSchedulerGlobalNamespaceMaxQPS` per namespace (the playbook's `batch-reports` example in
  5.2).

### Trap one: limiter on, shadow mode left on (playbook 4.2)

```sh
make historytasks ARGS="run --scenario shadow"     # same limits, shadow mode left at its default (true)
```

| Reading | `shadow` | the loop, no limits |
|---|---|---|
| scheduler throttled | **34.2/s**: the panel says the limiter is working | 0 |
| loop indicator peak | **11.1** | 13.2 |
| refused calls | **29.5/s** | 21.9/s |
| attempts per completed task, p99 | 17.1 | 16.7 |
| alert 86 | **would fire** (10 consecutive minutes over 10/s) | 9 minutes, one short |

```
RESULT: PASS (expect=loop+counted: the throttling panel shows throttling, yet the loop still runs
        (shadow mode counts and holds nothing back))
```

Everything except the throttling panel matches the unlimited run. That is the trap: shadow mode records the
throttle (`task_scheduler_throttled`) **before** it checks shadow mode, then lets the task through. The source
(`common/tasks/rate_limited_scheduler.go`) shows it directly:

```go
metrics.TaskSchedulerThrottled.With(s.metricsHandler).Record(1, ...)
if s.options.EnableShadowMode() {
    // in shadow mode, only emit metrics, but don't actually throttle
    return
}
```

This is what step 1 of the playbook relies on to size the limits safely, and why forgetting to switch it off leaves
a cluster that looks tuned and behaves untuned.

### Trap two: limits left at 0 (playbook 4.2)

```sh
make historytasks ARGS="run --scenario zero"       # limiter on, shadow off, no limits set
```

| Reading | `zero` |
|---|---|
| scheduler throttled | **0.0/s**: the limiter is live and never engages |
| loop indicator peak | **12.4** |
| refused calls | **41.5/s** (`GetWorkflowExecution` 16.6/s) |
| attempts per completed task, p99 | 17.0 |

```
RESULT: PASS (expect=loop: limiter on and live, yet nothing throttled and the loop runs:
        the 0 limits fell back to the persistence number)
```

At `0` each limit falls back to the persistence number, 300, which counts **database calls**. As a **task** limit
that admits 300 tasks/s against a budget that supports 65, so it never engages and the loop runs exactly as
before.

**The log does not show the fallback.** The playbook's 4.3 check (`grep "Quota changed"`) printed this when the
`zero` run started:

```
"Quota changed","component":"task-scheduler","scope":"namespace","wf-namespace":"lab","current-quota":48,"new-quota":0
"Quota changed","component":"task-scheduler","scope":"host","current-quota":64,"new-quota":0
```

The logged value is the task-scheduler setting itself. The fallback to the persistence rate happens afterwards, in
`NewPrioritySchedulerRateLimiter` (`service/history/queues`), which the log never sees. So **a `new-quota` of `0` on
a `task-scheduler` line is trap two**, not "no limit" and not "the persistence value". Read it that way.

### Limits set too low (playbook 5.4)

```sh
make historytasks ARGS="run --scenario toolow --max-drain-s 600"    # namespace limit 9 (15%), all-namespaces 64
```

```
  t=   84s parents done 0/400  loop indicator 1.4  refused 0.0/s  tasks completed 8.8/s  scheduler throttled 431.0/s
  t=  325s parents done 0/400  loop indicator 1.5  refused 0.0/s  tasks completed 9.0/s  scheduler throttled 625.4/s
  t=  566s parents done 0/400  loop indicator 1.5  refused 0.0/s  tasks completed 8.9/s  scheduler throttled 661.4/s

probe       burst  unrelated workflow: start->result p50 107013 ms  p95 344100 ms

RESULT: PASS (expect=slow: nothing refused and no loop, tasks held at the scheduler, completions pinned at the
        configured limit: the limit, not the database, is the constraint)
```

Every rejection panel looks perfect: nothing refused, loop indicator flat. Meanwhile **not one of the 400 parents
finished in 10 minutes**, because 9 tasks a second shared round-robin across all of them advances each one slowly,
and the one-step probe workflow took **107 s** (p50) and **344 s** (p95) instead of 36 ms. This is the case the
playbook warns is easy to miss. The signals that show it here:

* **Tasks completed pinned at the configured number** (8.8/s against 9) while the backlog is not draining.
* **Unrelated executions slow down**: in production, workflow-task and activity schedule-to-start latency, or any
  end-to-end SLO.
* *Immediate / Scheduled Queue Lag per Pod*, which the playbook recommends. On this lab those panels were empty: the
  lag metric is emitted every 5 minutes and only for some shard states.

What did **not** show it: *Task Scheduler Latency per Operation* (`task_latency_schedule`) stayed at **1 ms p95** the
whole time. Throttled tasks are handed back to be retried later rather than waiting inside the scheduler (the
`TrySubmit` path in `rate_limited_scheduler.go`), so the time they spend held back does not appear in that latency.
Do not rely on that panel to spot a limit that is too low.

**To move the number** (playbook 5.4): change `taskSchedulerGlobalNamespaceMaxQPS` only, in steps, wait about a
minute, and re-read. Try it here with `make historytasks ARGS="run --scenario fix --ns-limit 30"` and compare drain
time and refusals against 48.

## Results at a glance

All six on the same cluster (budget 300/s, 400 parents × 8 children, one history pod):

| Scenario | Verdict | Drained in | Tasks completed | Refused task attempts | Loop indicator peak | Throttled | Probe p95 |
|---|---|---|---|---|---|---|---|
| `calibrate` (trickle) | 4.63 calls/task | 120 s | 25.1/s | 0 | 2.5 | 0 | 39 ms |
| `loop` | PASS (loop) | 900 s cap, 1 parent left | 24.3/s, waves | 10.2/s | 13.2 | 0 | 4.2 s |
| `fix` | PASS (healthy) | **520 s** | ~45/s, steady | 1.6/s | 1.5 after 30 s | 235/s | 4.5 s |
| `shadow` | PASS (trap one) | 761 s | 29.2/s, waves | 13.8/s | 11.1 | **34/s** | 4.3 s |
| `zero` | PASS (trap two) | ended at 514 s with 33 left* | 39.1/s, waves | 18.8/s | 12.4 | **0** | 4.5 s |
| `toolow` | PASS (too low) | none in 600 s | 8.8/s, pinned | 0 | 1.8 | 584/s | **344 s** |

\* That run used an earlier stall rule (no parent finishing for 3 minutes), which fired during one of the loop's idle
gaps. The current rule also requires that no history task completes for a minute.

---

## 4. Things this test showed that the playbook does not spell out

1. **The loop turns into waves, and the database sits idle between them.** A refused task backs off before retrying:
   3 s, growing 1.5x per refusal, up to **5 minutes** (`taskResourceExhaustedReschedulePolicy` in
   `service/history/queues`). Tasks refused together back off together and come back together, so the history pod
   alternates between bursts of ~66 tasks/s and gaps of 1–2 minutes at ~2 tasks/s with **no** refusals, while the
   backlog waits. The playbook describes the loop as self-sustaining; on this cluster it is self-*interrupting*, and the
   gaps are a large part of why the unlimited run drained 1.7x slower than the tuned one.
2. **That is also why the alerts stayed quiet.** The waves pull the 5-minute ratio below 5 within two minutes, so
   alert 87 never fired, and alert 86 landed at 9 and 10 consecutive minutes in two runs: borderline. Both were tuned
   on a cluster where the loop stayed continuous. On a small or single-pod cluster, watch the panel.
3. **The budget is shared with the history pod's own housekeeping.** The history log during the first run showed the
   pod's internal calls failing with `System Persistence Max QPS Reached`: namespace-cache refreshes, queue reads and
   **membership upserts**. A persistence limit sized only for workflow work can starve the pod's own bookkeeping;
   leave headroom for it. (The most-refused operation in every run, `GetCurrentClusterMetadata`, is a lab artifact:
   the lab's `system.forceSearchAttributesCacheRefreshOnRead: true` turns every search-attribute read into that
   database call. It is off by default.)
4. **`task_requests` counts attempts, not tasks.** *History Task Throughput* (the panel the playbook divides by in
   5.2) read 48.9/s in the loop for 24.3 tasks actually completed, and 32.3/s against 25.1 even in calibration. Use
   `task_attempt_count` (recorded when a task finishes) for completions and for calls per task.
5. **`task_scheduler_throttled` counts refusals, not waiting tasks.** It read 235/s in `fix` while 45 tasks/s were
   admitted, and 584/s in `toolow` against 9: each held-back task is counted every time it is retried. The
   playbook's own test shows the same shape ("318 to 573 a second while tasks completed at 60"). Read it as "the
   limit is engaging", never as a queue length.
6. **Trap two is invisible in the log.** With the limits at `0`, `Quota changed` printed `new-quota: 0` for the task
   scheduler. The fallback to the persistence number happens after the logged value (section 3). A `0` on a
   `task-scheduler` line means the limits are not set.
7. **The scheduler limit protects the database, not your neighbours in the same namespace.** The probe workflow was no
   faster with the fix (p95 4.5 s), and 344 s with the limit too low: everything in the namespace queues behind the
   same limit. Isolation needs separate namespaces with their own limits.
8. **Too-low limits do not show on the scheduler latency panel** (1 ms p95 in `toolow` while tasks waited minutes).
   Judge them on completions pinned at the limit and on end-to-end latency (section 3).
9. **Retention clean-up after downtime is a history-task backlog too.** When the lab came back after 10 days,
   ~147,000 expired executions were deleted through the same task processing (`TimerActiveTaskDeleteHistoryEvent`,
   ~20 minutes). Any measurement taken during that is measuring the clean-up.
10. **A sleeping laptop freezes everything, including timers and backoffs.** Two batches of these runs were
    corrupted by macOS sleeping mid-run, which looked exactly like history tasks stalling for 17–33 minutes. The test
    now detects wall-clock jumps and reports `RESULT: INVALID`. Run long scenarios with the machine awake
    (`caffeinate -dimsu make historytasks ...`, lid open).

## Options

`make historytasks ARGS="run --help"` lists everything.

| Option | Default | Meaning |
|---|---|---|
| `--scenario` | (required) | `loop`, `fix`, `shadow`, `zero`, `toolow` |
| `--budget` | 300 | `history.persistenceMaxQPS` during the run |
| `--parents` / `--children` / `--rate` | 400 / 8 / 100 per second | the burst |
| `--ns-limit` / `--all-limit` | from calibration | override `taskSchedulerGlobalNamespaceMaxQPS` / `taskSchedulerGlobalMaxQPS` |
| `--settle-s` / `--max-drain-s` / `--stall-s` | 75 / 900 / 180 | phase lengths; a stall is no parent finishing **and** no task completing |

Other commands: `calibrate`, `indicator --window 120` (the playbook's readings right now), `reset` (remove the
test's dynamic-config block).
