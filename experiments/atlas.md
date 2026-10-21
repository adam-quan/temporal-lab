# Temporal Failure Atlas

One row per experiment. Fill **Predicted** BEFORE you run it; the rows where Predicted != Happened are the point.
The `results/chaos.log` file gives you exact fault timestamps; the `results/*.jsonl` files hold the ramp numbers.

- **Metric that revealed it**: the panel/query where the fault first became visible (and how many seconds after injection).
- **Why**: the internal mechanism, in terms of shards, task queues, sticky cache, transfer/timer queues, range ids, retries...
- **Mitigation**: config, code, or alert that would have prevented it or shortened detection.

| # | Fault | What I predicted | What happened | Metric that revealed it | Why (internal mechanism) | Mitigation |
|---|---|---|---|---|---|---|
| 3A-1 | Activity slots = 5 |  |  |  |  |  |
| 3A-2 | Workflow/activity pollers = 2 vs 50 vs autoscale |  |  |  |  |  |
| 3A-3 | Sticky cache = 5 |  |  |  |  |  |
| 3A-4 | CPU-bound workflow code (detector on / off) |  |  |  |  |  |
| 3A-5 | Worker bottlenecks via `make workerperf` (slots, cpu, blocking, pollers, cache, ratelimit; experiments/worker-performance.md) |  |  |  |  |  |
| 3B-1 | Task queue partitions 1 vs 4 vs 16 |  |  |  |  |  |
| 3B-2 | DB throttled (CPU cap, +latency) |  |  |  |  |  |
| 3B-3 | Hot key vs spread keys, more history hosts |  |  |  |  |  |
| 3B-3b | Hot shard: restart / signal / CAN loops, colocated IDs (`make hotshard`, experiments/hot-shard.md) |  |  |  |  |  |
| 3B-5 | History task processing: write-reject loop and task-scheduler limits (`make historytasks`, experiments/history-task-processing.md) |  |  |  |  |  |
| 3B-6 | Rejected database calls: persistence QPS limits, their traps, outcome A/C (`make persistenceqps`, experiments/history-persistence-qps.md) |  |  |  |  |  |
| 3B-7 | Shard IO concurrency: writes queuing inside shards, raise vs slow DB vs small pool (`make shardio`, experiments/shard-io-concurrency.md) |  |  |  |  |  |
| 3B-4 | numHistoryShards 1 vs 4 vs 512 |  |  |  |  |  |
| 3C-1 | Namespace RPS limit -> RESOURCE_EXHAUSTED |  |  |  |  |  |
| 3C-2 | Per-shard / per-namespace ceilings |  |  |  |  |  |
| 4-1 | kill -9 worker mid-activity (heartbeat 5 s vs none) |  |  |  |  |  |
| 4-2 | kill history host |  |  |  |  |  |
| 4-3 | kill matching |  |  |  |  |  |
| 4-4 | kill frontend (1 vs 2 replicas) |  |  |  |  |  |
| 4-5 | rolling restart of all services under load |  |  |  |  |  |
| 4-6 | DB paused 30 s / 2 min / 10 min |  |  |  |  |  |
| 4-7 | DB +500 ms latency |  |  |  |  |  |
| 4-8 | DB crash / cut / reset (failover analogue) |  |  |  |  |  |
| 4-9 | workers partitioned < and > workflow task timeout |  |  |  |  |  |
| 4-10 | history host isolated (range-id fencing) |  |  |  |  |  |
| 4-11 | non-determinism: v1 -> v2 -> rollback / reset / patch |  |  |  |  |  |
| 4-12 | poison pill workflow (raise / raise_after_activity / cpu / fail) |  |  |  |  |  |
| 4-13 | retry storm (no backoff, unlimited retries) |  |  |  |  |  |
| 4-14 | hanging activity, no heartbeat |  |  |  |  |  |
| 4-15 | unbounded history vs continue-as-new |  |  |  |  |  |
| 4-16 | oversized payloads (2 MB blob / 4 MB gRPC) |  |  |  |  |  |
| 4-17 | pending activity / child / signal limits |  |  |  |  |  |
| 4-18 | clock skew on a worker |  |  |  |  |  |

---

# Capacity model (fill in from Phase 2 and 3B/3C results)

> On **hardware**: ______ (CPU/RAM of the Docker host, container caps used),
> with **H** history replicas, **M** matching, **F** frontend, **numHistoryShards = S**, Postgres capped at ______,
> the cluster sustains **X** workflow starts/sec of shape ______ at p99 start latency under ______ ms and p99 e2e under ______ ms,
> bottlenecked by **Z** (evidence: ______).

| Shape | Shards | History replicas | DB cap | Sustained starts/s at knee | p99 rpc (ms) | p99 e2e (ms) | Bottleneck | Evidence (panel/metric) |
|---|---|---|---|---|---|---|---|---|
| noop | | | | | | | | |
| chain (n=5) | | | | | | | | |
| fanout (n=100) | | | | | | | | |
| signal (keys=20) | | | | | | | | |
| signal (keys=1) | | | | | | | | |

Scaling questions to answer with data: does doubling history replicas double throughput (which shape? which not, and why)? What does `S=1` vs `S=512` change? Cost of one activity in DB writes?

---

# Runbook: top five failure modes

Copy this block five times. Keep each to one page.

```
FAILURE MODE:            <name>
HOW IT PRESENTS:         <alert that fires / what the user sees>
FIRST 3 CHECKS:          1) <dashboard panel>  2) <CLI command>  3) <log line>
LIKELY CAUSES (ranked):  
IMMEDIATE MITIGATION:    <command; safe to run under load?>
PERMANENT FIX:           
HOW TO CONFIRM RECOVERY: <metric back to normal; make count RUN=... shows no Failed>
WHAT NOT TO DO:          <the tempting action that makes it worse>
```

Candidates: worker schedule-to-start SLO breach; non-determinism after deploy; DB latency cascade; poison-pill workflow task loop; history-size limit hit; RESOURCE_EXHAUSTED storm; hot workflow/shard.
