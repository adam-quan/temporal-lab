# Hot shard: produce it, investigate it, fix it

A runnable version of the playbook
[Hot Shard Detection & Remediation](https://github.com/tsurdilo/temporal-server-operations/blob/main/playbooks/hot-shard-detection-remediation.md).
The test is `lab/hotshard.py` (`make hotshard ARGS="..."`). It creates a hot shard on purpose, measures who gets hurt,
reads the playbook's detector panels from Prometheus, and prints a PASS/FAIL verdict. Run it once to reproduce the
problem, then again with `--expect healthy` after each fix to show the fix worked.

**Background, in three lines.** Every workflow ID belongs to exactly one history shard
(`farm.Fingerprint32(namespaceID + "_" + workflowID) % numHistoryShards + 1`). Each shard writes to the database
through one pipeline (`history.shardIOConcurrency`, default 1 on SQL). When one workload floods a shard, every other
workflow on that shard waits in the same line, including workflows from other namespaces.

## What the test does

```
 phase:      baseline (60 s)        hot (120 s)                    after (60 s)
 victims:    ──── 5/s "same-shard" IDs (mined to land on the hot shard) ───────────────►
             ──── 5/s "other-shards" IDs (control group) ──────────────────────────────►
 aggressor:                         ████ hammers the hot shard ████
```

* **Victims** start `NoopWorkflow` at a fixed, open-loop rate in two groups. The groups do identical work and differ
  only in the shard their IDs hash to. The test measures *start latency* (one `CreateWorkflowExecution` write on the
  victim's shard, no worker involved) and *start→result latency*.
* **The aggressor** (`--kind`) runs in its own process, with its own worker on its own task queue
  (`hotshard-aggressor`), so its workflow tasks never queue in front of the victims' tasks on the lab's `sdk-worker`.

| `--kind` | Playbook pattern | What it does |
|---|---|---|
| `restart` (default) | Kind 2: repeated starts / terminate-and-restart loops | start-with-`TERMINATE_EXISTING` loop on `--hot-ids` IDs |
| `signal` | Kind 2: hot key | signal-with-start flood on `--hot-ids` IDs |
| `can` | Kind 2: rapid ContinueAsNew | one `ChurnWorkflow` per hot ID that continues-as-new on every run |
| `colocate` | Kind 1: many workflow IDs on one shard | a new ID per start, every ID mined onto the hot shard |
| `none` | control | victims only |

**Why `--hot-ids` defaults to 8.** All operations on one workflow ID are serialized by that workflow's lock, so one ID
can only use the shard about as fast as a single database write completes. With the lab's local Postgres
(sub-millisecond commits), one ID is easy to detect but barely slows its neighbours: 1.2–1.4x in our runs. A production
database at 5–20 ms per write gives the shard proportionally less headroom, and there one ID is enough. Eight IDs on
one shard reproduces that saturation on a laptop. Use `--hot-ids 1` for the strict single-ID case.

**Verdict.**

* *Detected* means two things held during the hot phase: the playbook's rule (shard RPS max ≥ 5x p50), and the hottest
  shard reaching at least 2x its baseline RPS.
* *Harm* means the same-shard victims ran ≥ 2x slower than the control group (p50 of start or start→result latency).
* `--expect healthy` also fails if the same-shard victims got ≥ 2x slower than their own baseline. Without this
  check, a "fix" that slows everything equally would look like a success; see Fix B.

Exit codes: `0` = PASS, `1` = FAIL, `2` = INVALID (the aggressor did not run). Each run writes `results/<run-id>.json`.

---

## 0. Set up

```sh
make up && make smoke                      # the lab; smoke must be 12/12
docker compose build sdk-worker            # only if lab/ changed since the last `make up` (the test runs from the image)
```

The test reads `numHistoryShards` from the server, so it works at any shard count. The lab's database was created
with 512 shards (`make reset SHARDS=...` changes that, but **wipes all data**).

If another Temporal stack on your machine already uses ports 7233/8080/9090, change the host ports in `.env`
(`FRONTEND_HOST_PORTS`, `UI_HOST_PORT`, `PROM_HOST_PORT`, `GRAFANA_HOST_PORT`, `TOXIPROXY_HOST_PORT`). The test talks to
the cluster over the Docker network, so host ports do not affect it.

---

## 1. Produce the issue

```sh
# Kind 2: a few hot workflow IDs on one shard (start / restart loop)
make hotshard ARGS="run --kind restart"

# Kind 1: many workflows crowding one shard, at a fixed rate
make hotshard ARGS="run --kind colocate --aggressor-rate 130"
```

Each run takes about 4.5 minutes. Expect `RESULT: PASS (expect=hot ...)`. A real run of the first command
(server 1.32.0, 512 shards, one history host):

```
phase     victim group       n | start p50      p99      max |  e2e p50      p99
baseline  same-shard       300 |      12.4     20.5     26.3 |     37.3     50.6
baseline  other-shards     300 |      11.9     20.8     24.7 |     35.2     53.9
hot       same-shard       600 |      37.0    104.2    244.8 |    134.4    296.3     <- only the hot shard's neighbours
hot       other-shards     600 |       6.8     20.8     88.4 |     26.5     80.3
after     same-shard       300 |      12.3     29.7     36.7 |     36.8     54.6     <- recovers when the load stops
after     other-shards     300 |      12.0     19.9     29.6 |     35.1     47.5

aggressor (restart): ok=14561 (121/s)  errors={}

phase     | shard RPS p50      p99      max  max/p50 | shard IO sem p99 | DB write p99 | hist task p99
baseline  |           0.5      1.0     50.0    99.5x |           1.0 ms |       9.9 ms |       28.7 ms
hot       |           0.5      1.0    500.0   994.1x |          71.4 ms |      12.1 ms |       98.7 ms
after     |           0.5      1.0    200.0   398.9x |           1.0 ms |       9.9 ms |       28.0 ms

  detector  ... hottest shard vs baseline = 10.0x  -> HOT SHARD DETECTED
  impact    hot      same-shard / other-shards victim p50:  start 5.4x   start->result 5.1x
RESULT: PASS (expect=hot: the hot shard must be both detected and harmful)
```

Variations:

| Command | What it shows |
|---|---|
| `run --kind restart --hot-ids 1` | strict single hot ID: detected, but neighbours only ~1.4x slower on a fast local DB |
| `run --kind signal` | hot-key signal flood: shard RPS 50 → 1000, neighbours 3.9x slower (PASS) |
| `run --kind can --hot-ids 64` | continue-as-new loops: detected (shard RPS 50 → 500) but only 1.7x harm here, just under the 2x threshold. Each run waits on a worker round trip, so CAN loops push fewer writes than start loops |
| `run --kind colocate` (no rate cap) | worst case: the aggressor grows until the shard saturates (~150 starts/s); neighbours are 55x slower |
| `run --kind restart --victim-namespace lab-victim` | multi-tenant: victims in **another namespace** are hurt too (5.8x). First run `make cli ARGS="operator namespace create -n lab-victim --retention 24h"` |
| `run --kind none` | control: no aggressor, everything at 1.0x |

**Choosing `--aggressor-rate`:** run `--kind colocate` once without a cap and read the aggressor's ops/s. That is the
shard's ceiling on your machine (about 146/s here). Use about 90% of it. Fix C needs a fixed rate to compare fairly.

---

## 2. Show and investigate it (the playbook's steps)

Start a long hot phase in one terminal and investigate from another, as you would during a real incident:

```sh
make hotshard ARGS="run --kind restart --hot-s 900"
```

Grafana (http://localhost:3000, folder *Temporal Lab*) has the playbook's dashboards: **Temporal Server Dashboard** and
**Temporal Shard IO Concurrency Dashboard**. Each step below also gives the CLI equivalent.

### Step 1: Confirm a hot shard exists

Panel **Per-Shard Persistence RPS Distribution (Hot-Shard Detector)** (Temporal Server Dashboard), or:

```sh
make hotshard ARGS="detector --window 2m"
```
```promql
histogram_quantile(1.00, sum by (le) (rate(persistence_shard_rps_bucket{service_name="history"}[$__rate_interval])))   # max
histogram_quantile(0.50, sum by (le) (rate(persistence_shard_rps_bucket{service_name="history"}[$__rate_interval])))   # p50
```

What you see: the max line goes from 50 to 500 RPS (`restart`), or to 1000–2000 (`colocate`), while p50 stays near 0.5.
How to read it:

* The metric is a histogram of per-shard request rates. It has **no shard label**: it shows *that* some shard is hot,
  not *which* one (Step 5 finds it).
* It is emitted every **30 s**, so use windows of at least 1 minute.
* Values snap to bucket bounds (…, 50, 100, 200, 500, 1000, 2000, …).
* On a quiet cluster, p50 is close to zero and max/p50 is always large. Compare against your normal baseline, not just
  against p50. The test does this ("hottest shard vs baseline").

### Step 2: Read the distribution shape

| Shape | Meaning |
|---|---|
| max far above p50, p99 near p50 | one or a few hot shards; usually a hot workflow ID (Kind 2) |
| p99 and max far above p50 | many hot shards: too few shards for the workload (Kind 1) |
| whole distribution shifted up | every shard crowded: too few shards overall |

Every `--kind` here makes **one** shard hot, so all of them show the first shape, `colocate` included. Shape tells you
*how many* shards are hot; Step 4 tells you whether it is one ID or many IDs.

### Step 3: Confirm write contention (SQL)

Shard IO Concurrency Dashboard → **Shard IO Semaphore Latency** and **Shard IO Semaphore Failures**:

```promql
histogram_quantile(0.99, sum by (le) (rate(semaphore_latency_bucket{service_name="history",operation="ShardInfo"}[$__rate_interval])))
```

Semaphore p99 goes from 1 ms to 70 ms (`restart`) and to 0.4–2 s (`colocate`). Meanwhile **DB write p99 stays flat at
~10 ms**. That pairing tells a hot shard apart from a slow database: the waiting happens in front of the shard's write
pipeline, not inside Postgres.

### Step 4: Identify the driver

* Temporal Server Dashboard → **Persistence Requests per Namespace and Operation** (namespace `lab`): in a `restart`
  run, `CreateWorkflowExecution`, `UpdateWorkflowExecution` and `GetWorkflowExecution` each go from ~10/s to ~80–90/s.
  This identifies the namespace and the operation mix (starts plus updates means a start or restart loop).
* Find the workflow IDs from visibility:

  ```sh
  make hotshard ARGS="top --since 10m"
  ```

  It lists the IDs with the most runs (restart, continue-as-new and retry loops), the IDs with the longest histories
  (signal or update floods), and the shards with the most starts compared with the mean. One ID with thousands of runs
  is Kind 2. Thousands of distinct IDs piled onto one shard is Kind 1. Example after three runs (it scanned 12,921
  executions in ~30 s):

  ```
  top workflow IDs by number of runs (restart / continue-as-new / retry loops):
        190  hotshard-restart-184522-a60a-hot-x-2436  (shard 384)
  top workflow IDs by history length (signal / update floods, huge loops):
       3549  hotshard-signal-184922-7bfe-hot  (shard 256)
  top shards by executions started (mean 25.2 per shard):
    shard    97:    4785  (190x mean)          <- the continue-as-new run
  ```

  It scans visibility, so keep `--since` short on a busy production cluster. The shard numbers use the lab's hash,
  which matches `tdbg` on 1.32.0.

### Step 5: Get the exact shard

```sh
make hotshard ARGS="shard <workflow-id>"            # computes it, and prints the tdbg command
make cli-tdbg ARGS="history-host get-shardid --namespace-id <ns-id> --workflow-id <id> --number-of-shards 512"
docker compose exec admin-tools tdbg --address frontend:7233 shard describe --shard-id <n>   # owner host, queue state
```

The lab's hash was checked against `tdbg history-host get-shardid` on 1.32.0, and they agree.

---

## 3. Fix it

Change `config/dynamicconfig/docker.yaml` (commented examples are in its "Hot shard" section). The file reloads within
5 s; the history log prints `dynamic config changed for the key: ...`. Then re-run the **same** command with
`--expect healthy`. Make one change at a time, and revert it before trying the next.

Measured in this lab (victim p50 slowdown, same-shard vs control group):

| Fix | Scenario | Before | After | Cost / caveat |
|---|---|---|---|---|
| A. same-ID start throttle | `restart` | 5.4x | **1.1x** | aggressor 121/s → 4/s (`BusyWorkflow`) |
| B. per-shard namespace cap | `restart --victim-namespace lab-victim` | 5.8x | **1.2x** | the capped namespace's own workflows slow down **everywhere** |
| C. `shardIOConcurrency` 1 → 4 | `colocate --aggressor-rate 130` | 7.1x | **1.0x** | does not help against an uncapped workload |
| D. more shards | (not run: needs a wipe) | | | fixed at cluster creation |

### Fix A: throttle repeated starts of the same ID (Kind 2, no app change)

```yaml
history.enableWorkflowIdReuseStartTimeValidation:
  - value: true
    constraints: {namespace: lab}
history.workflowIdReuseMinimalInterval:
  - value: 2s
    constraints: {namespace: lab}
```
```sh
make hotshard ARGS="run --kind restart --expect healthy"       # PASS: 1.1x, aggressor errors: RESOURCE_EXHAUSTED:BusyWorkflow
```

Starts of the same ID closer together than the interval are rejected with `RESOURCE_EXHAUSTED` / `BusyWorkflow`.
Eight IDs × one start per 2 s = 4 starts/s, so the hot shard dropped from 500 to 100 RPS. The server counted ~46
rejections/s (`service_errors_resource_exhausted{resource_exhausted_cause="BusyWorkflow"}`). The aggressor, which has
SDK retries disabled, saw only ~3 errors/s, and each of its calls took seconds: most rejections are retried inside
the server before they reach the client. A normal SDK client also retries `RESOURCE_EXHAUSTED`, so real callers
mostly experience *slow starts*, not failures.
It only covers **starts** (reuse or `TERMINATE_EXISTING`). It does nothing for signal floods or continue-as-new loops;
those need the application fixes below.

### Fix B: cap one namespace's DB rate per shard (multi-tenant guardrail)

```yaml
history.persistencePerShardNamespaceMaxQPS:
  - value: 100            # the playbook suggests starting at 500; the lab's hot shard only reaches ~500
    constraints: {namespace: lab}
```
```sh
make hotshard ARGS="run --kind restart --victim-namespace lab-victim --expect healthy"   # PASS: other namespace protected
make hotshard ARGS="run --kind restart --expect healthy"                                  # FAIL: see below
```

This protects **other** namespaces: victims in `lab-victim` went from 5.8x to 1.2x, and the aggressor was cut from
116/s to 25/s (`PersistenceLimit`). Know the cost before using it. When the victims share the capped namespace, the
second command fails hard: start→result p50 went from 38 ms to **35 s** for *every* `lab` workflow, on every shard,
with timeouts. The capped namespace's history tasks wait on the rate limiter while holding that namespace's share of
the history host's task scheduler, so the whole namespace stalls (history task p99 ~5 s). Cap only the namespace that
causes the problem, and only to protect other namespaces.

### Fix C: raise per-shard write concurrency (Kind 1, SQL only)

```yaml
history.shardIOConcurrency:
  - value: 4
```
```sh
docker compose restart history        # read when a shard is loaded: the 5 s reload is not enough
make hotshard ARGS="run --kind colocate --aggressor-rate 130 --expect healthy"   # PASS: 1.0x (was 7.1x at concurrency 1)
```

With the same 130 starts/s, semaphore p99 fell from 357 ms to 1.6 ms and the neighbours were unaffected. Two caveats:

* **Uncapped load eats the headroom.** Without `--aggressor-rate`, the aggressor went from 146/s to 321/s and the
  neighbours were *still* 17x slower. More capacity does not fix an unbounded workload; pair it with Fix A/B or an
  application fix.
* **Each extra concurrent write needs a DB connection.** Check the Shard IO dashboard panel *Max Safe
  shardIOConcurrency Per Pod* (`maxConns ÷ shards per pod`) and the DB health row before raising it. In this lab, 512
  shards on one history host with `maxConns: 20` gives far less than 1, so the panel advises against it; under a
  cluster-wide load the connection pool would become the next bottleneck.

### Fix D: add shards (Kind 1, permanent)

`numHistoryShards` is fixed when the cluster is created. In production, adding shards means a new cluster and a
migration. In the lab, `make reset SHARDS=<n>` **deletes all data**. Use it for experiments 3B-4 in `EXPERIMENTS.md`,
not as part of this test.

### Application fixes (Kind 2, the permanent ones)

| Aggressor pattern | Fix in code | Lab equivalent to compare against |
|---|---|---|
| terminate-and-restart / repeated starts (`restart`) | one long-running workflow fed by Signal-With-Start / Update-With-Start | `lg ramp --shape signal --keys 1000` |
| continue-as-new every run (`can`) | continue-as-new only when `workflow.info().is_continue_as_new_suggested()` | `LongLivedWorkflow` with `can_mode=suggested` |
| signal floods on one ID (`signal`) | batch signals client-side, or spread over more IDs | `--shape signal --keys 1` vs `--keys 1000` |
| polling `DescribeWorkflowExecution` | wait on the result (`handle.result()` / `GetWorkflow(...).Get`) | n/a |
| workflow retry policies | let activities retry; spread retry backoff | n/a |

---

## 4. Things this test showed that the playbook does not say

1. **One hot ID needs a slow database to hurt.** A single ID is serialized by its workflow lock. On a fast database it
   is very visible on the detector (10x shard RPS) but cheap for its neighbours. The less write headroom a shard has,
   the less it takes.
2. **The blast radius can reach beyond the shard.** In the uncapped `colocate` run, the *control group's*
   start→result p50 went from 36 ms to 7.5 s, although its starts stayed fast. The hot shard's tasks waited on the
   shard semaphore while holding the history host's shared task-processing workers. If your "other shards" are also
   slow, look for a hot shard on the same history host.
3. **A detector-only view can mislead after a fix.** Compare victims against their own baseline, not only against
   other shards. That is how Fix B's namespace-wide damage shows up.
4. **The detector can go quiet under DB stress.** With +2 ms of toxiproxy latency on every DB round trip,
   `persistence_shard_rps` stopped emitting for about 4 minutes. Alert on the metric being absent, not just on it
   being high.

---

## Clean up

* The aggressor terminates its hot workflows when the hot phase ends. Victim and `colocate` workflows complete on
  their own.
* Revert `config/dynamicconfig/docker.yaml`. If you changed `shardIOConcurrency`, also run `docker compose restart history`.
* Optionally: `make cli ARGS="operator namespace delete -n lab-victim --yes"`.

## Options

`make hotshard ARGS="run --help"` lists them all. The ones you will use:

| Option | Default | Meaning |
|---|---|---|
| `--kind` | `restart` | aggressor pattern (table above) |
| `--hot-ids` | 8 | hot IDs for restart/signal/can, all on one shard |
| `--aggressor-rate` | 0 (max) | cap on aggressor ops/s; set it when comparing fixes |
| `--concurrency` | 64 | aggressor loops |
| `--victim-namespace` | `lab` | put victims in another namespace (multi-tenant case) |
| `--victim-rate` | 5 | victim starts/s per group |
| `--baseline-s` / `--hot-s` / `--after-s` | 60 / 120 / 60 | phase lengths (≥ 60: the detector is emitted every 30 s) |
| `--expect` | `hot` | `hot` = reproduce; `healthy` = verify a fix |
| `--impact-ratio` / `--detect-ratio` | 2 / 5 | verdict thresholds |
