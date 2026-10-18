"""Worker-performance test: reproduce a Worker bottleneck, diagnose it from metrics, verify the fix.

Follows Temporal's worker tuning guidance:
  https://docs.temporal.io/develop/worker-tuning-reference
  https://docs.temporal.io/develop/worker-performance/runtime-tuning
  https://github.com/temporalio/skill-temporal-workertuning  (Recipe 2: diagnosing high schedule-to-start)

    python -m lab.workerperf list                              # the scenarios
    python -m lab.workerperf run --scenario slots              # reproduce (exit 0 = issue surfaced AND diagnosed)
    python -m lab.workerperf run --scenario slots --fix        # recommended fix (exit 0 = healthy)
    python -m lab.workerperf run --scenario slots --worker-env MAX_CONCURRENT_ACTIVITIES=50 --expect healthy

How it works
------------
1. Starts the WORKER UNDER TEST: the lab's own `python -m lab.worker` (the same env knobs as .env's W_*),
   as 1..N child processes, on a fresh task queue `perf-<run-id>`, metrics on :9001.. (Prometheus scrapes
   them as job="loadgen" when the container is named `loadgen`, which `make workerperf` does).
2. Drives a fixed, open-loop workload for --duration seconds and records start->result latency.
3. Every second scrapes each worker's /metrics and reads its CPU and RSS from /proc.
4. Walks Recipe 2 of the worker tuning skill on the measured window and prints the branch it lands on:

     cache      cache evicting AND replays costly               (Recipe 4: raise max_cached_workflows)
     cpu        worker CPU saturated                            (Step 2a: scale out / up)
     blocking   event loop busy while CPU is low                (Python, checked BEFORE 2b-A: a blocking call in
                                                                 async code holds slots and looks like depletion)
     slots      executor slots depleted, CPU low                (Step 2b-A: raise slots)
     pollers    slots free, CPU low, slow link to the server    (Step 2b-B/D: more pollers / autoscaling)
     ratelimit  slots free, CPU low, a rate limit is set        (Step 2b-C: remove/raise the limit)

Without --fix the test passes when the diagnosis matches the scenario. With --fix (or --expect healthy) it
passes when no rule fires and schedule-to-start p95 stays under the alert threshold.
"""
import argparse
import asyncio
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import defaultdict
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

from lab import common

TOXIPROXY = os.environ.get("TOXIPROXY_URL", "http://toxiproxy:8474")
WAN_PROXY = "frontend_wan"          # toxiproxy proxy that puts latency between the worker and the frontend
WAN_LISTEN = "toxiproxy:17233"
METRICS_BASE = 9001                 # Prometheus job "loadgen" scrapes loadgen:9001-9004

# Thresholds (skill: Recipe 1 alert table and troubleshooting.md)
S2S_ALERT_MS = 200.0                # schedule-to-start p95 above this = elevated (alert threshold is p99 > 200 ms)
CPU_SATURATED = 0.8                 # cores per worker process; a GIL-bound Python process tops out near 1.0
SLOTS_DEPLETED = 0.3                # fraction of samples with 0 slots available
CACHE_MISS_RATIO = 0.2              # misses / (hits + misses)
REPLAY_COSTLY_MS = 5.0              # replay p95; the skill: "high if it exceeds a few milliseconds"
WFT_COSTLY_MS = 50.0                # workflow task execution p95. In the Python SDK (1.33) replay_latency stayed
                                    # < 1 ms while full replays made workflow tasks 400x slower, so use both
SLOW_LINK_MS = 50.0                 # RespondWorkflowTaskCompleted p50. The SDK's request-latency buckets are
                                    # 50/100/500/... ms, so it can only tell "under 50 ms" from "over 50 ms"
LOOP_BUSY = 0.8                     # worker_event_loop_busy_ratio p50: loop busy most of the time...
                                    # ...while process CPU is low = blocked on synchronous I/O
LOOP_LAG_MS = 250.0                 # worker_event_loop_lag_ms p95 (lab/worker.py probe). Idle ~5-15 ms; heavy cache
                                    # churn alone reached ~120 ms; a blocking call in async code ~530 ms
WFT_EXEC_SLOW_MS = 200.0            # workflow task execution p95

# ----------------------------------------------------------------------------------
# Scenarios. `bad` / `fix` are worker env vars (lab/worker.py), plus two test-level keys:
#   workers  number of worker processes          latency_ms  added RTT between worker and frontend
# `fix_params` overrides workflow params for the fix (code-level fixes).
# ----------------------------------------------------------------------------------
SCENARIOS: Dict[str, dict] = {
    "slots": dict(
        title="Activity slots too few for I/O-bound activities",
        workflow="ChainWorkflow", params={"n": 2, "spec": {"work_ms": 500}}, rate=20.0,
        design_ms=500,
        bad={"MAX_CONCURRENT_ACTIVITIES": "5"},
        fix={"MAX_CONCURRENT_ACTIVITIES": "100"},
        fix_note="slots >= rate x duration x headroom: 40 activities/s x 0.5 s = 20 in flight; 100 leaves 5x headroom",
    ),
    "pollers": dict(
        title="Too few pollers for a slow link to the server",
        workflow="ChainWorkflow", params={"n": 1, "spec": {"work_ms": 0}}, rate=30.0,
        design_ms=0, latency_ms=50,
        bad={"MAX_CONCURRENT_WORKFLOW_TASK_POLLS": "2", "MAX_CONCURRENT_ACTIVITY_TASK_POLLS": "1"},
        fix={"POLLER_AUTOSCALE": "1"},
        fix_note="PollerBehaviorAutoscaling: the poller count follows demand (server >= 1.28)",
    ),
    "blocking": dict(
        title="Blocking call inside an async activity (event loop stalled)",
        workflow="ChainWorkflow", params={"n": 1, "spec": {"work_ms": 0, "block_ms": 100}}, rate=12.0,
        design_ms=100, bad={}, fix={},
        fix_params={"call": {"activity": "work_sync"}},
        fix_note="run blocking code in a sync `def` activity (thread pool) or use an async client",
        asyncio_debug=True,
    ),
    "cpu": dict(
        title="CPU-bound activities saturate one Python process (GIL)",
        workflow="ChainWorkflow", params={"n": 1, "call": {"activity": "work_sync"}, "spec": {"work_ms": 0, "cpu_ms": 40}},
        rate=40.0, design_ms=40,
        bad={"workers": "1"}, fix={"workers": "3"},
        fix_note="one Python process ~ one core for Python code: run more worker processes / pods",
    ),
    "cache": dict(
        title="Sticky cache smaller than the number of open workflows",
        workflow="LongLivedWorkflow", population=100, rate=10.0,
        params={"iterations": 1000, "sleep_ms": 1000, "spec": {"work_ms": 10}},
        design_ms=10,
        bad={"MAX_CACHED_WORKFLOWS": "10"},
        fix={"MAX_CACHED_WORKFLOWS": "1000"},
        fix_note="max_cached_workflows >= open workflows per worker (memory permitting)",
    ),
    "ratelimit": dict(
        title="Task-queue activity rate limit throttles dispatch",
        workflow="ChainWorkflow", params={"n": 1, "spec": {"work_ms": 50}}, rate=20.0,
        design_ms=50,
        bad={"MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND": "5"},
        fix={},
        fix_note="remove max_task_queue_activities_per_second (or set it above the real demand)",
    ),
}


def deep_merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in over.items():
        out[k] = deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


# ----------------------------------------------------------------------------------
# Prometheus text format + histogram maths
# ----------------------------------------------------------------------------------
_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+(\S+)$')
_LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')

Sample = Dict[Tuple[str, frozenset], float]


def parse_metrics(text: str) -> Sample:
    out: Sample = {}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        m = _LINE.match(line)
        if not m:
            continue
        try:
            v = float(m.group(4))
        except ValueError:
            continue
        labels = frozenset(_LABEL.findall(m.group(3) or ""))
        out[(m.group(1), labels)] = v
    return out


def _match(labels: frozenset, want: Dict[str, str]) -> bool:
    d = dict(labels)
    return all(d.get(k) == v for k, v in want.items())


def counter(s: Sample, name: str, **want) -> float:
    return sum(v for (n, l), v in s.items() if n == name and _match(l, want))


def gauges(s: Sample, name: str, **want) -> List[float]:
    return [v for (n, l), v in s.items() if n == name and _match(l, want)]


def buckets(s: Sample, name: str, **want) -> Dict[float, float]:
    out: Dict[float, float] = defaultdict(float)
    for (n, l), v in s.items():
        if n == f"{name}_bucket" and _match(l, want):
            le = dict(l).get("le", "+Inf")
            out[math.inf if le == "+Inf" else float(le)] += v
    return out


def quantile(q: float, b: Dict[float, float]) -> Optional[float]:
    """Prometheus-style histogram_quantile over cumulative buckets."""
    if not b:
        return None
    les = sorted(b)
    total = b[les[-1]]
    if total <= 0:
        return None
    rank = q * total
    prev_le, prev_c = 0.0, 0.0
    for le in les:
        c = b[le]
        if c >= rank:
            if math.isinf(le):
                return prev_le
            if c == prev_c:
                return le
            return prev_le + (le - prev_le) * (rank - prev_c) / (c - prev_c)
        prev_le, prev_c = le, c
    return prev_le


def bucket_delta(a: Dict[float, float], b: Dict[float, float]) -> Dict[float, float]:
    return {le: b.get(le, 0.0) - a.get(le, 0.0) for le in b}


def pct(vals: List[float], p: float) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    return s[max(0, min(len(s) - 1, math.ceil(p / 100.0 * len(s)) - 1))]


def fmt(x: Optional[float], w: int = 8, d: int = 1) -> str:
    return f"{'-':>{w}}" if x is None else f"{x:>{w}.{d}f}"


# ----------------------------------------------------------------------------------
# worker processes
# ----------------------------------------------------------------------------------
class WorkerProc:
    def __init__(self, idx: int, env: Dict[str, str], log_path: str) -> None:
        self.idx = idx
        self.port = METRICS_BASE + idx
        self.log_path = log_path
        full = dict(os.environ)
        for k in list(full):  # start from a clean slate: only the scenario's knobs apply
            if k in ("MAX_CONCURRENT_WORKFLOW_TASKS", "MAX_CONCURRENT_ACTIVITIES", "MAX_CONCURRENT_WORKFLOW_TASK_POLLS",
                     "MAX_CONCURRENT_ACTIVITY_TASK_POLLS", "MAX_CACHED_WORKFLOWS", "ACTIVITY_THREADS",
                     "STICKY_QUEUE_TIMEOUT_S", "MAX_ACTIVITIES_PER_SECOND", "MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND",
                     "TUNER", "POLLER_AUTOSCALE", "DEBUG_MODE"):
                full.pop(k)
        full.update(env)
        full["METRICS_PORT"] = str(self.port)
        full.setdefault("LOG_LEVEL", "WARNING")
        self.log = open(log_path, "w")
        self.proc = subprocess.Popen([sys.executable, "-m", "lab.worker"], env=full, stdout=self.log,
                                     stderr=subprocess.STDOUT)
        self._ticks = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100

    def cpu_seconds(self) -> Optional[float]:
        try:
            with open(f"/proc/{self.proc.pid}/stat") as f:
                parts = f.read().rsplit(")", 1)[1].split()
            return (int(parts[11]) + int(parts[12])) / self._ticks  # utime + stime
        except (OSError, IndexError, ValueError):
            return None

    def rss_bytes(self) -> Optional[int]:
        try:
            with open(f"/proc/{self.proc.pid}/statm") as f:
                return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        except (OSError, IndexError, ValueError):
            return None

    def scrape(self) -> Sample:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/metrics", timeout=3) as r:
                return parse_metrics(r.read().decode())
        except Exception:  # noqa: BLE001
            return {}

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.log.close()


# ----------------------------------------------------------------------------------
# toxiproxy: a slow link between worker and frontend (pollers scenario)
# ----------------------------------------------------------------------------------
def _toxi(method: str, path: str, body: Optional[dict] = None) -> Tuple[int, str]:
    req = urllib.request.Request(f"{TOXIPROXY}{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def wan_on(latency_ms: int) -> str:
    code, _ = _toxi("GET", f"/proxies/{WAN_PROXY}")
    if code == 404:
        code, body = _toxi("POST", "/proxies", {"name": WAN_PROXY, "listen": "0.0.0.0:17233",
                                                  "upstream": "frontend:7233", "enabled": True})
        if code >= 300:
            raise RuntimeError(f"toxiproxy: cannot create {WAN_PROXY}: {code} {body}")
    _toxi("DELETE", f"/proxies/{WAN_PROXY}/toxics/wan_latency")
    code, body = _toxi("POST", f"/proxies/{WAN_PROXY}/toxics",
                       {"name": "wan_latency", "type": "latency", "stream": "downstream",
                        "attributes": {"latency": latency_ms, "jitter": 0}})
    if code >= 300:
        raise RuntimeError(f"toxiproxy: cannot add latency: {code} {body}")
    return WAN_LISTEN


def wan_off() -> None:
    _toxi("DELETE", f"/proxies/{WAN_PROXY}/toxics/wan_latency")


# ----------------------------------------------------------------------------------
# run
# ----------------------------------------------------------------------------------
async def task_queue_stats(client, tq: str) -> dict:
    """DescribeTaskQueue stats (backlog, rates) per task-queue type, plus the effective rate limit."""
    from temporalio.api.enums.v1 import TaskQueueType
    from temporalio.api.taskqueue.v1 import TaskQueue
    from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

    out = {}
    for name, t in (("workflow", TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW),
                    ("activity", TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY)):
        try:
            r = await client.workflow_service.describe_task_queue(DescribeTaskQueueRequest(
                namespace=client.namespace, task_queue=TaskQueue(name=tq), task_queue_type=t, report_stats=True,
                report_config=True))
            st = r.stats
            erl = r.effective_rate_limit if r.HasField("effective_rate_limit") else None
            out[name] = dict(
                backlog=st.approximate_backlog_count,
                backlog_age_s=st.approximate_backlog_age.ToTimedelta().total_seconds(),
                add_rate=round(st.tasks_add_rate, 1), dispatch_rate=round(st.tasks_dispatch_rate, 1),
                pollers=len(r.pollers),
                rate_limit=(round(erl.requests_per_second, 1) if erl and erl.requests_per_second else None),
            )
        except Exception as e:  # noqa: BLE001
            out[name] = dict(error=f"{type(e).__name__}: {e}"[:120])
    return out


async def run(cfg: dict) -> int:
    sc = SCENARIOS[cfg["scenario"]]
    mode = "fix" if cfg["fix"] else "issue"
    knobs = dict(sc["fix"] if cfg["fix"] else sc["bad"])
    for kv in cfg["worker_env"] or []:
        k, _, v = kv.partition("=")
        knobs[k] = v
    expect = cfg["expect"] or ("healthy" if cfg["fix"] else "issue")
    params = deep_merge(sc["params"], sc.get("fix_params", {})) if cfg["fix"] else dict(sc["params"])
    if cfg["params"]:
        params = deep_merge(params, json.loads(cfg["params"]))
    nworkers = int(knobs.pop("workers", "1"))
    latency_ms = int(knobs.pop("latency_ms", sc.get("latency_ms", 0)))
    rate = cfg["rate"] or sc["rate"]
    run_id = cfg["run_id"] or f"workerperf-{cfg['scenario']}-{mode}-{time.strftime('%H%M%S')}-{uuid.uuid4().hex[:4]}"
    tq = f"perf-{run_id}"
    os.makedirs("results", exist_ok=True)

    worker_env = {k: str(v) for k, v in knobs.items()}
    worker_env.update(TASK_QUEUE=tq, ROLE="all", TEMPORAL_NAMESPACE=common.NAMESPACE)
    if latency_ms:
        worker_env["TEMPORAL_ADDRESS"] = wan_on(latency_ms)
    if sc.get("asyncio_debug"):
        worker_env["PYTHONASYNCIODEBUG"] = "1"  # logs "Executing <Task ...> took N seconds" for slow callbacks

    print(f"workerperf run_id={run_id} scenario={cfg['scenario']} ({sc['title']})  mode={mode}  expect={expect}")
    print(f"worker under test: {nworkers} x `python -m lab.worker`  task_queue={tq}  knobs={knobs or '{} (SDK defaults)'}"
          + (f"  +{latency_ms} ms to the frontend (toxiproxy)" if latency_ms else ""))
    pop = sc.get("population")
    print(f"workload: {sc['workflow']} params={json.dumps(params)}  "
          + (f"{pop} long-lived workflows started at {rate}/s" if pop else f"{rate} starts/s for {cfg['duration']} s"))
    if cfg["fix"]:
        print(f"fix: {sc['fix_note']}")

    workers = [WorkerProc(i, worker_env, f"results/{run_id}-worker{i}.log") for i in range(nworkers)]
    client = await common.connect(id_prefix="workerperf")
    try:
        # wait until every worker polls
        deadline = time.time() + 30
        while time.time() < deadline:
            if all(sum(gauges(w.scrape(), "temporal_num_pollers")) > 0 for w in workers):
                break
            if any(w.proc.poll() is not None for w in workers):
                raise RuntimeError("a worker exited at start; see " + ", ".join(w.log_path for w in workers))
            await asyncio.sleep(0.5)
        else:
            raise RuntimeError("workers did not start polling within 30 s")
        return await _drive(cfg, sc, params, rate, tq, run_id, workers, client, knobs, nworkers, latency_ms, expect)
    finally:
        for w in workers:
            w.stop()
        if latency_ms:
            wan_off()


async def _drive(cfg, sc, params, rate, tq, run_id, workers, client, knobs, nworkers, latency_ms, expect) -> int:
    dur, warm = cfg["duration"], cfg["warmup"]
    wall_start = time.time()   # for screenshots / Grafana time ranges
    errors: Dict[str, int] = defaultdict(int)
    pending: set = set()
    samples: List[Tuple[float, List[Sample], List[Optional[float]], List[Optional[int]]]] = []
    t0 = time.monotonic()
    t_end = t0 + dur
    pop = sc.get("population")

    started = 0

    async def one(i: int) -> None:
        nonlocal started
        # Start only. Awaiting thousands of results would hold thousands of long-polls and hit the frontend's
        # per-namespace concurrent long-poll limit (ConcurrentLimit), starving the worker's own history fetches.
        # Completions and start->close latency come from the worker's metrics instead.
        try:
            await client.start_workflow(sc["workflow"], params, id=f"{run_id}-{i}", task_queue=tq,
                                        execution_timeout=timedelta(seconds=dur + 300))
            started += 1
        except Exception as e:  # noqa: BLE001
            errors[f"start: {type(e).__name__}"] += 1

    def completed(sample_set: List[Sample]) -> float:
        return sum(counter(x, "temporal_workflow_completed") for x in sample_set)

    async def sampler() -> None:
        while True:
            ts = time.monotonic()
            scraped = await asyncio.gather(*(asyncio.to_thread(w.scrape) for w in workers))
            samples.append((ts, list(scraped), [w.cpu_seconds() for w in workers], [w.rss_bytes() for w in workers]))
            await asyncio.sleep(max(0.0, 1.0 - (time.monotonic() - ts)))

    async def live() -> None:
        last = 0.0
        while True:
            await asyncio.sleep(10)
            if len(samples) < 2:
                continue
            ts, sc_, cpu, _ = samples[-1]
            ts0, _, cpu0, _ = samples[max(0, len(samples) - 11)]
            cores = [(c - c0) / (ts - ts0) for c, c0 in zip(cpu, cpu0) if c is not None and c0 is not None]
            act = [min(gauges(x, "temporal_worker_task_slots_available", worker_type="ActivityWorker"), default=None)
                   for x in sc_]
            done = completed(sc_)
            print(f"  t={int(time.monotonic() - t0):>4}s  started={started}  completed={int(done)} (+{int(done - last)})  "
                  f"worker CPU={'/'.join(f'{c:.2f}' for c in cores)} cores  activity slots free={act}")
            last = done

    samp = asyncio.create_task(sampler())
    liv = asyncio.create_task(live())
    n_ops = pop if pop else int(rate * dur)
    for i in range(n_ops):
        await asyncio.sleep(max(0.0, t0 + i / rate - time.monotonic()))
        t = asyncio.create_task(one(i))
        pending.add(t)
        t.add_done_callback(pending.discard)
    await asyncio.sleep(max(0.0, t_end - time.monotonic()))
    tq_stats = await task_queue_stats(client, tq)   # at the end of the load window, before the drain
    window_end = len(samples) - 1
    if pending:
        await asyncio.wait(set(pending), timeout=30)
    unfinished = 0
    if pop:
        await terminate_all(client, tq)  # long-lived workload: the window is the measurement
    else:
        base = completed(samples[0][1])
        deadline = time.monotonic() + cfg["drain_s"]
        print(f"  load window over; draining (up to {cfg['drain_s']} s)")
        while time.monotonic() < deadline and completed(samples[-1][1]) - base < started:
            await asyncio.sleep(2)
        unfinished = int(started - (completed(samples[-1][1]) - base))
        if unfinished > 0:
            await terminate_all(client, tq)
    samp.cancel()
    liv.cancel()
    final = samples[-1]

    # ---- measurements over [warmup, end of load] ------------------------------------
    i0 = next((i for i, s in enumerate(samples) if s[0] >= t0 + warm), 0)
    a, b = samples[i0], samples[window_end]
    span = b[0] - a[0]
    m: dict = dict(window_s=round(span, 1))

    def hist(name: str, **want) -> Dict[float, float]:
        tot: Dict[float, float] = defaultdict(float)
        for sa, sb in zip(a[1], b[1]):
            for le, v in bucket_delta(buckets(sa, name, **want), buckets(sb, name, **want)).items():
                tot[le] += v
        return tot

    def cdelta(name: str, **want) -> float:
        return sum(counter(sb, name, **want) - counter(sa, name, **want) for sa, sb in zip(a[1], b[1]))

    m["wft_s2s_p95"] = quantile(0.95, hist("temporal_workflow_task_schedule_to_start_latency"))
    m["act_s2s_p95"] = quantile(0.95, hist("temporal_activity_schedule_to_start_latency"))
    m["act_exec_p50"] = quantile(0.50, hist("temporal_activity_execution_latency"))
    m["act_exec_p95"] = quantile(0.95, hist("temporal_activity_execution_latency"))
    m["wft_exec_p95"] = quantile(0.95, hist("temporal_workflow_task_execution_latency"))
    m["replay_p95"] = quantile(0.95, hist("temporal_workflow_task_replay_latency"))
    # A pure server round trip (no history download): the link latency the pollers live with.
    m["rpc_p50"] = quantile(0.50, hist("temporal_request_latency", operation="RespondWorkflowTaskCompleted"))
    m["poll_p50"] = quantile(0.50, hist("temporal_long_request_latency"))
    hits, miss = cdelta("temporal_sticky_cache_hit"), cdelta("temporal_sticky_cache_miss")
    m["cache_hits"], m["cache_misses"] = int(hits), int(miss)
    m["cache_miss_ratio"] = miss / (hits + miss) if hits + miss else None
    m["forced_evictions"] = int(cdelta("temporal_sticky_cache_total_forced_eviction"))
    m["poll_empty"] = int(cdelta("temporal_workflow_task_queue_poll_empty") + cdelta("temporal_activity_poll_no_task"))
    window = samples[i0:window_end + 1]
    lags = [v for _, ss, _, _ in window for x in ss for v in gauges(x, "worker_event_loop_lag_ms")]
    m["loop_lag_p95"], m["loop_lag_max"] = pct(lags, 95), (max(lags) if lags else None)
    busy = [v for _, ss, _, _ in window for x in ss for v in gauges(x, "worker_event_loop_busy_ratio")]
    m["loop_busy_p50"] = pct(busy, 50)
    m["cache_size_max"] = max((max(gauges(s, "temporal_sticky_cache_size"), default=0) for _, ss, _, _ in window for s in ss),
                              default=None)

    def slot_stats(wt: str) -> Tuple[Optional[float], Optional[float]]:
        vals = [v for _, ss, _, _ in window for s in ss for v in gauges(s, "temporal_worker_task_slots_available",
                                                                         worker_type=wt)]
        if not vals:
            return None, None
        return sum(1 for v in vals if v <= 0) / len(vals), sum(vals) / len(vals)

    m["wf_slots_zero"], m["wf_slots_avg"] = slot_stats("WorkflowWorker")
    m["act_slots_zero"], m["act_slots_avg"] = slot_stats("ActivityWorker")
    m["pollers"] = {pt: max((max(gauges(s, "temporal_num_pollers", poller_type=pt), default=0)
                             for _, ss, _, _ in window for s in ss), default=0)
                    for pt in ("workflow_task", "sticky_workflow_task", "activity_task")}
    cores = [(cb - ca) / span for ca, cb in zip(a[2], b[2]) if ca is not None and cb is not None]
    m["cpu_cores"] = [round(c, 2) for c in cores]
    m["rss_mb"] = [round(r / 2**20) for r in b[3] if r]
    # start->close latency of every workflow that completed after the warm-up (incl. the drain), from the worker
    e2e_b: Dict[float, float] = defaultdict(float)
    for sa, sb in zip(a[1], final[1]):
        for le, v in bucket_delta(buckets(sa, "temporal_workflow_endtoend_latency"),
                                  buckets(sb, "temporal_workflow_endtoend_latency")).items():
            e2e_b[le] += v
    m["e2e_p50"], m["e2e_p95"] = quantile(0.50, e2e_b), quantile(0.95, e2e_b)
    m["started"], m["unfinished"] = started, unfinished
    if unfinished and not pop:
        errors["unfinished after drain (terminated)"] = unfinished
    m["errors"] = dict(errors)
    m["task_queue"] = tq_stats

    # ---- report ------------------------------------------------------------------------
    print("\n" + "=" * 100)
    print(f"WORKER-PERF TEST  scenario={cfg['scenario']}  mode={'fix' if cfg['fix'] else 'issue'}  "
          f"window={m['window_s']} s (after {warm} s warm-up)")
    print("=" * 100)
    if not pop:
        print(f"workflows  started {m['started']}   start->close p50 {fmt(m['e2e_p50'], 0)} ms  p95 {fmt(m['e2e_p95'], 0)} ms"
              f"   problems {m['errors'] or '{}'}")
    print(f"step 1     worker CPU {m['cpu_cores']} cores/process (saturated >= {CPU_SATURATED})   RSS {m['rss_mb']} MB"
          f"   event loop busy p50 {fmt((m['loop_busy_p50'] or 0) * 100, 0).strip()}%  lag p95 "
          f"{fmt(m['loop_lag_p95'], 0).strip()} ms / max {fmt(m['loop_lag_max'], 0).strip()} ms")
    print(f"latency    schedule->start p95: workflow task {fmt(m['wft_s2s_p95'], 0)} ms   activity {fmt(m['act_s2s_p95'], 0)} ms"
          f"   (alert > {S2S_ALERT_MS:.0f} ms)")
    print(f"           execution: activity p50 {fmt(m['act_exec_p50'], 0)} / p95 {fmt(m['act_exec_p95'], 0)} ms "
          f"(designed {sc['design_ms']} ms)   workflow task p95 {fmt(m['wft_exec_p95'], 0)} ms   "
          f"replay p95 {fmt(m['replay_p95'], 0)} ms")
    print("           (SDK histograms are coarse, e.g. request/activity buckets 50/100/500/1000 ms: values in between "
          "are interpolated)")
    print(f"compute    slots available  workflow: avg {fmt(m['wf_slots_avg'], 0)}  at 0 {fmt((m['wf_slots_zero'] or 0) * 100, 0)}% of "
          f"samples   activity: avg {fmt(m['act_slots_avg'], 0)}  at 0 {fmt((m['act_slots_zero'] or 0) * 100, 0)}%")
    print(f"memory     sticky cache: max size {m['cache_size_max']}  hits {m['cache_hits']}  misses {m['cache_misses']}  "
          f"miss ratio {fmt((m['cache_miss_ratio'] or 0) * 100, 0)}%  forced evictions {m['forced_evictions']}")
    print(f"io         pollers (max) {m['pollers']}   RPC p50 {fmt(m['rpc_p50'], 0)} ms   long-poll p50 "
          f"{fmt(m['poll_p50'], 0)} ms   empty polls {m['poll_empty']}")
    for k, v in tq_stats.items():
        print(f"queue      {k:<8} {v}")

    diag, why = diagnose(m, sc, knobs, latency_ms)
    healthy = diag is None and all((x or 0) <= S2S_ALERT_MS for x in (m["wft_s2s_p95"], m["act_s2s_p95"])) \
        and not m["errors"]
    print("\ndiagnosis (Recipe 2: diagnosing high schedule-to-start)")
    for line in why:
        print(f"  {line}")
    print(f"  => {diag.upper() if diag else 'HEALTHY'}")

    if expect == "issue":
        passed = diag == cfg["scenario"]
        print(f"\nRESULT: {'PASS' if passed else 'FAIL'} (expect=issue: the `{cfg['scenario']}` problem must be "
              f"surfaced and diagnosed as such; got {diag or 'healthy'})")
    else:
        passed = healthy
        print(f"\nRESULT: {'PASS' if passed else 'FAIL'} (expect=healthy: no rule fires, schedule->start p95 <= "
              f"{S2S_ALERT_MS:.0f} ms, no errors)")
    logs = [w.log_path for w in workers]
    if sc.get("asyncio_debug"):
        slow = []
        for p in logs:
            with open(p) as f:
                slow += [ln.strip() for ln in f if "Executing <" in ln and "took" in ln]
        print(f"\nasyncio debug: {len(slow)} slow event-loop callbacks logged by the worker"
              + (f", e.g.\n  {slow[0][:220]}" if slow else ""))
    out = f"results/{run_id}.json"
    with open(out, "w") as f:
        json.dump(dict(cfg=cfg, scenario=sc, knobs=knobs, workers=nworkers, latency_ms=latency_ms, metrics=m,
                       wall_start=wall_start, wall_end=time.time(),
                       diagnosis=diag, healthy=healthy, passed=passed), f, indent=1, default=str)
    print(f"wrote {out}  (worker logs: {', '.join(logs)})")
    return 0 if passed else 1


async def terminate_all(client, tq: str) -> None:
    """Batch-terminate whatever is still open on the test's task queue (needs the internal worker service)."""
    from temporalio.api.batch.v1 import BatchOperationTermination
    from temporalio.api.workflowservice.v1 import StartBatchOperationRequest

    try:
        await client.workflow_service.start_batch_operation(StartBatchOperationRequest(
            namespace=client.namespace, visibility_query=f'TaskQueue = "{tq}" AND ExecutionStatus = "Running"',
            job_id=str(uuid.uuid4()), reason="workerperf cleanup",
            termination_operation=BatchOperationTermination()))
    except Exception as e:  # noqa: BLE001
        print(f"  (cleanup: batch terminate failed: {type(e).__name__}: {e}; "
              f"run: temporal workflow terminate --query 'TaskQueue=\"{tq}\"' --yes)")


def diagnose(m: dict, sc: dict, knobs: dict, latency_ms: int) -> Tuple[Optional[str], List[str]]:
    """Recipe 2 of the worker tuning skill, plus the cache recipe (4) and the Python event-loop case."""
    why: List[str] = []
    s2s = max(m["wft_s2s_p95"] or 0, m["act_s2s_p95"] or 0)
    elevated = s2s > S2S_ALERT_MS
    cpu = max(m["cpu_cores"] or [0])
    loop_lag, loop_busy = m["loop_lag_p95"] or 0, m.get("loop_busy_p50") or 0
    exec_slow = loop_busy >= LOOP_BUSY or loop_lag >= LOOP_LAG_MS or (m["wft_exec_p95"] or 0) > WFT_EXEC_SLOW_MS

    # Recipe 4: evictions only matter when the replays they force are expensive. Checked first because
    # costly replays burn CPU and workflow slots, which would otherwise be misread as steps 1 / 2b-A.
    evicting = m["forced_evictions"] > 0 and (m["cache_miss_ratio"] or 0) >= CACHE_MISS_RATIO
    costly = (m["replay_p95"] or 0) >= REPLAY_COSTLY_MS or (m["wft_exec_p95"] or 0) >= WFT_COSTLY_MS
    if evicting and costly:
        why.append(f"cache: {m['forced_evictions']} forced evictions, miss ratio {m['cache_miss_ratio']:.0%}, "
                   f"cache full at {m['cache_size_max']:.0f}, workflow task p95 {fmt(m['wft_exec_p95'], 0).strip()} ms / "
                   f"replay p95 {fmt(m['replay_p95'], 0).strip()} ms -> replays are expensive: sticky cache too small "
                   f"(Recipe 4)")
        return "cache", why
    if evicting:
        why.append(f"cache: {m['forced_evictions']} forced evictions (miss ratio {m['cache_miss_ratio']:.0%}) but replay "
                   f"workflow tasks stay fast (p95 {fmt(m['wft_exec_p95'], 0).strip()} ms) -> a SIDE EFFECT of open "
                   f"workflows piling up, "
                   f"not the bottleneck")
    else:
        why.append(f"cache: miss ratio {fmt((m['cache_miss_ratio'] or 0) * 100, 0).strip()}%, "
                   f"{m['forced_evictions']} forced evictions -> ok")

    why.append(f"schedule->start p95 {s2s:.0f} ms -> {'ELEVATED' if elevated else 'ok'} (threshold {S2S_ALERT_MS:.0f} ms)")
    if not elevated and not exec_slow:
        return None, why

    # Step 1: CPU
    if cpu >= CPU_SATURATED:
        why.append(f"step 1: worker CPU {cpu:.2f} cores >= {CPU_SATURATED} -> saturated")
        why.append("step 2a: scale out / up (Python: one process ~ one core for Python code)")
        return "cpu", why
    why.append(f"step 1: worker CPU {cpu:.2f} cores -> underutilized, go to step 2b")

    # Python pre-check, BEFORE 2b-A: a stalled asyncio loop keeps workflow-task slots occupied (the tasks wait for
    # the loop), so it looks exactly like slot depletion, and raising slots would not help.
    if exec_slow:
        why.append(f"python: event loop busy {loop_busy:.0%} of the time while the process uses {cpu:.2f} cores "
                   f"(lag p95 {loop_lag:.0f} ms, workflow task p95 {fmt(m['wft_exec_p95'], 0).strip()} ms) -> a blocking "
                   f"call is stalling the asyncio event loop (any depleted slots are held by tasks waiting for the loop)")
        return "blocking", why
    why.append(f"python: event loop busy {loop_busy:.0%}, lag p95 {loop_lag:.0f} ms -> loop is responsive")

    # 2b-A: slots
    for wt, zero in (("activity", m["act_slots_zero"]), ("workflow", m["wf_slots_zero"])):
        if (zero or 0) >= SLOTS_DEPLETED:
            why.append(f"step 2b-A: {wt} slots at 0 in {zero:.0%} of samples -> executor slots depleted")
            return "slots", why
    why.append("step 2b-A: slots available most of the time -> not slots")

    # 2b-B / D: pollers vs slow link
    if (m["rpc_p50"] or 0) > SLOW_LINK_MS:
        why.append(f"step 2b-B/D: RPC p50 {m['rpc_p50']:.0f} ms to the server with pollers {m['pollers']} "
                   f"-> each poller is held up by the link; too few pollers")
        return "pollers", why
    why.append(f"step 2b-B: RPC p50 {fmt(m['rpc_p50'], 0).strip()} ms -> link is fast, pollers keep up")

    # 2b-C: rate limits (check the worker options)
    limits = {k: v for k, v in knobs.items() if "PER_SECOND" in k}
    tq_limit = (m.get("task_queue", {}).get("activity") or {}).get("rate_limit")
    if limits or tq_limit:
        why.append(f"step 2b-C: rate limit in effect ({limits or ''} task-queue effective limit {tq_limit}/s) "
                   f"-> the limit throttles dispatch")
        return "ratelimit", why
    why.append("step 2b-C: no rate limits")
    return "unexplained", why


def cmd_list() -> int:
    for name, sc in SCENARIOS.items():
        print(f"{name:<10} {sc['title']}")
        fix = sc["fix"] or sc.get("fix_params") or "remove the bad setting"
        print(f"{'':<10} issue: {sc['bad'] or 'code-level (see params)'}   fix: {fix}  ({sc['fix_note']})")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(prog="lab.workerperf", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="list scenarios")
    r = sub.add_parser("run", help="run one scenario")
    r.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    r.add_argument("--fix", action="store_true", help="apply the scenario's recommended fix")
    r.add_argument("--worker-env", nargs="*", metavar="KEY=VALUE",
                   help="extra/overriding worker knobs (lab/worker.py env names), plus workers=N, latency_ms=N")
    r.add_argument("--params", default=None, help="JSON deep-merged into the workflow params")
    r.add_argument("--rate", type=float, default=None, help="override the scenario's start rate")
    r.add_argument("--duration", type=int, default=90, help="load window, seconds")
    r.add_argument("--warmup", type=int, default=15, help="seconds excluded from the measurements")
    r.add_argument("--drain-s", type=int, default=120)
    r.add_argument("--expect", choices=("issue", "healthy"), default=None,
                   help="default: issue without --fix, healthy with --fix")
    r.add_argument("--run-id", default=None)
    a = ap.parse_args()
    if a.cmd == "list":
        sys.exit(cmd_list())
    sys.exit(asyncio.run(run(vars(a))))


if __name__ == "__main__":
    main()
