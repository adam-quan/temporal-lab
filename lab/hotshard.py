"""Hot-shard test: reproduce, detect and verify fixes for a hot history shard.

Follows the playbook
https://github.com/tsurdilo/temporal-server-operations/blob/main/playbooks/hot-shard-detection-remediation.md

    python -m lab.hotshard run --kind restart                   # reproduce (exit 0 = hot shard surfaced)
    python -m lab.hotshard run --kind restart --expect healthy  # after a fix (exit 0 = no more impact)
    python -m lab.hotshard detector --window 5m                 # the playbook's detector, right now
    python -m lab.hotshard top --since 10m                      # busiest workflow IDs / shards (steps 4-5)
    python -m lab.hotshard shard <workflow-id>                  # which shard owns an ID (same as tdbg get-shardid)

How the test works
------------------
Every workflow ID maps to one history shard: farm.Fingerprint32(namespaceID + "_" + workflowID) % N + 1.
A shard has one write pipeline (history.shardIOConcurrency, default 1 on SQL), so whatever overloads
the shard slows every other workflow on it. The test makes that visible with two victim groups:

    same-shard victims   unique workflow IDs chosen so they land on the HOT shard
    other-shard victims  unique workflow IDs on any other shard (the control group)

Both groups start NoopWorkflow at the same open-loop rate for three phases:

    baseline  victims only
    hot       victims + an AGGRESSOR hammering the hot shard
    after     victims only (the aggressor stopped)

Aggressor kinds (--kind). The first three are the playbook's "Kind 2: hot workflow ID" patterns:

    restart   tight start loop with TERMINATE_EXISTING (terminate-and-restart loop)
    signal    signal-with-start flood (hot key)
    can       a workflow that continues-as-new on every run (ContinueAsNew loop)
    colocate  a new workflow ID per start, every ID mined to hash to the hot shard (Kind 1 in miniature:
              the "too many workflows per shard" effect, concentrated on one shard)
    none      no aggressor (control run)

restart/signal/can drive --hot-ids IDs (default 8), all mined onto the same shard. Every operation on one
workflow ID is serialized by its workflow lock, so on the lab's sub-millisecond local Postgres a single ID
cannot saturate a shard (--hot-ids 1 is detected but barely hurts neighbours). With production DB latency
(5-20 ms per write) one ID can. The aggressor runs in its own process with its own worker on its own task
queue, so its workflow tasks never queue in front of the victims' on the lab's sdk-worker.

The victim metric is the StartWorkflowExecution latency measured from the intended send time
(one CreateWorkflowExecution write on the victim's shard, no worker involved), plus start->result
latency when the victims are awaited. At the end the test queries Prometheus for the playbook's panels
(per-shard persistence RPS distribution, shard IO semaphore latency) for each phase and prints a verdict.
"""
import argparse
import asyncio
import itertools
import json
import math
import multiprocessing as mp
import os
import sys
import time
import urllib.parse
import urllib.request
import uuid
from collections import Counter, defaultdict
from datetime import timedelta
from typing import Dict, Iterator, List, Optional

from lab import common

PROM_URL = os.environ.get("PROM_URL", "http://prometheus:9090")
KINDS = ("restart", "signal", "can", "colocate", "none")

# The playbook's queries. W is replaced by the window, e.g. "120s".
Q_SHARD_RPS = 'histogram_quantile({q}, sum by (le) (increase(persistence_shard_rps_bucket{{service_name="history"}}[{w}])))'
Q_SEM_P99 = ('histogram_quantile(0.99, sum by (le) (rate(semaphore_latency_bucket'
             '{{service_name="history",operation="ShardInfo"}}[{w}])))')
Q_SEM_FAIL = 'sum(rate(semaphore_failures{{service_name="history",operation="ShardInfo"}}[{w}]))'
Q_DB_P99 = ('histogram_quantile(0.99, sum by (le) (rate(persistence_latency_bucket'
            '{{service_name="history",operation=~"CreateWorkflowExecution|UpdateWorkflowExecution"}}[{w}])))')
Q_TASK_P99 = 'histogram_quantile(0.99, sum by (le) (rate(task_latency_queue_bucket{{service_name="history"}}[{w}])))'
Q_RES_EXH = ('sum by (resource_exhausted_cause) (rate(service_errors_resource_exhausted'
             '{{service_name=~"frontend|history"}}[{w}]))')


# ----------------------------------------------------------------------------------
# shard mapping
# ----------------------------------------------------------------------------------
def _fp32():
    try:
        import farmhash  # pyfarmhash
    except ImportError:
        sys.exit("pyfarmhash is required for shard mapping (it is installed in the lab image)")
    return farmhash.fingerprint32


def shard_of(namespace_id: str, workflow_id: str, num_shards: int, fp32=None) -> int:
    """Mirrors the server (verified against `tdbg history-host get-shardid`)."""
    return (fp32 or _fp32())(f"{namespace_id}_{workflow_id}") % num_shards + 1


def ids_on_shard(namespace_id: str, prefix: str, num_shards: int, want: int, same: bool) -> Iterator[str]:
    """Endless stream of unique workflow IDs whose shard is (same=True) / is not (same=False) `want`."""
    fp32 = _fp32()
    for i in itertools.count():
        wf_id = f"{prefix}-{i}"
        if (shard_of(namespace_id, wf_id, num_shards, fp32) == want) == same:
            yield wf_id


async def cluster_facts(client) -> dict:
    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest, GetClusterInfoRequest

    info = await client.workflow_service.get_cluster_info(GetClusterInfoRequest())
    ns = await client.workflow_service.describe_namespace(DescribeNamespaceRequest(namespace=client.namespace))
    return dict(num_shards=info.history_shard_count, namespace_id=ns.namespace_info.id,
                server_version=info.server_version)


# ----------------------------------------------------------------------------------
# stats helpers
# ----------------------------------------------------------------------------------
def pct(vals: List[float], p: float) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    return s[max(0, min(len(s) - 1, math.ceil(p / 100.0 * len(s)) - 1))]


def err_label(e: BaseException) -> str:
    status = getattr(e, "status", None)
    name = type(e).__name__
    if status is not None and hasattr(status, "name"):
        name = f"{name}:{status.name}"
    msg = str(e)
    if "Too many" in msg and ("starts" in msg or "restarts" in msg):
        name += ":BusyWorkflow"
    return name


def fmt(x: Optional[float], w: int = 8, d: int = 1) -> str:
    return f"{'-':>{w}}" if x is None else f"{x:>{w}.{d}f}"


def ratio(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b <= 0:
        return None
    return a / b


# ----------------------------------------------------------------------------------
# aggressor (separate process, so its client work never delays the victims' timers)
# ----------------------------------------------------------------------------------
async def _with_worker(namespace: str, task_queue: str, t_end: float, body=None) -> None:
    """Run a workflow worker on (namespace, task_queue) until t_end, optionally alongside `body`."""
    from temporalio.worker import Worker

    from lab.workflows import ALL_WORKFLOWS

    from temporalio.runtime import LoggingConfig, Runtime, TelemetryConfig, TelemetryFilter

    # The restart loop terminates runs while their workflow tasks are in flight, so the SDK would log a
    # "Task not found when completing" warning per restart. Expected here; keep only errors.
    quiet = Runtime(telemetry=TelemetryConfig(logging=LoggingConfig(
        filter=TelemetryFilter(core_level="ERROR", other_level="ERROR"))))
    client = await common.connect(namespace=namespace, id_prefix=f"hotshard-worker-{task_queue}", runtime=quiet)
    worker = Worker(client, task_queue=task_queue, workflows=ALL_WORKFLOWS,
                    identity=common.identity(f"hotshard-worker-{task_queue}"))
    worker_task = asyncio.create_task(worker.run())
    try:
        if body is not None:
            await body
        await asyncio.sleep(max(0.0, t_end - time.time()))  # keep draining the queue
    finally:
        await worker.shutdown()
        await asyncio.gather(worker_task, return_exceptions=True)


async def _aggressor(cfg: dict, t_start: float, t_stop: float, t_end: float, q) -> None:
    """Hammer the hot shard from t_start to t_stop. Its own worker (own task queue) runs until t_end,
    so the aggressor's workflow tasks never queue in front of the victims' on the shared worker."""
    await _with_worker(cfg["namespace"], cfg["task_queue"], t_end, _hammer(cfg, t_start, t_stop, q))


def worker_main(namespace: str, task_queue: str, t_end: float) -> None:
    asyncio.run(_with_worker(namespace, task_queue, t_end))


async def _hammer(cfg: dict, t_start: float, t_stop: float, q) -> None:
    from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
    from temporalio.service import RetryConfig

    # No client-side retries: a guardrail rejection (RESOURCE_EXHAUSTED / BusyWorkflow) is counted
    # instead of being hidden as retry latency inside the SDK.
    client = await common.connect(namespace=cfg["namespace"], id_prefix="hotshard-aggressor",
                                  retry_config=RetryConfig(max_retries=0))
    kind, hot_ids, tq = cfg["kind"], cfg["hot_ids_list"], cfg["task_queue"]
    ok, err = 0, Counter()
    timeout = timedelta(seconds=cfg["hot_s"] + cfg["after_s"] + 120)  # leak guard

    async def terminate_all() -> None:
        for wf_id in hot_ids:
            try:
                await client.get_workflow_handle(wf_id).terminate(reason="hotshard test: hot phase over")
            except Exception:  # noqa: BLE001
                pass

    await asyncio.sleep(max(0.0, t_start - time.time()))

    if kind == "can":
        # One continue-as-new loop per hot ID; the loop itself is driven by the aggressor's worker.
        for wf_id in hot_ids:
            try:
                await client.start_workflow("ChurnWorkflow", {}, id=wf_id, task_queue=tq, execution_timeout=timeout,
                                            id_conflict_policy=WorkflowIDConflictPolicy.TERMINATE_EXISTING)
                ok += 1
            except Exception as e:  # noqa: BLE001
                err[err_label(e)] += 1
        await asyncio.sleep(max(0.0, t_stop - time.time()))
        await terminate_all()
        q.put(dict(ok=ok, err=dict(err), note="(ok = ChurnWorkflow chains started; their CAN rate shows in shard RPS)"))
        return

    coloc_ids = ids_on_shard(cfg["namespace_id"], f"{cfg['run_id']}-coloc", cfg["num_shards"], cfg["hot_shard"], True)

    async def one(wf_id: str) -> None:
        if kind == "restart":
            await client.start_workflow(
                "SignalSinkWorkflow", {}, id=wf_id, task_queue=tq, execution_timeout=timeout,
                id_conflict_policy=WorkflowIDConflictPolicy.TERMINATE_EXISTING,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE)
        elif kind == "signal":
            await client.start_workflow(
                "SignalSinkWorkflow", {"can_on_suggest": True}, id=wf_id, task_queue=tq, execution_timeout=timeout,
                start_signal="push", start_signal_args=[""])
        elif kind == "colocate":
            await client.start_workflow("NoopWorkflow", {}, id=next(coloc_ids), task_queue=tq,
                                        execution_timeout=timedelta(seconds=60))

    # Optional rate cap shared by all loops (0 = as fast as the server allows). Use a fixed rate to compare
    # a fix fairly: an uncapped aggressor simply grows into any capacity the fix adds.
    rate = cfg["aggressor_rate"]
    pace = {"next": time.time()}

    async def wait_turn() -> None:
        if rate <= 0:
            return
        t = max(pace["next"], time.time() - 1.0)  # no catch-up burst after a stall
        pace["next"] = t + 1.0 / rate
        await asyncio.sleep(max(0.0, t - time.time()))

    async def loop(i: int) -> None:
        nonlocal ok
        wf_id = hot_ids[i % len(hot_ids)] if hot_ids else ""  # colocate picks a fresh ID per start
        while time.time() < t_stop:
            await wait_turn()
            try:
                await one(wf_id)
                ok += 1
            except Exception as e:  # noqa: BLE001
                err[err_label(e)] += 1
                await asyncio.sleep(0.01)  # do not spin on a hard error

    async def reporter() -> None:
        while time.time() < t_stop:
            await asyncio.sleep(5)
            q.put(dict(progress=True, ok=ok, err=sum(err.values())))

    rep = asyncio.create_task(reporter())
    await asyncio.gather(*(loop(i) for i in range(cfg["concurrency"])))
    rep.cancel()
    if kind in ("restart", "signal"):
        await terminate_all()
    q.put(dict(ok=ok, err=dict(err)))


def aggressor_main(cfg: dict, t_start: float, t_stop: float, t_end: float, q) -> None:
    try:
        asyncio.run(_aggressor(cfg, t_start, t_stop, t_end, q))
    except BaseException as e:  # noqa: BLE001
        q.put(dict(fatal=f"{type(e).__name__}: {e}"))
        raise


# ----------------------------------------------------------------------------------
# Prometheus
# ----------------------------------------------------------------------------------
def prom(expr: str, at: Optional[float] = None) -> Dict[str, float]:
    """Instant query -> {label-string: value}. Empty dict on any error (the test still reports latencies)."""
    params = {"query": expr}
    if at is not None:
        params["time"] = f"{at:.3f}"
    url = f"{PROM_URL}/api/v1/query?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.load(r)["data"]["result"]
    except Exception as e:  # noqa: BLE001
        print(f"  (prometheus query failed: {type(e).__name__}: {e})", file=sys.stderr)
        return {}
    out = {}
    for s in data:
        v = float(s["value"][1])
        if not math.isnan(v):
            out[",".join(f"{k}={v}" for k, v in s["metric"].items()) or "value"] = v
    return out


def prom1(expr: str, at: Optional[float] = None) -> Optional[float]:
    r = prom(expr, at)
    return next(iter(r.values()), None) if r else None


def detector(window_s: int, at: Optional[float] = None) -> dict:
    w = f"{int(window_s)}s"
    p50 = prom1(Q_SHARD_RPS.format(q="0.50", w=w), at)
    p99 = prom1(Q_SHARD_RPS.format(q="0.99", w=w), at)
    mx = prom1(Q_SHARD_RPS.format(q="1.00", w=w), at)
    sem = prom1(Q_SEM_P99.format(w=w), at)
    db = prom1(Q_DB_P99.format(w=w), at)
    tq = prom1(Q_TASK_P99.format(w=w), at)
    return dict(
        shard_rps_p50=p50, shard_rps_p99=p99, shard_rps_max=mx, max_over_p50=ratio(mx, p50),
        shard_io_sem_p99_ms=None if sem is None else sem * 1000.0,
        shard_io_sem_failures_per_s=prom1(Q_SEM_FAIL.format(w=w), at),
        db_write_p99_ms=None if db is None else db * 1000.0,
        history_task_p99_ms=None if tq is None else tq * 1000.0,
        resource_exhausted_per_s=prom(Q_RES_EXH.format(w=w), at),
    )


def shape_of(d: dict) -> str:
    """The playbook's Step 2: read the distribution shape."""
    p50, p99, mx = d["shard_rps_p50"], d["shard_rps_p99"], d["shard_rps_max"]
    if None in (p50, p99, mx) or p50 <= 0:
        return "no data (persistence_shard_rps is emitted every 30s; use windows >= 60s)"
    if mx / p50 < 3:
        return "flat: no hot shard"
    if p99 / p50 < 3:
        return ("max >> p50, p99 ~ p50: ONE (or a few) hot shards. Playbook: usually a single hot workflow ID "
                "(Kind 2); Step 4 confirms whether it is one ID or many IDs on that shard")
    return "p99 and max >> p50: MANY hot shards -> too few shards for the workload (Kind 1)"


# ----------------------------------------------------------------------------------
# run
# ----------------------------------------------------------------------------------
async def run(cfg: dict) -> int:
    from temporalio.common import TypedSearchAttributes, SearchAttributeKey, SearchAttributePair

    agg_client = await common.connect(namespace=cfg["namespace"], id_prefix="hotshard")
    vic_client = (agg_client if cfg["victim_namespace"] == cfg["namespace"]
                  else await common.connect(namespace=cfg["victim_namespace"], id_prefix="hotshard-victim"))
    agg_facts = await cluster_facts(agg_client)
    vic_facts = await cluster_facts(vic_client)
    n = agg_facts["num_shards"]
    run_id = cfg["run_id"]
    hot_id = cfg["hot_id"] or f"{run_id}-hot"
    hot_shard = shard_of(agg_facts["namespace_id"], hot_id, n)
    victim_await = cfg["victim_await"] if cfg["victim_await"] is not None else True
    # Extra hot IDs are mined so they land on the SAME shard as the first one.
    extra = ids_on_shard(agg_facts["namespace_id"], f"{hot_id}-x", n, hot_shard, True)
    hot_ids = [hot_id] + [next(extra) for _ in range(max(0, cfg["hot_ids"] - 1))] if cfg["kind"] != "colocate" else []
    cfg.update(hot_id=hot_id, hot_ids_list=hot_ids, hot_shard=hot_shard, num_shards=n,
               namespace_id=agg_facts["namespace_id"])

    print(f"hotshard run_id={run_id} kind={cfg['kind']} server={agg_facts['server_version']} numHistoryShards={n}")
    print(f"aggressor: namespace={cfg['namespace']} ({agg_facts['namespace_id']}) hot workflow id={hot_id!r} "
          f"-> HOT SHARD {hot_shard}")
    print(f"           hot ids={len(hot_ids) or 'n/a (colocate: a new ID per start)'}  concurrency={cfg['concurrency']}  "
          f"task_queue={cfg['task_queue']}  rate={cfg['aggressor_rate'] or 'max'}")
    print(f"victims:   namespace={cfg['victim_namespace']} rate={cfg['victim_rate']}/s per group "
          f"(same-shard + other-shards)  task_queue={cfg['victim_task_queue']}  await_result={victim_await}")
    print(f"phases:    baseline {cfg['baseline_s']}s -> hot {cfg['hot_s']}s -> after {cfg['after_s']}s   "
          f"prometheus={PROM_URL}")
    print(f"verify the shard: tdbg history-host get-shardid --namespace-id {agg_facts['namespace_id']} "
          f"--workflow-id {hot_id} --number-of-shards {n}\n")

    vic_ns_id = vic_facts["namespace_id"]
    gen = {
        "same-shard": ids_on_shard(vic_ns_id, f"{run_id}-v-same", n, hot_shard, True),
        "other-shards": ids_on_shard(vic_ns_id, f"{run_id}-v-other", n, hot_shard, False),
    }
    phases = [("baseline", cfg["baseline_s"]), ("hot", cfg["hot_s"]), ("after", cfg["after_s"])]
    t0 = time.time() + 5.0  # time for the worker processes to spawn and connect
    bounds = {}
    t = t0
    for name, dur in phases:
        bounds[name] = (t, t + dur)
        t += dur

    # aggressor process
    ctx = mp.get_context("spawn")
    aq = ctx.Queue()
    proc = None
    if cfg["kind"] != "none":
        proc = ctx.Process(target=aggressor_main, args=(dict(cfg), *bounds["hot"], bounds["after"][1], aq), daemon=True)
        proc.start()

    # Victims in another namespace (multi-tenant case) get a worker here; the lab's sdk-worker only
    # serves namespace `lab`.
    vproc = None
    if cfg["victim_namespace"] != cfg["namespace"] and victim_await:
        vproc = ctx.Process(target=worker_main, daemon=True,
                            args=(cfg["victim_namespace"], cfg["victim_task_queue"], bounds["after"][1] + 70))
        vproc.start()

    start_ms: Dict[tuple, List[float]] = defaultdict(list)
    e2e_ms: Dict[tuple, List[float]] = defaultdict(list)
    errs: Dict[tuple, Counter] = defaultdict(Counter)
    window: Dict[str, List[float]] = defaultdict(list)  # live view, reset every 5 s
    pending: set = set()
    sa = None
    if cfg["victim_namespace"] != cfg["namespace"]:
        cfg["no_search_attrs"] = True  # LabRunId/LabShape are only registered in namespace `lab`
    if not cfg["no_search_attrs"]:
        sa = TypedSearchAttributes([SearchAttributePair(SearchAttributeKey.for_keyword("LabRunId"), run_id),
                                    SearchAttributePair(SearchAttributeKey.for_keyword("LabShape"), "hotshard")])

    def phase_at(ts: float) -> str:
        for name, (a, b) in bounds.items():
            if a <= ts < b:
                return name
        return "after"

    async def victim(group: str, intended_wall: float, intended_mono: float) -> None:
        ph = phase_at(intended_wall)
        kw = dict(task_queue=cfg["victim_task_queue"], execution_timeout=timedelta(seconds=60))
        if sa is not None:
            kw["search_attributes"] = sa
        try:
            h = await vic_client.start_workflow("NoopWorkflow", {}, id=next(gen[group]), **kw)
        except Exception as e:  # noqa: BLE001
            errs[(ph, group)][err_label(e)] += 1
            return
        ms = (time.monotonic() - intended_mono) * 1000.0
        start_ms[(ph, group)].append(ms)
        window[group].append(ms)
        if victim_await:
            try:
                await asyncio.wait_for(h.result(), timeout=60)
                e2e_ms[(ph, group)].append((time.monotonic() - intended_mono) * 1000.0)
            except Exception as e:  # noqa: BLE001
                errs[(ph, group)]["result:" + err_label(e)] += 1

    async def live() -> None:
        agg_ok = agg_err = 0
        while True:
            await asyncio.sleep(5)
            while not aq.empty():
                m = aq.get_nowait()
                if m.get("progress"):
                    agg_ok, agg_err = m["ok"], m["err"]
                else:
                    aq.put(m)  # final result: leave it for the end
                    break
            ph = phase_at(time.time())
            s, o = window["same-shard"], window["other-shards"]
            print(f"[{time.strftime('%H:%M:%S')}] {ph:<8} victim start p50/p99 ms  same-shard "
                  f"{fmt(pct(s, 50), 7)} /{fmt(pct(s, 99), 7)}   other-shards {fmt(pct(o, 50), 7)} /{fmt(pct(o, 99), 7)}"
                  + (f"   | aggressor ops={agg_ok} err={agg_err}" if proc else ""))
            window.clear()

    live_task = asyncio.create_task(live())
    interval = 1.0 / cfg["victim_rate"]
    end = bounds["after"][1]
    mono0 = time.monotonic() + (t0 - time.time())
    for k in itertools.count():
        intended_mono = mono0 + k * interval
        intended_wall = t0 + k * interval
        if intended_wall >= end:
            break
        delay = intended_mono - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        for group in gen:
            task = asyncio.create_task(victim(group, intended_wall, intended_mono))
            pending.add(task)
            task.add_done_callback(pending.discard)
    if pending:
        await asyncio.wait(set(pending), timeout=70)
    live_task.cancel()

    agg = {}
    if proc is not None:
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                m = aq.get(timeout=1)
            except Exception:  # noqa: BLE001
                if not proc.is_alive():
                    break
                continue
            if not m.get("progress"):
                agg = m
                break
        proc.join(timeout=5)
    if vproc is not None:
        vproc.terminate()

    # ---- report ---------------------------------------------------------------
    print("\n" + "=" * 110)
    print(f"HOT-SHARD TEST  run_id={run_id}  kind={cfg['kind']}  hot shard={hot_shard}/{n}  hot id={hot_id}")
    print("=" * 110)
    print(f"{'phase':<9} {'victim group':<13} {'n':>6} | {'start p50':>9} {'p99':>8} {'max':>8} | "
          f"{'e2e p50':>8} {'p99':>8} | errors")
    rows = {}
    for ph, _ in phases:
        for group in gen:
            s, e = start_ms[(ph, group)], e2e_ms[(ph, group)]
            rows[(ph, group)] = dict(n=len(s), start_p50=pct(s, 50), start_p99=pct(s, 99), start_max=pct(s, 100),
                                     e2e_p50=pct(e, 50), e2e_p99=pct(e, 99), errors=dict(errs[(ph, group)]))
            r = rows[(ph, group)]
            print(f"{ph:<9} {group:<13} {r['n']:>6} | {fmt(r['start_p50'], 9)} {fmt(r['start_p99'])} "
                  f"{fmt(r['start_max'])} | {fmt(r['e2e_p50'])} {fmt(r['e2e_p99'])} | {r['errors'] or ''}")
    print("(ms, measured from the intended send time; same-shard victims share shard "
          f"{hot_shard} with the aggressor, other-shards victims are the control group)")

    if proc is not None:
        dur = cfg["hot_s"]
        print(f"\naggressor ({cfg['kind']}): ok={agg.get('ok')} ({(agg.get('ok') or 0) / dur:.0f}/s)  "
              f"errors={agg.get('err') or {}}  {agg.get('note') or ''}{agg.get('fatal') or ''}")

    # Prometheus, per phase. Skip the first 10 s of each phase (rate windows straddling the boundary).
    print(f"\nplaybook panels per phase ({PROM_URL}):")
    print(f"{'phase':<9} | {'shard RPS p50':>13} {'p99':>8} {'max':>8} {'max/p50':>8} | "
          f"{'shard IO sem p99':>16} {'sem fail/s':>10} | {'DB write p99':>12} | {'hist task p99':>13} | resource_exhausted/s")
    time.sleep(max(0.0, bounds["after"][1] + 6 - time.time()))  # let the last scrape land
    det = {}
    for ph, dur in phases:
        a, b = bounds[ph]
        det[ph] = d = detector(max(60, int(b - a - 10)), at=b)
        rex = {k.replace("resource_exhausted_cause=", ""): round(v, 1) for k, v in d["resource_exhausted_per_s"].items() if v > 0}
        print(f"{ph:<9} | {fmt(d['shard_rps_p50'], 13)} {fmt(d['shard_rps_p99'])} {fmt(d['shard_rps_max'])} "
              f"{fmt(d['max_over_p50'], 7)}x | {fmt(d['shard_io_sem_p99_ms'], 13)} ms {fmt(d['shard_io_sem_failures_per_s'], 10, 2)} | "
              f"{fmt(d['db_write_p99_ms'], 9)} ms | {fmt(d['history_task_p99_ms'], 10)} ms | {rex or ''}")
    print(f"shape during 'hot': {shape_of(det['hot'])}")

    # ---- verdict ----------------------------------------------------------------
    # Impact = how much slower the same-shard victims are than the control group, at the median
    # (p99 on a laptop is dominated by cluster-wide hiccups that hit both groups equally).
    def impact_of(ph: str) -> Dict[str, Optional[float]]:
        s_, o_ = rows[(ph, "same-shard")], rows[(ph, "other-shards")]
        return dict(start=ratio(s_["start_p50"], o_["start_p50"]), e2e=ratio(s_["e2e_p50"], o_["e2e_p50"]))

    def vs_baseline(group: str) -> Optional[float]:
        """How much slower a victim group got in the hot phase than in its own baseline (worst of start/e2e)."""
        h, b = rows[("hot", group)], rows[("baseline", group)]
        vals = [ratio(h["start_p50"], b["start_p50"]), ratio(h["e2e_p50"], b["e2e_p50"])]
        return max((v for v in vals if v is not None), default=None)

    imp = {ph: impact_of(ph) for ph, _ in phases}
    worst = max((v for v in imp["hot"].values() if v is not None), default=None)
    impacted = worst is not None and worst >= cfg["impact_ratio"]
    # A fix can also hurt BOTH groups (e.g. a namespace-wide throttle), which a same/other ratio cannot see.
    same_vs_base, other_vs_base = vs_baseline("same-shard"), vs_baseline("other-shards")
    regressed = same_vs_base is not None and same_vs_base >= cfg["impact_ratio"]
    wide = other_vs_base is not None and other_vs_base >= cfg["impact_ratio"]
    # Detection = the playbook's rule (max far above p50) AND the hottest shard got hotter than in the
    # baseline (on a quiet lab cluster the same-shard victims alone already make one shard the "max").
    det_ratio = det["hot"]["max_over_p50"]
    growth = ratio(det["hot"]["shard_rps_max"], det["baseline"]["shard_rps_max"])
    detected = (det_ratio is not None and det_ratio >= cfg["detect_ratio"]
                and growth is not None and growth >= 2.0)
    print("\nverdict")
    print(f"  detector  hot-phase shard RPS max/p50 = {fmt(det_ratio, 0)}x (threshold {cfg['detect_ratio']}x), "
          f"hottest shard vs baseline = {fmt(growth, 0, 1)}x (threshold 2x)  "
          f"-> {'HOT SHARD DETECTED' if detected else 'not detected'}")
    for ph, _ in phases:
        print(f"  impact    {ph:<8} same-shard / other-shards victim p50:  start {fmt(imp[ph]['start'], 0, 1)}x   "
              f"start->result {fmt(imp[ph]['e2e'], 0, 1)}x")
    print(f"            hot phase worst = {fmt(worst, 0, 1)}x (threshold {cfg['impact_ratio']}x)  "
          f"-> {'NEIGHBOURS ON THE HOT SHARD ARE SLOWED' if impacted else 'no neighbour impact'}")
    print(f"  vs base   hot phase / baseline victim p50 (worst of start, start->result):  "
          f"same-shard {fmt(same_vs_base, 0, 1)}x   other-shards {fmt(other_vs_base, 0, 1)}x")
    if wide:
        print("            the CONTROL group slowed too: the damage is not confined to the shard "
              "(shared history host / namespace scheduler / workers)")

    if proc is not None and (agg.get("fatal") or not agg.get("ok")):
        print(f"\nRESULT: INVALID (the aggressor did not run: {agg.get('fatal') or agg.get('err') or 'no result'})")
        return 2
    if cfg["expect"] == "hot":
        passed = detected and impacted
        print(f"\nRESULT: {'PASS' if passed else 'FAIL'} (expect=hot: the hot shard must be both detected and harmful)")
    else:
        passed = not impacted and not regressed
        print(f"\nRESULT: {'PASS' if passed else 'FAIL'} (expect=healthy: same-shard victims must not be "
              f"{cfg['impact_ratio']}x slower than the control group or than their own baseline; "
              f"the detector may still show the aggressor's traffic)")

    os.makedirs("results", exist_ok=True)
    out = os.path.join("results", f"{run_id}.json")
    with open(out, "w") as f:
        json.dump(dict(cfg=cfg, bounds=bounds, victims={f"{k[0]}/{k[1]}": v for k, v in rows.items()},
                       aggressor=agg, detector=det, impact=imp, same_vs_baseline=same_vs_base,
                       other_vs_baseline=other_vs_base, detected=detected, impacted=impacted, regressed=regressed,
                       passed=passed),
                  f, indent=1)
    print(f"wrote {out}")
    return 0 if passed else 1


async def cmd_shard(args) -> int:
    client = await common.connect(namespace=args.namespace, id_prefix="hotshard")
    f = await cluster_facts(client)
    s = shard_of(f["namespace_id"], args.workflow_id, f["num_shards"])
    print(f"namespace={args.namespace} namespace_id={f['namespace_id']} numHistoryShards={f['num_shards']}")
    print(f"workflow_id={args.workflow_id} -> history shard {s}")
    print(f"confirm: tdbg history-host get-shardid --namespace-id {f['namespace_id']} --workflow-id {args.workflow_id} "
          f"--number-of-shards {f['num_shards']}")
    return 0


def _seconds(w: str) -> int:
    w = w.strip().lower()
    return int(w[:-1]) * {"s": 1, "m": 60, "h": 3600}[w[-1]] if w[-1] in "smh" else int(w)


async def cmd_top(args) -> int:
    """Playbook Steps 4-5 from visibility: which workflow IDs / shards had the most activity recently."""
    from datetime import datetime, timezone

    client = await common.connect(namespace=args.namespace, id_prefix="hotshard-top")
    f = await cluster_facts(client)
    since = datetime.fromtimestamp(time.time() - _seconds(args.since), tz=timezone.utc)
    q = f'StartTime > "{since.strftime("%Y-%m-%dT%H:%M:%SZ")}"'
    runs: Counter = Counter()
    hist: Dict[str, int] = {}
    n = 0
    async for wf in client.list_workflows(q, limit=args.limit):
        n += 1
        runs[wf.id] += 1
        hist[wf.id] = max(hist.get(wf.id, 0), wf.history_length or 0)
    fp32 = _fp32()
    by_shard: Counter = Counter()
    for wf_id, c in runs.items():
        by_shard[shard_of(f["namespace_id"], wf_id, f["num_shards"], fp32)] += c
    print(f"namespace={args.namespace} executions started in the last {args.since}: {n}"
          f"{'  (hit --limit, counts are partial)' if n >= args.limit else ''}  numHistoryShards={f['num_shards']}")
    print("\ntop workflow IDs by number of runs (restart / continue-as-new / retry loops):")
    for wf_id, c in runs.most_common(args.top):
        print(f"  {c:>7}  {wf_id}  (shard {shard_of(f['namespace_id'], wf_id, f['num_shards'], fp32)})")
    print("\ntop workflow IDs by history length (signal / update floods, huge loops):")
    for wf_id, h in sorted(hist.items(), key=lambda kv: -kv[1])[: args.top]:
        print(f"  {h:>7}  {wf_id}  (shard {shard_of(f['namespace_id'], wf_id, f['num_shards'], fp32)})")
    mean = n / f["num_shards"]
    print(f"\ntop shards by executions started (mean {mean:.1f} per shard):")
    for shard, c in by_shard.most_common(args.top):
        print(f"  shard {shard:>5}: {c:>7}  ({c / mean if mean else 0:.0f}x mean)")
    print("\nnext: tdbg history-host get-shardid / tdbg shard describe --shard-id <n> (owner host)")
    return 0


def cmd_detector(args) -> int:
    d = detector(_seconds(args.window))
    print(f"last {args.window}:  shard RPS p50={fmt(d['shard_rps_p50'], 0)}  p99={fmt(d['shard_rps_p99'], 0)}  "
          f"max={fmt(d['shard_rps_max'], 0)}  max/p50={fmt(d['max_over_p50'], 0)}x")
    print(f"  shard IO semaphore p99={fmt(d['shard_io_sem_p99_ms'], 0)} ms  failures/s={fmt(d['shard_io_sem_failures_per_s'], 0, 2)}  "
          f"DB write p99={fmt(d['db_write_p99_ms'], 0)} ms")
    print(f"  resource_exhausted/s by cause: {d['resource_exhausted_per_s'] or '{}'}")
    print(f"  shape: {shape_of(d)}")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(prog="lab.hotshard", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="reproduce a hot shard and measure its blast radius")
    r.add_argument("--kind", choices=KINDS, default="restart")
    r.add_argument("--namespace", default=common.NAMESPACE, help="aggressor namespace")
    r.add_argument("--victim-namespace", default=None, help="victim namespace (default: same as --namespace)")
    r.add_argument("--task-queue", default="hotshard-aggressor",
                   help="aggressor task queue (served by a worker inside the aggressor process)")
    r.add_argument("--victim-task-queue", default=common.DEFAULT_TASK_QUEUE)
    r.add_argument("--concurrency", type=int, default=64, help="aggressor concurrent loops (not used by --kind can)")
    r.add_argument("--aggressor-rate", type=float, default=0.0,
                   help="cap on aggressor ops/s across all loops (0 = as fast as possible). Fix a rate to compare fixes")
    r.add_argument("--victim-rate", type=float, default=5.0, help="victim starts/s PER GROUP")
    r.add_argument("--victim-await", action=argparse.BooleanOptionalAction, default=None,
                   help="also wait for victim results (start->result latency). Default on; a victim namespace "
                        "other than `lab` gets a worker inside this process")
    r.add_argument("--baseline-s", type=int, default=60)
    r.add_argument("--hot-s", type=int, default=120)
    r.add_argument("--after-s", type=int, default=60)
    r.add_argument("--hot-id", default=None, help="the hot workflow ID (default <run-id>-hot); it picks the hot shard")
    r.add_argument("--hot-ids", type=int, default=8,
                   help="restart/signal/can: number of hot IDs, all mined onto the hot shard. 1 = strict single-ID "
                        "case (one ID is serialized by its workflow lock, so on a fast local DB it cannot saturate a shard)")
    r.add_argument("--detect-ratio", type=float, default=5.0, help="shard RPS max/p50 that counts as hot")
    r.add_argument("--impact-ratio", type=float, default=2.0, help="same/other victim p50 latency that counts as harm")
    r.add_argument("--expect", choices=("hot", "healthy"), default="hot")
    r.add_argument("--run-id", default=None)
    r.add_argument("--no-search-attrs", action="store_true")
    d = sub.add_parser("detector", help="print the playbook's hot-shard detector for the last window")
    d.add_argument("--window", default="5m")
    t = sub.add_parser("top", help="busiest workflow IDs and shards from visibility (playbook steps 4-5)")
    t.add_argument("--since", default="10m")
    t.add_argument("--namespace", default=common.NAMESPACE)
    t.add_argument("--limit", type=int, default=100_000, help="max executions to scan")
    t.add_argument("--top", type=int, default=5)
    s = sub.add_parser("shard", help="which history shard owns a workflow ID")
    s.add_argument("workflow_id")
    s.add_argument("--namespace", default=common.NAMESPACE)
    a = ap.parse_args()

    if a.cmd == "detector":
        sys.exit(cmd_detector(a))
    if a.cmd == "shard":
        sys.exit(asyncio.run(cmd_shard(a)))
    if a.cmd == "top":
        sys.exit(asyncio.run(cmd_top(a)))
    cfg = vars(a)
    cfg["victim_namespace"] = cfg["victim_namespace"] or cfg["namespace"]
    cfg["run_id"] = cfg["run_id"] or f"hotshard-{cfg['kind']}-{time.strftime('%H%M%S')}-{uuid.uuid4().hex[:4]}"
    for k in ("baseline_s", "hot_s", "after_s"):
        if cfg[k] < 60:
            ap.error(f"--{k.replace('_', '-')} must be >= 60 (persistence_shard_rps is emitted every 30 s)")
    sys.exit(asyncio.run(run(cfg)))


if __name__ == "__main__":
    main()
