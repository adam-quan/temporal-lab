"""History persistence QPS test: reproduce rejected database calls, diagnose them, verify the fix.

Follows the playbook
https://github.com/tsurdilo/temporal-server-operations/blob/main/playbooks/history-persistence-qps-limits.md
and its alert runbook 86 (History Database Calls Rejected).

    python -m lab.persistenceqps list
    python -m lab.persistenceqps run --scenario throttle     # limit too low: outcome A
    python -m lab.persistenceqps run --scenario raise        # the fix for A: compared with the last `throttle` run
    python -m lab.persistenceqps run --scenario nsequal      # 2.4 trap: namespace limit left at the old pod value
    python -m lab.persistenceqps run --scenario global       # 2.5 trap: a cluster-wide limit replaces the pod limit
    python -m lab.persistenceqps run --scenario zero         # 2.6 trap: per-pod 0 switches ALL limiting off
    python -m lab.persistenceqps run --scenario dbslow       # check 2: the database is the constraint: outcome C
    python -m lab.persistenceqps run --scenario backoff      # check 2's safety net: adaptive backoff acting
    python -m lab.persistenceqps reset                       # remove the test's dynamic-config block / DB latency

Every run: write the scenario's settings into a marked dynamic-config block, settle 75 s (the limiter re-reads
its quota once a minute), take a baseline, then a burst of CLIENT starts (NoopWorkflow, priority 1 in the limiter)
together with parent/child background work (child completions, priority 4), drain, and read the playbook's panels
from Prometheus. The report walks the four checks of section 4.5 and lands on an outcome of section 4.6:

            | client requests failing | none failing
    --------+-------------------------+-------------
    DB room |  A  raise the limit     |  B  your call
    no room |  C  the hard one        |  D  leave it
"""
import argparse
import asyncio
import glob
import json
import os
import sys
import time
import uuid
from collections import defaultdict
from datetime import timedelta
from typing import Dict, List, Optional

from lab import common
from lab.historytasks import H, apply_block, remove_block, terminate_open
from lab.hotshard import prom, prom1
from lab.workerperf import WorkerProc, _toxi, counter, pct

BEGIN = "# >>> lab.persistenceqps managed block: removed when the run ends (`make persistenceqps ARGS=reset` after a crash)"
END = "# <<< lab.persistenceqps"
TOXIC = "persistenceqps_db_latency"
# The lab sets system.forceSearchAttributesCacheRefreshOnRead: true (so new search attributes appear at once). That
# turns every search-attribute read into a GetCurrentClusterMetadata call, and a refused one is retried on the next
# read. It is not a production default, so it is excluded when comparing runs.
LAB_ARTIFACT_OPS = {"GetCurrentClusterMetadata"}

# Client-facing API methods (playbook check 4), as the frontend reports them.
CLIENT_OPS = {"StartWorkflowExecution", "SignalWorkflowExecution", "SignalWithStartWorkflowExecution",
              "UpdateWorkflowExecution", "RequestCancelWorkflowExecution", "TerminateWorkflowExecution"}
# Internal RPCs that carry workflow progress (check 4): no client error, but executions stall.
PROGRESS_OPS = {"RecordChildExecutionCompleted", "RecordActivityTaskStarted", "RecordWorkflowTaskStarted",
                "RespondWorkflowTaskCompleted", "RespondActivityTaskCompleted", "ScheduleWorkflowTask",
                "StartWorkflowExecution", "AddWorkflowTask", "AddActivityTask"}

SCENARIOS: Dict[str, dict] = {
    "throttle": dict(title="Limit too low for the workload (outcome A)",
                     settings={"history.persistenceMaxQPS": 300}, expect="A"),
    "raise": dict(title="The fix for A: raise the enforced limit (300 -> 1200)",
                  settings={"history.persistenceMaxQPS": 1200}, expect="raised"),
    "nsequal": dict(title="2.4 trap: pod limit raised to 1200, namespace limit left at the old value 300",
                    settings={"history.persistenceMaxQPS": 1200, "history.persistenceNamespaceMaxQPS": 300},
                    expect="namespace-binds"),
    "global": dict(title="2.5 trap: cluster-wide limit 300, per-pod limit 'raised' to 20000",
                   settings={"history.persistenceGlobalMaxQPS": 300, "history.persistenceMaxQPS": 20000},
                   expect="global-binds"),
    "zero": dict(title="2.6 trap: per-pod 0 and cluster-wide 0 set on a RUNNING pod (namespace limit 50)",
                 settings={"history.persistenceMaxQPS": 0, "history.persistenceGlobalMaxQPS": 0,
                           "history.persistenceNamespaceMaxQPS": 50}, expect="refuse-all"),
    "dbslow": dict(title="Check 2: the database is the constraint (limit 300, +DB latency): outcome C",
                   settings={"history.persistenceMaxQPS": 300}, db_latency_ms=3, expect="C"),
    "backoff": dict(title="Check 2 safety net: adaptive backoff under a raised limit with a slow database",
                    settings={"history.persistenceMaxQPS": 1200,
                              "history.persistenceDynamicRateLimitingParams": {
                                  "enabled": True, "latencyThreshold": 20.0, "errorThreshold": 0.05,
                                  "rateMultiMin": 0.2}},
                    db_latency_ms=3, expect="backed-off"),
}


# ----------------------------------------------------------------------------------
# database latency (toxiproxy sits between every Temporal service and Postgres)
# ----------------------------------------------------------------------------------
def db_latency_on(ms: int) -> None:
    _toxi("DELETE", f"/proxies/postgres/toxics/{TOXIC}")
    code, body = _toxi("POST", "/proxies/postgres/toxics", {"name": TOXIC, "type": "latency", "stream": "downstream",
                                                            "attributes": {"latency": ms, "jitter": 0}})
    if code >= 300:
        raise RuntimeError(f"toxiproxy: cannot add latency: {code} {body}")


def db_latency_off() -> None:
    _toxi("DELETE", f"/proxies/postgres/toxics/{TOXIC}")


# ----------------------------------------------------------------------------------
# readings: the playbook's panels as numbers, over a window
# ----------------------------------------------------------------------------------
def readings(w: int, at: float) -> dict:
    win = f"{w}s"

    def rate(expr: str) -> float:
        return prom1(f"sum(increase({expr}[{win}])) / {w}", at) or 0.0

    def by(expr: str, labels: str, minimum: float = 0.05) -> Dict[str, float]:
        """Keys are the label values joined by ',' in the order of `labels` (Prometheus sorts them otherwise)."""
        order = [x.strip() for x in labels.split(",")]
        out = {}
        for k, v in prom(f"sum by ({labels}) (increase({expr}[{win}])) / {w}", at).items():
            if v >= minimum:
                lv = dict(p.split("=", 1) for p in k.split(",") if "=" in p)
                out[",".join(lv.get(name, "") for name in order)] = round(v, 1)
        return dict(sorted(out.items(), key=lambda kv: -kv[1]))

    def q(quant: float, expr: str) -> Optional[float]:
        v = prom1(f"histogram_quantile({quant}, sum by (le) (rate({expr}[{win}])))", at)
        return None if v is None else v * 1000

    d: dict = {}
    rej = f"persistence_errors_resource_exhausted{{{H}}}"
    d["rejected"] = rate(rej)                                                     # 4.3: the real volume
    d["by_scope_cause"] = by(rej, "resource_exhausted_scope, resource_exhausted_cause")
    d["by_operation"] = dict(list(by(rej, "operation").items())[:6])
    d["requests"] = rate(f"persistence_requests{{{H}}}")
    d["reached"] = d["requests"] - d["rejected"]
    # 4.3: the two counters that read lower, for the same event
    d["requests_failed"] = rate('service_errors_resource_exhausted{resource_exhausted_cause="PersistenceLimit"}')
    d["task_attempts_throttled"] = rate(f'task_errors_throttled{{{H},resource_exhausted_cause="PersistenceLimit"}}')
    # 4.2: panels that stay clean while throttled
    errs = rate(f"persistence_errors{{{H}}}")
    d["availability_pct"] = 100.0 - (errs / d["requests"] * 100.0) if d["requests"] else None
    d["timer_task_errors"] = rate('task_errors{operation=~"TimerActive.*"}')
    # check 2: was the database struggling?
    upd = f'persistence_latency_bucket{{{H},operation="UpdateWorkflowExecution"}}'
    d["db_write_p95_ms"], d["db_write_p99_ms"] = q(0.95, upd), q(0.99, upd)
    d["db_all_p95_ms"] = q(0.95, f"persistence_latency_bucket{{{H}}}")
    # Only failures that say the store itself struggled. Condition-failed / not-found are normal answers.
    d["db_error_types"] = {k: v for k, v in by(f"persistence_error_with_type{{{H}}}", "error_type").items()
                           if any(s in k for s in ("Timeout", "Unavailable", "DeadlineExceeded"))}
    # check 3: the write-reject loop
    cl, cm = rate("workflow_context_cleared"), rate('cache_miss{cache_type="mutablestate"}')
    d["cleared"], d["cache_miss"] = cl, cm
    d["loop_ratio"] = cl / cm if cm else None
    # check 4: who is paying for it (requests that failed, by service and API/RPC method)
    d["failed_by_method"] = by('service_errors_resource_exhausted{resource_exhausted_cause="PersistenceLimit"}',
                               "service_name, operation", minimum=0.01)
    # 3.1: low priority starving first? child completions rejected per client start rejected
    starts = rate('service_errors_resource_exhausted{resource_exhausted_cause="PersistenceLimit",'
                  'service_name="frontend",operation="StartWorkflowExecution"}')
    child = rate('service_errors_resource_exhausted{resource_exhausted_cause="PersistenceLimit",'
                 'operation="RecordChildExecutionCompleted"}')
    d["child_per_start_rejected"] = (child / starts) if starts else None
    # saturation line inputs (4.5): per-shard RPS distribution and owned shards
    d["shards"] = prom1(f"sum(numshards_gauge{{{H}}})", at)
    for name, qq in (("shard_rps_p99", 0.99), ("shard_rps_max", 1.0)):
        d[name] = prom1(f"histogram_quantile({qq}, sum by (le) (rate(persistence_shard_rps_bucket{{{H}}}[{win}])))", at)
    d["adaptive_multiplier_min"] = prom1(f"min(min_over_time(dynamic_rate_limit_multiplier[{win}]))", at)
    d["tasks_completed"] = rate(f"task_attempt_count{{{H}}}")
    return d


def classify(failed: Dict[str, float]) -> dict:
    client, progress, other = {}, {}, {}
    for k, v in failed.items():
        svc, _, op = k.partition(",")
        if svc == "frontend" and op in CLIENT_OPS:
            client[op] = v
        elif op in PROGRESS_OPS:
            progress[k] = v
        else:
            other[k] = v
    return dict(client=client, progress=progress, other=other)


# ----------------------------------------------------------------------------------
# run
# ----------------------------------------------------------------------------------
async def run(cfg: dict) -> int:
    sc = SCENARIOS[cfg["scenario"]]
    run_id = cfg["run_id"] or f"persistenceqps-{cfg['scenario']}-{time.strftime('%H%M%S')}-{uuid.uuid4().hex[:4]}"
    tq = f"pq-{run_id}"
    settings = dict(sc["settings"])
    print(f"persistenceqps run_id={run_id} scenario={cfg['scenario']}: {sc['title']}")
    print("dynamic config applied for this run:")
    for k, v in settings.items():
        print(f"  {k}: {json.dumps(v)}")
    if sc.get("db_latency_ms"):
        print(f"database: +{sc['db_latency_ms']} ms per Postgres response (toxiproxy), applied after the baseline")
    apply_block(settings, BEGIN, END)
    workers = [WorkerProc(i, {"TASK_QUEUE": tq, "ROLE": "all", "TEMPORAL_NAMESPACE": common.NAMESPACE,
                              "MAX_CACHED_WORKFLOWS": "5000"}, f"results/{run_id}-worker{i}.log")
               for i in range(cfg["workers"])]
    client = await common.connect(id_prefix="persistenceqps")
    try:
        return await _run(cfg, sc, settings, run_id, tq, workers, client)
    finally:
        for w in workers:
            w.stop()
        db_latency_off()
        remove_block(BEGIN, END)
        print("dynamic-config block and DB latency removed")


async def _run(cfg, sc, settings, run_id, tq, workers, client) -> int:
    probe_e2e: Dict[str, List[float]] = defaultdict(list)
    phase = {"name": "settle"}
    stop = asyncio.Event()

    async def probe() -> None:
        i, pending = 0, set()

        async def one(n: int, ph: str) -> None:
            t = time.monotonic()
            try:
                h = await client.start_workflow("ChainWorkflow", {"n": 0}, id=f"{run_id}-probe-{n}", task_queue=tq,
                                                execution_timeout=timedelta(minutes=20))
                await asyncio.wait_for(h.result(), timeout=600)
                probe_e2e[ph].append((time.monotonic() - t) * 1000)
            except Exception:  # noqa: BLE001
                pass

        while not stop.is_set():
            task = asyncio.create_task(one(i, phase["name"]))
            pending.add(task)
            task.add_done_callback(pending.discard)
            i += 1
            try:
                await asyncio.wait_for(stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
        if pending:
            await asyncio.wait(pending, timeout=30)

    def completed() -> float:   # the workload only: the probe runs as ChainWorkflow(n=0)
        return sum(counter(w.scrape(), "temporal_workflow_completed", workflow_type=t) for w in workers
                   for t in ("NoopWorkflow", "ParentWorkflow", "ChildWorkflow"))

    deadline = time.time() + 30
    while time.time() < deadline and not all(w.scrape() for w in workers):
        await asyncio.sleep(0.5)
    probe_task = asyncio.create_task(probe())
    print(f"\nsettle {cfg['settle_s']} s (the limiter re-reads its quota once a minute: `Quota changed` in the history log)")
    await asyncio.sleep(cfg["settle_s"])
    base = readings(45, time.time())
    print(f"  baseline: history DB calls {base['requests']:.1f}/s, refused {base['rejected']:.1f}/s, UpdateWorkflowExecution "
          f"p95 {base['db_write_p95_ms'] or 0:.1f} ms")
    if sc.get("db_latency_ms"):
        db_latency_on(sc["db_latency_ms"])

    # ---- burst: client starts (priority 1) + parent/child background work (priority 4) ----
    phase["name"] = "burst"
    n_starts = int(cfg["start_rate"] * cfg["start_s"])
    print(f"\nburst: {n_starts} client starts (NoopWorkflow, {cfg['start_rate']}/s) + {cfg['parents']} parents x "
          f"{cfg['children']} children ({cfg['parent_rate']}/s), all on task queue {tq}")
    done0 = completed()
    t_burst = time.time()
    start_ms: List[float] = []
    errors: Dict[str, int] = defaultdict(int)
    started = {"n": 0}

    async def start(wf: str, params: dict, wid: str) -> None:
        t = time.monotonic()
        try:
            await client.start_workflow(wf, params, id=wid, task_queue=tq, execution_timeout=timedelta(minutes=30))
            started["n"] += 1
            if wf == "NoopWorkflow":
                start_ms.append((time.monotonic() - t) * 1000)
        except Exception as e:  # noqa: BLE001
            errors[f"{wf}: {type(e).__name__}"] += 1

    jobs = [(t_burst + i / cfg["start_rate"], "NoopWorkflow", {}, f"{run_id}-s{i}") for i in range(n_starts)]
    jobs += [(t_burst + i / cfg["parent_rate"], "ParentWorkflow", {"n": cfg["children"]}, f"{run_id}-p{i}")
             for i in range(cfg["parents"])]
    jobs.sort(key=lambda j: j[0])
    inflight = set()
    for when, wf, params, wid in jobs:
        await asyncio.sleep(max(0.0, when - time.time()))
        task = asyncio.create_task(start(wf, params, wid))
        inflight.add(task)
        task.add_done_callback(inflight.discard)
    if inflight:
        await asyncio.wait(set(inflight), timeout=300)
    target = (n_starts + cfg["parents"] * (cfg["children"] + 1)) - sum(errors.values())

    paused, prev, last_print = 0.0, time.time(), 0.0
    progress = (0.0, time.time())
    stalled = False
    while True:
        done = completed() - done0
        now = time.time()
        if now - prev > 30:
            paused += now - prev
            print(f"  WARNING: the test was paused for {now - prev:.0f} s (did the host sleep? `pmset -g log`)")
        prev = now
        if done != progress[0]:
            progress = (done, now)
        elif now - progress[1] > cfg["stall_s"] and (readings(60, now)["tasks_completed"] or 0) <= 1.0:
            stalled = True
            print(f"  STALLED: nothing completed for {cfg['stall_s']} s")
            break
        if now - last_print >= 20:
            r = readings(20, now)
            print(f"  t={now - t_burst:>5.0f}s executions done {done:.0f}/{target}  refused {r['rejected']:>6.1f}/s "
                  f"{r['by_scope_cause'] or ''}  requests failed {r['requests_failed']:>5.1f}/s  "
                  f"DB write p95 {r['db_write_p95_ms'] or 0:>6.1f} ms")
            last_print = now
        if done >= target or now - t_burst > cfg["max_drain_s"]:
            break
        await asyncio.sleep(2)
    t_end = time.time()
    unfinished = int(target - (completed() - done0))
    if unfinished > 0:
        await terminate_open(client, tq)
    phase["name"] = "after"
    stop.set()
    await probe_task

    if paused > 30:
        print(f"\nRESULT: INVALID (the test was paused for {paused:.0f} s: a sleeping host freezes Docker. Keep the "
              f"machine awake: `caffeinate -dimsu make persistenceqps ...`, lid open)")
        return 2

    w = max(30, int(t_end - t_burst))
    m = readings(w, t_end + 5)
    m.update(drain_s=t_end - t_burst, target=target, unfinished=unfinished, stalled=stalled,
             start_errors=dict(errors), start_p50=pct(start_ms, 50), start_p99=pct(start_ms, 99),
             probe_settle_p95=pct(probe_e2e.get("settle", []), 95), probe_burst_p95=pct(probe_e2e.get("burst", []), 95),
             baseline=base)
    report(cfg, sc, settings, run_id, m)
    return verdict(cfg, sc, settings, run_id, m)


# ----------------------------------------------------------------------------------
# report: the four checks of 4.5 and the decision of 4.6
# ----------------------------------------------------------------------------------
def pf(x, d=1, w=7):
    return f"{'-':>{w}}" if x is None else f"{x:>{w}.{d}f}"


def checks(settings: dict, m: dict) -> dict:
    base = m["baseline"]
    b95 = base.get("db_write_p95_ms") or 0.0
    db_room = (m["db_write_p95_ms"] or 0) <= max(2 * b95, b95 + 10) and not m["db_error_types"]
    who = classify(m["failed_by_method"])
    client = bool(who["client"])
    progress = bool(who["progress"])
    loop = (m["loop_ratio"] or 0) > 5
    outcome = ("A" if (client or progress) else "B") if db_room else ("C" if (client or progress) else "D")
    if m["rejected"] < 1:
        outcome = "-"
    return dict(db_room=db_room, who=who, client=client, progress=progress, loop=loop, outcome=outcome)


def report(cfg, sc, settings, run_id, m) -> None:
    c = checks(settings, m)
    base = m["baseline"]
    g = settings.get("history.persistenceGlobalMaxQPS", 0)
    pod = settings.get("history.persistenceMaxQPS", 9000)
    ns = settings.get("history.persistenceNamespaceMaxQPS", 0)
    effective = g if g else pod
    print("\n" + "=" * 106)
    print(f"PERSISTENCE-QPS TEST  scenario={cfg['scenario']}  burst window={m['drain_s']:.0f} s")
    print("=" * 106)
    print(f"workload    {m['target']} executions drained in {m['drain_s']:.0f} s"
          + (f", {m['unfinished']} UNFINISHED" if m["unfinished"] else "")
          + f"   client start latency p50 {pf(m['start_p50'], 0).strip()} ms p99 {pf(m['start_p99'], 0).strip()} ms"
          + (f"   start errors {m['start_errors']}" if m["start_errors"] else ""))
    print("\n4.1 confirm  history DB calls refused, by scope and cause: "
          + (", ".join(f"{k} {v}/s" for k, v in m["by_scope_cause"].items()) or "none"))
    print(f"             top refused operations: " + (", ".join(
        f"{k} {v}" + (" (lab artifact)" if k in LAB_ARTIFACT_OPS else "") for k, v in m["by_operation"].items()) or "none"))
    print(f"4.3 counters database calls refused {m['rejected']:.1f}/s  |  requests failed {m['requests_failed']:.1f}/s  |  "
          f"task attempts throttled {m['task_attempts_throttled']:.1f}/s"
          + (f"   (refused / failed = {m['rejected'] / m['requests_failed']:.1f}x)" if m["requests_failed"] else ""))
    print(f"4.2 clean    Persistence Availability {pf(m['availability_pct'], 2).strip()}%   Total Timer Tasks Errors "
          f"{m['timer_task_errors']:.1f}/s   <- read healthy whatever is happening")
    print(f"\ncheck 1      enforced per-pod limit: {'persistenceGlobalMaxQPS ' + str(g) + ' (replaces persistenceMaxQPS ' + str(pod) + ')' if g else 'persistenceMaxQPS ' + str(pod)}"
          + (f";  persistenceNamespaceMaxQPS {ns} (checked FIRST)" if ns else "")
          + ("   ALL LIMITING OFF (per-pod 0 and cluster-wide 0)" if not g and not pod else ""))
    print(f"check 2      UpdateWorkflowExecution p95 {pf(base.get('db_write_p95_ms'), 1).strip()} ms at baseline -> "
          f"{pf(m['db_write_p95_ms'], 1).strip()} ms (p99 {pf(m['db_write_p99_ms'], 1).strip()});  persistence errors "
          f"other than throttling: {m['db_error_types'] or 'none'}  -> {'DB HAD ROOM' if c['db_room'] else 'DB COULD NOT TAKE MORE'}")
    print(f"check 3      loop indicator {pf(m['loop_ratio'], 1).strip()} (cleared {m['cleared']:.1f}/s / cache miss "
          f"{m['cache_miss']:.1f}/s)  -> {'THE LOOP IS RUNNING: clear it first' if c['loop'] else 'no loop'}")
    w = c["who"]
    print(f"check 4      requests failed by method: client-facing {w['client'] or 'none'};  carrying workflow progress "
          f"{w['progress'] or 'none'};  other {w['other'] or 'none'}")
    if m["child_per_start_rejected"] is not None:
        print(f"3.1 priority child completions rejected per client start rejected: {m['child_per_start_rejected']:.1f} "
              f"(near 1 = priority-1 traffic no longer fits: high and low priority starve together)")
    if m["shards"] and effective:
        line = effective / m["shards"]
        print(f"saturation  line = {effective} / {m['shards']:.0f} shards = {line:.2f}/s per shard;  per-shard RPS p99 "
              f"{pf(m['shard_rps_p99'], 1).strip()}, max {pf(m['shard_rps_max'], 1).strip()}")
    if "history.persistenceDynamicRateLimitingParams" in settings and m["adaptive_multiplier_min"] is not None:
        # Only meaningful when backoff is configured: the gauge is recorded only when the multiplier moves, so it
        # keeps showing its last value after backoff is switched off (until the pod restarts).
        print(f"backoff     Adaptive Rate Limit Multiplier reached {m['adaptive_multiplier_min']:.2f} (1.0 = no reduction)")
    print(f"probe       unrelated workflow start->result p95: {pf(m['probe_settle_p95'], 0).strip()} ms at baseline, "
          f"{pf(m['probe_burst_p95'], 0).strip()} ms during the burst")
    meaning = {"A": "database had room, requests failing -> RAISE the enforced limit, in steps, with adaptive backoff on",
               "B": "database had room, no requests failing -> your call; nothing was lost",
               "C": "database could not take more, requests failing -> do NOT raise; more DB capacity or less work",
               "D": "database could not take more, nothing failing -> leave it; the limiter is doing its job",
               "-": "no rejections"}
    print(f"\n4.6 outcome  {c['outcome']}: {meaning[c['outcome']]}")
    m["checks"] = {k: v for k, v in c.items() if k != "who"} | {"who": c["who"]}


def last_result(scenario: str) -> Optional[dict]:
    files = sorted(glob.glob(f"results/persistenceqps-{scenario}-*.json"), key=os.path.getmtime)
    for f in reversed(files):
        with open(f) as fh:
            d = json.load(fh)
        if d.get("passed") is not None:
            return d
    return None


def verdict(cfg, sc, settings, run_id, m) -> int:
    c = m["checks"]
    scope = m["by_scope_cause"]
    sys_rej = sum(v for k, v in scope.items() if k.startswith("System"))
    ns_rej = sum(v for k, v in scope.items() if k.startswith("Namespace"))
    expect = cfg.get("expect") or sc["expect"]
    if expect == "A":
        ok = c["outcome"] == "A" and m["rejected"] > 10 and sys_rej > ns_rej
        why = "outcome A: > 10 refused calls/s at scope System, database had room, requests failing"
    elif expect == "raised":
        prev = last_result("throttle")
        if prev:
            # Compare TOTAL refused calls over each run (a faster drain packs the same work into fewer seconds, so
            # per-second rates are not comparable), leaving out the lab's force-refresh artifact.
            def workload_total(mm: dict) -> float:
                art = sum(v for k, v in mm["by_operation"].items() if k in LAB_ARTIFACT_OPS)
                return (mm["rejected"] - art) * mm["drain_s"]
            before, after = workload_total(prev["metrics"]), workload_total(m)
            drop = 1 - after / before if before else 0
            faster = prev["metrics"]["drain_s"] / m["drain_s"] if m["drain_s"] else 0
            print(f"vs throttle  refused calls in total (lab artifact excluded) {before:,.0f} -> {after:,.0f} "
                  f"({drop:.0%} down);  drained {prev['metrics']['drain_s']:.0f} s -> {m['drain_s']:.0f} s "
                  f"({faster:.1f}x faster);  DB write p95 {prev['metrics']['db_write_p95_ms'] or 0:.1f} -> "
                  f"{m['db_write_p95_ms'] or 0:.1f} ms")
            m["vs_throttle"] = dict(refused_total_before=before, refused_total_after=after, drop=drop, faster=faster)
            ok = drop >= 0.5 and faster >= 2 and c["db_room"]
            why = ("vs the last `throttle` run: refused calls in total down >= 50%, backlog drained >= 2x faster, "
                   "database latency still flat")
        else:
            ok = m["rejected"] < 10 and c["db_room"]
            why = "(no `throttle` result to compare with) < 10 refused calls/s, database latency flat"
    elif expect == "namespace-binds":
        ok = m["rejected"] > 10 and ns_rej > sys_rej
        why = ("refusals continue although the pod limit was raised, and scope Namespace outweighs System: the "
               "stale namespace limit is the ceiling now")
    elif expect == "global-binds":
        ok = m["rejected"] > 10 and sys_rej > 0
        why = "refused at scope System although persistenceMaxQPS is 20000: the cluster-wide limit replaced it"
    elif expect == "unlimited":
        ok = m["rejected"] < 0.5
        why = ("nothing refused at all, not even by the namespace limit of 50: the pod STARTED with per-pod 0, so no "
               "limiter was built")
    elif expect == "refuse-all":
        completed_n = m["target"] - m["unfinished"]
        ok = m["rejected"] > 100 and completed_n <= 0.05 * m["target"]
        why = ("set on a running pod, per-pod 0 does not switch limiting off: the limiters already exist and now "
               "refuse every call, so history stops")
    elif expect == "C":
        ok = c["outcome"] == "C"
        why = "outcome C: database latency climbing (check 2) while requests fail -> do not raise the limit"
    elif expect == "backed-off":
        ok = (m["adaptive_multiplier_min"] is not None and m["adaptive_multiplier_min"] < 1.0)
        why = "Adaptive Rate Limit Multiplier dropped below 1.0: backoff cut the pod's rate as the database slowed"
    else:
        ok, why = False, "unknown expectation"
    print(f"\nRESULT: {'PASS' if ok else 'FAIL'} ({why})")
    out = f"results/{run_id}.json"
    os.makedirs("results", exist_ok=True)
    with open(out, "w") as f:
        json.dump(dict(cfg=cfg, scenario=cfg["scenario"], settings=settings, metrics=m, passed=ok), f, indent=1,
                  default=str)
    print(f"wrote {out}")
    return 0 if ok else 1


def main() -> None:
    ap = argparse.ArgumentParser(prog="lab.persistenceqps", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    sub.add_parser("reset")
    r = sub.add_parser("run")
    r.add_argument("--scenario", required=True, choices=list(SCENARIOS))
    r.add_argument("--start-rate", type=float, default=120.0, help="client NoopWorkflow starts/s")
    r.add_argument("--start-s", type=int, default=10)
    r.add_argument("--parents", type=int, default=100)
    r.add_argument("--children", type=int, default=8)
    r.add_argument("--parent-rate", type=float, default=20.0)
    r.add_argument("--workers", type=int, default=1)
    r.add_argument("--settle-s", type=int, default=75)
    r.add_argument("--max-drain-s", type=int, default=900)
    r.add_argument("--stall-s", type=int, default=180)
    r.add_argument("--run-id", default=None)
    r.add_argument("--expect", default=None, help="override the scenario's expectation (e.g. unlimited for `zero` "
                                                  "after restarting history with the 0 settings)")
    a = ap.parse_args()
    if a.cmd == "list":
        for k, v in SCENARIOS.items():
            print(f"{k:<9} {v['title']}\n{'':<9} {v['settings']}" + (f"  +{v['db_latency_ms']} ms DB latency"
                                                                       if v.get("db_latency_ms") else ""))
        sys.exit(0)
    if a.cmd == "reset":
        db_latency_off()
        print("removed" if remove_block(BEGIN, END) else "no managed block found")
        sys.exit(0)
    rc = asyncio.run(run(vars(a)))
    sys.stdout.flush()
    os._exit(rc)   # see lab/historytasks.py: never hang at interpreter shutdown after results are written


if __name__ == "__main__":
    main()
