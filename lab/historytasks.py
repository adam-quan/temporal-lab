"""History task processing test: reproduce the write-reject loop, investigate it, verify the fix.

Follows the playbook
https://github.com/tsurdilo/temporal-server-operations/blob/main/playbooks/history-task-processing-tuning.md
and its alert runbook 87 (History Write-Reject Loop).

    python -m lab.historytasks run --scenario loop        # reproduce the loop (exit 0 = loop surfaced)
    python -m lab.historytasks calibrate                  # playbook 5.2: measure database calls per task
    python -m lab.historytasks run --scenario fix         # playbook 5.3: scheduler limits sized from calibrate
    python -m lab.historytasks run --scenario shadow      # trap one: limiter on, shadow mode left on
    python -m lab.historytasks run --scenario zero        # trap two: limiter on, limits left at 0
    python -m lab.historytasks run --scenario toolow      # 5.4: limits set too low
    python -m lab.historytasks indicator                  # the playbook's readings for the last few minutes
    python -m lab.historytasks reset                      # remove the test's dynamic-config block (after a crash)

How a run works
---------------
1. Writes the scenario's settings into a marked block at the end of the lab's dynamic-config file (always
   `history.persistenceMaxQPS` = --budget, the "database budget"; plus the scenario's task-scheduler settings),
   and removes the block when the run ends.
2. Settle (75 s): the persistence limiter and the task scheduler pick up new values within about a minute.
   A probe workflow starts every 2 s throughout the run and measures how long an unrelated execution takes.
3. Burst: --parents ParentWorkflows, each starting --children ChildWorkflows. The children are started BY
   HISTORY TASKS, not by clients, so this is a backlog of task processing (the playbook's parent/child test).
4. Drain: until every parent has completed. Then the test reads the playbook's panels from Prometheus over
   the burst window and prints a verdict.

The workload runs on the test's own worker (a `python -m lab.worker` child process, its own task queue), so the
lab's sdk-worker is not involved.
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time
import uuid
from collections import defaultdict
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

from lab import common
from lab.hotshard import prom, prom1
from lab.workerperf import WorkerProc, counter, pct

DYNCONFIG = os.environ.get("DYNCONFIG_PATH", "/app/dynamicconfig/docker.yaml")
CALIBRATION = "results/historytasks-calibration.json"
BEGIN = "# >>> lab.historytasks managed block: removed when the run ends (`make historytasks ARGS=reset` after a crash)"
END = "# <<< lab.historytasks"

LOOP_RATIO = 5.0        # alert 87: cleared / cache miss above 5 ...
REJECT_GATE = 10.0      # ... while more than 10 history calls/s are refused (PersistenceLimit)
DEFAULT_CPT = 3.7       # the playbook's measured calls per task, used only if calibrate has not been run

SCENARIOS = {
    "loop": dict(title="No task-scheduler limits: the default (the write-reject loop)", expect="loop"),
    "fix": dict(title="Task-scheduler limits sized from calls per task, shadow mode off (playbook 5.3)",
                expect="healthy"),
    "shadow": dict(title="Trap one: limiter on and limits set, but shadow mode left on", expect="loop+counted"),
    "zero": dict(title="Trap two: limiter on, shadow off, the four limits left at 0", expect="loop"),
    "toolow": dict(title="Limits set too low (playbook 5.4)", expect="slow"),
}


# ----------------------------------------------------------------------------------
# dynamic config block
# ----------------------------------------------------------------------------------
def _strip_block(text: str, begin: str = BEGIN, end: str = END) -> str:
    return re.sub(re.escape(begin) + r".*?" + re.escape(end) + r"\n?", "", text, flags=re.S)


def apply_block(settings: Dict[str, object], begin: str = BEGIN, end: str = END) -> None:
    """Write `settings` as a marked block at the end of the dynamic-config file (replacing an earlier block)."""
    with open(DYNCONFIG) as f:
        base = _strip_block(f.read(), begin, end)
    active = {m.group(1) for m in re.finditer(r"^([A-Za-z][\w.]*):", base, flags=re.M)}
    clash = sorted(active & set(settings))
    if clash:
        sys.exit(f"{DYNCONFIG} already sets {clash} (uncommented). Comment them out first: "
                 f"duplicate keys make the server reject the whole file.")
    lines = [begin]
    for k, v in settings.items():
        lines += [f"{k}:", f"  - value: {json.dumps(v)}"]
    lines.append(end)
    with open(DYNCONFIG, "w") as f:
        f.write(base.rstrip("\n") + "\n\n" + "\n".join(lines) + "\n")


def remove_block(begin: str = BEGIN, end: str = END) -> bool:
    with open(DYNCONFIG) as f:
        text = f.read()
    new = _strip_block(text, begin, end)
    if new != text:
        with open(DYNCONFIG, "w") as f:
            f.write(new.rstrip("\n") + "\n")
        return True
    return False


def scenario_settings(name: str, budget: int, cpt: float, ns_limit: Optional[int], all_limit: Optional[int]) -> dict:
    s: Dict[str, object] = {"history.persistenceMaxQPS": budget}
    supported = budget / cpt
    ns = ns_limit or int(supported * 0.75)
    al = all_limit or int(supported)
    if name == "fix":
        s.update({"history.taskSchedulerEnableRateLimiter": True,
                  "history.taskSchedulerEnableRateLimiterShadowMode": False,
                  "history.taskSchedulerGlobalNamespaceMaxQPS": ns,
                  "history.taskSchedulerGlobalMaxQPS": al})
    elif name == "shadow":
        s.update({"history.taskSchedulerEnableRateLimiter": True,
                  "history.taskSchedulerGlobalNamespaceMaxQPS": ns,
                  "history.taskSchedulerGlobalMaxQPS": al})       # shadow mode left at its default (true)
    elif name == "zero":
        s.update({"history.taskSchedulerEnableRateLimiter": True,
                  "history.taskSchedulerEnableRateLimiterShadowMode": False})   # the four limits left at 0
    elif name == "toolow":
        low = ns_limit or max(5, int(supported * 0.15))
        s.update({"history.taskSchedulerEnableRateLimiter": True,
                  "history.taskSchedulerEnableRateLimiterShadowMode": False,
                  "history.taskSchedulerGlobalNamespaceMaxQPS": low,
                  "history.taskSchedulerGlobalMaxQPS": al})
    return s


def load_cpt() -> Tuple[float, str]:
    try:
        with open(CALIBRATION) as f:
            c = json.load(f)
        return float(c["calls_per_task"]), f"measured by `calibrate` at {c['when']}"
    except (OSError, KeyError, ValueError):
        return DEFAULT_CPT, f"playbook default {DEFAULT_CPT} (run `calibrate` to measure this lab's own)"


# ----------------------------------------------------------------------------------
# Prometheus readings: the playbook's panels, averaged over a window
# ----------------------------------------------------------------------------------
H = 'service_name="history"'


def readings(w: int, at: float) -> dict:
    """The playbook's panels as numbers, averaged over the `w` seconds ending at `at`."""
    win = f"{w}s"

    def rate(expr: str) -> Optional[float]:
        return prom1(f"sum(increase({expr}[{win}])) / {w}", at)

    def by(expr: str, label: str) -> Dict[str, float]:
        r = prom(f"sum by ({label}) (increase({expr}[{win}])) / {w}", at)
        return {k.split("=", 1)[1] if "=" in k else k: v for k, v in r.items() if v > 0.05}

    d: dict = {}
    d["cleared"] = rate("workflow_context_cleared")
    d["cache_miss"] = rate('cache_miss{cache_type="mutablestate"}')
    d["ratio"] = (d["cleared"] / d["cache_miss"]) if d["cleared"] and d["cache_miss"] else None
    d["rejected"] = rate(f"persistence_errors_resource_exhausted{{{H}}}") or 0.0
    d["rejected_by_op"] = dict(sorted(by(f"persistence_errors_resource_exhausted{{{H}}}", "operation").items(),
                                      key=lambda kv: -kv[1])[:5])
    d["attempted"] = rate(f"persistence_requests{{{H}}}") or 0.0
    d["reached"] = d["attempted"] - d["rejected"]
    d["gwe_attempted"] = rate(f'persistence_requests{{{H},operation="GetWorkflowExecution"}}') or 0.0
    d["gwe_rejected"] = rate(f'persistence_errors_resource_exhausted{{{H},operation="GetWorkflowExecution"}}') or 0.0
    # "Resource Exhausted with Cause": API calls (worker / client / history-internal) refused by PersistenceLimit.
    api = prom(f'sum by (service_name, operation) (increase(service_errors_resource_exhausted'
               f'{{resource_exhausted_cause="PersistenceLimit"}}[{win}]))', at)
    d["api_refused"] = dict(sorted(((k.replace("operation=", "").replace("service_name=", ""), round(v))
                                    for k, v in api.items() if v >= 1), key=lambda kv: -kv[1])[:5])
    d["load_rate"] = rate(f'persistence_requests{{{H},operation=~"Get.*Tasks"}}') or 0.0
    d["task_attempts"] = rate(f"task_requests{{{H}}}") or 0.0                  # "History Task Throughput": attempts
    d["tasks_completed"] = rate(f"task_attempt_count{{{H}}}") or 0.0          # recorded once per completed task
    d["task_refused"] = rate(f"task_errors_throttled{{{H}}}") or 0.0
    d["sched_throttled"] = rate(f"task_scheduler_throttled{{{H}}}") or 0.0
    d["calls_per_task"] = (d["reached"] / d["tasks_completed"]) if d["tasks_completed"] else None
    sched = prom1(f"histogram_quantile(0.95, sum by (le) (rate(task_latency_schedule_bucket{{{H}}}[{win}])))", at)
    d["sched_latency_p95_ms"] = None if sched is None else sched * 1000
    att = prom1(f"histogram_quantile(0.99, sum by (le) (rate(task_attempt_bucket{{{H}}}[{win}])))", at)
    d["attempts_p99"] = att
    db = prom1(f'histogram_quantile(0.99, sum by (le) (rate(persistence_latency_bucket{{{H},'
               f'operation="UpdateWorkflowExecution"}}[{win}])))', at)
    d["db_write_p99_ms"] = None if db is None else db * 1000
    return d


def longest_run(values: List[float], threshold: float) -> int:
    best = cur = 0
    for v in values:
        cur = cur + 1 if v > threshold else 0
        best = max(best, cur)
    return best


def peak_ratio_and_alert(start: float, end: float) -> Tuple[Optional[float], dict]:
    """Peak of the loop indicator (1m rates, 15 s steps), and for alerts 86 and 87 the longest run of consecutive
    minutes their condition held (both fire after 10)."""
    import urllib.parse
    import urllib.request

    from lab.hotshard import PROM_URL

    expr87 = ('((sum(rate(workflow_context_cleared[5m])) > 0) / (sum(rate(cache_miss{cache_type="mutablestate"}[5m])) '
              '> 0)) and (sum(rate(persistence_errors_resource_exhausted{service_name="history",'
              'resource_exhausted_cause="PersistenceLimit"}[5m])) > 10)')
    ratio1m = ('(sum(rate(workflow_context_cleared[1m])) > 0) / '
               '(sum(rate(cache_miss{cache_type="mutablestate"}[1m])) > 0)')

    expr86 = ('sum(rate(persistence_errors_resource_exhausted{service_name="history"}[5m])) '
              'by (resource_exhausted_scope, resource_exhausted_cause)')

    def series(expr: str, step: int) -> List[List[float]]:
        q = urllib.parse.urlencode({"query": expr, "start": f"{start:.0f}", "end": f"{end:.0f}", "step": f"{step}"})
        try:
            with urllib.request.urlopen(f"{PROM_URL}/api/v1/query_range?{q}", timeout=15) as r:
                res = json.load(r)["data"]["result"]
        except Exception:  # noqa: BLE001
            return []
        return [[float(v[1]) for v in s["values"]] for s in res]

    peak = max((v for s in series(ratio1m, 15) for v in s), default=None)
    a87 = series(expr87, 60)
    a86 = series(expr86, 60)
    return peak, dict(
        alert87_longest_min=max((longest_run(s, LOOP_RATIO) for s in a87), default=0),
        alert86_longest_min=max((longest_run(s, REJECT_GATE) for s in a86), default=0),
        minutes=max(1, round((end - start) / 60)))


# ----------------------------------------------------------------------------------
# run
# ----------------------------------------------------------------------------------
async def run(cfg: dict, scenario: str) -> int:
    budget = cfg["budget"]
    cpt, cpt_src = load_cpt()
    settings = scenario_settings(scenario, budget, cpt, cfg["ns_limit"], cfg["all_limit"])
    run_id = cfg["run_id"] or f"historytasks-{scenario}-{time.strftime('%H%M%S')}-{uuid.uuid4().hex[:4]}"
    tq = f"ht-{run_id}"
    title = SCENARIOS[scenario]["title"] if scenario in SCENARIOS else "calibration: steady work under the budget"
    print(f"historytasks run_id={run_id} scenario={scenario}: {title}")
    print(f"database budget (history.persistenceMaxQPS) = {budget}/s on the lab's single history pod")
    if scenario not in ("loop", "calibrate"):
        print(f"calls per task = {cpt:.2f} ({cpt_src}) -> the budget supports ~{budget / cpt:.0f} tasks/s")
    print("dynamic config applied for this run:")
    for k, v in settings.items():
        print(f"  {k}: {json.dumps(v)}")

    apply_block(settings)
    workers = [WorkerProc(i, {"TASK_QUEUE": tq, "ROLE": "all", "TEMPORAL_NAMESPACE": common.NAMESPACE,
                              "MAX_CACHED_WORKFLOWS": "5000"}, f"results/{run_id}-worker{i}.log")
               for i in range(cfg["workers"])]
    client = await common.connect(id_prefix="historytasks")
    try:
        return await _run(cfg, scenario, settings, cpt, run_id, tq, workers, client)
    finally:
        for w in workers:
            w.stop()
        remove_block()
        print("dynamic-config block removed (the server reloads it within ~5 s; limiters re-read within ~1 min)")


async def _run(cfg, scenario, settings, cpt, run_id, tq, workers, client) -> int:
    probe_start: Dict[str, List[float]] = defaultdict(list)
    probe_e2e: Dict[str, List[float]] = defaultdict(list)
    phase = {"name": "settle"}
    stop = asyncio.Event()

    async def probe() -> None:
        """An unrelated workflow every 2 s: does work outside the burst still move?"""
        i = 0
        pending = set()

        async def one(n: int, ph: str) -> None:
            t = time.monotonic()
            try:
                h = await client.start_workflow("NoopWorkflow", {}, id=f"{run_id}-probe-{n}", task_queue=tq,
                                                execution_timeout=timedelta(minutes=20))
                probe_start[ph].append((time.monotonic() - t) * 1000)
                await asyncio.wait_for(h.result(), timeout=600)
                probe_e2e[ph].append((time.monotonic() - t) * 1000)
            except Exception:  # noqa: BLE001
                probe_e2e[ph].append(float("inf"))

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

    def parents_done() -> float:
        return sum(counter(w.scrape(), "temporal_workflow_completed", workflow_type="ParentWorkflow") for w in workers)

    deadline = time.time() + 30
    while time.time() < deadline and not all(w.scrape() for w in workers):
        await asyncio.sleep(0.5)
    probe_task = asyncio.create_task(probe())

    settle = cfg["settle_s"]
    print(f"\nsettle {settle} s: the limiters pick the new values up (watch for `Quota changed` in the history log)")
    t_settle = time.time()
    await asyncio.sleep(settle)
    bw = min(45, settle - 15)
    base = readings(bw, time.time())
    from lab.hotshard import cluster_facts
    shards = (await cluster_facts(client))["num_shards"]
    floor = shards * 3 / 60 + shards * 2 / 300   # playbook 7.1: shards x queues / poll interval
    print(f"  baseline: Get*Tasks loading {base['load_rate']:.1f}/s (idle floor for {shards} shards ~{floor:.0f}/s), "
          f"history DB calls reached {base['reached']:.1f}/s, refused {base['rejected']:.1f}/s, "
          f"tasks completed {base['tasks_completed']:.1f}/s")
    busy = prom(f"sum by (operation) (increase(task_requests{{{H}}}[{bw}s])) / {bw} > 2")
    if base["tasks_completed"] > 10 or busy:
        top = sorted(((v, k.split("=", 1)[-1]) for k, v in busy.items()), reverse=True)[:4]
        print(f"  WARNING: the cluster is not idle ({', '.join(f'{k} {v:.0f}/s' for v, k in top)}). Its own task "
              f"backlog will be in every number below. Retention clean-up after the lab was down "
              f"(TimerActiveTaskDeleteHistoryEvent) is the usual cause: wait for it to finish.")
    base["shards"], base["idle_floor"] = shards, floor

    calibrating = scenario == "calibrate"
    parents, children = (cfg["cal_parents"], cfg["children"]) if calibrating else (cfg["parents"], cfg["children"])
    rate = cfg["cal_rate"] if calibrating else cfg["rate"]
    phase["name"] = "burst"
    done0 = parents_done()
    t_burst = time.time()
    print(f"\nburst: {parents} ParentWorkflows x {children} children = {parents * (children + 1)} executions, "
          f"started at {rate}/s")
    errors: Dict[str, int] = defaultdict(int)
    for i in range(parents):
        await asyncio.sleep(max(0.0, t_burst + i / rate - time.time()))
        try:
            await client.start_workflow("ParentWorkflow", {"n": children}, id=f"{run_id}-p{i}", task_queue=tq,
                                        execution_timeout=timedelta(minutes=30))
        except Exception as e:  # noqa: BLE001
            errors[type(e).__name__] += 1
    started = parents - sum(errors.values())

    t_last = time.time()
    progress = (0.0, time.time())          # (parents done, when that count last changed)
    stalled = False
    paused = 0.0                           # wall-clock jumps: the host slept (laptop lid, idle sleep)
    prev = time.time()
    while True:
        done = parents_done() - done0
        now = time.time()
        if now - prev > 30:
            paused += now - prev
            print(f"  WARNING: the test was paused for {now - prev:.0f} s (did the host sleep? `pmset -g log`)")
        prev = now
        if done != progress[0]:
            progress = (done, now)
        elif done < started and now - progress[1] > cfg["stall_s"]:
            # No parent finished for a while. With a low scheduler limit the work is shared round-robin across every
            # parent, so none may finish for minutes while tasks complete steadily: only a stop in TASKS is a stall.
            if (readings(60, now)["tasks_completed"] or 0) > 1.0:
                progress = (done, now)
            else:
                stalled = True
                print(f"  STALLED: no history task completed for 60 s and no parent for {cfg['stall_s']} s "
                      f"({done:.0f}/{started} done); stopping the wait")
                break
        if now - t_last >= 20:
            r = readings(20, now)
            print(f"  t={now - t_burst:>5.0f}s parents done {done:.0f}/{started}  "
                  f"loop indicator {('%.1f' % r['ratio']) if r['ratio'] else '-':>5}  refused {r['rejected']:>6.1f}/s  "
                  f"tasks completed {r['tasks_completed']:>6.1f}/s  attempts {r['task_attempts']:>6.1f}/s  "
                  f"scheduler throttled {r['sched_throttled']:>6.1f}/s")
            t_last = now
        if done >= started or now - t_burst > cfg["max_drain_s"]:
            break
        await asyncio.sleep(2)
    t_drained = progress[1] if stalled else time.time()
    unfinished = int(started - (parents_done() - done0))
    stuck: List[str] = []
    if unfinished:
        stuck = await open_workflows(client, tq, limit=5)
        await terminate_open(client, tq)
    phase["name"] = "after"
    await asyncio.sleep(6)
    stop.set()
    await probe_task

    w = max(30, int(t_drained - t_burst))
    m = readings(w, t_drained + 5)
    peak, alerts = peak_ratio_and_alert(t_burst, t_drained + 5)
    drain_s = t_drained - t_burst
    m.update(drain_s=drain_s, parents=started, children=children, unfinished=unfinished, stalled=stalled,
             stuck_examples=stuck, peak_ratio=peak,
             alerts=alerts, start_errors=dict(errors),
             executions_per_s=(started * (children + 1) - unfinished * (children + 1)) / drain_s,
             baseline=base)

    # ---- report ------------------------------------------------------------------------
    def pf(x, d=1, w=7):
        return f"{'-':>{w}}" if x is None else f"{x:>{w}.{d}f}"

    print("\n" + "=" * 104)
    print(f"HISTORY-TASK TEST  scenario={scenario}  burst window={drain_s:.0f} s  budget={cfg['budget']}/s")
    print("=" * 104)
    print(f"backlog     {started} parents x {children} children: drained in {drain_s:.0f} s "
          f"({m['executions_per_s']:.1f} executions/s)" + (f", {unfinished} parents UNFINISHED" if unfinished else ""))
    if unfinished:
        print(f"            {'stalled' if stalled else 'still open'} (terminated so the next run starts clean), e.g. "
              f"{', '.join(stuck)}: `temporal workflow show -w <id>` shows how far each got")
    print(f"loop        indicator (cleared / cache miss) avg {pf(m['ratio'])}  peak {pf(peak)}   "
          f"state cleared {pf(m['cleared'])}/s   genuine cache misses {pf(m['cache_miss'])}/s")
    print(f"refused     {pf(m['rejected'])}/s history DB calls refused (PersistenceLimit); top: "
          + ", ".join(f"{k} {v:.1f}" for k, v in m["rejected_by_op"].items()))
    print(f"reached     GetWorkflowExecution attempted {pf(m['gwe_attempted'])}/s, refused {pf(m['gwe_rejected'])}/s, "
          f"reached the DB {pf(m['gwe_attempted'] - m['gwe_rejected'])}/s   all history calls reached "
          f"{pf(m['reached'])}/s")
    print(f"api calls   refused over the window (Resource Exhausted with Cause, PersistenceLimit): "
          + (", ".join(f"{k} {v}" for k, v in m["api_refused"].items()) or "none")
          + "  (SDKs retry these: no error reaches the caller, the work just waits)")
    print(f"tasks       completed {pf(m['tasks_completed'])}/s   attempts {pf(m['task_attempts'])}/s "
          f"('History Task Throughput' counts attempts)   refused attempts {pf(m['task_refused'])}/s   "
          f"attempts/task p99 {pf(m['attempts_p99'])}")
    print(f"scheduler   throttled {pf(m['sched_throttled'])}/s (counts every refusal: a task can be counted many times)   "
          f"schedule latency p95 {pf(m['sched_latency_p95_ms'], 0)} ms")
    print(f"database    calls per completed task {pf(m['calls_per_task'], 2)}   UpdateWorkflowExecution p99 "
          f"{pf(m['db_write_p99_ms'], 1)} ms (flat = the limiter, not the DB, is refusing)")
    a = alerts

    def fires(n: int) -> str:
        return "WOULD FIRE" if n >= 10 else "would not fire"

    print(f"alerts      86 (refused > 10/s): {a['alert86_longest_min']} consecutive min -> {fires(a['alert86_longest_min'])};"
          f"  87 (ratio > 5 and refused > 10/s): {a['alert87_longest_min']} consecutive min -> "
          f"{fires(a['alert87_longest_min'])}  (both need 10; window {a['minutes']} min)")
    for ph in ("settle", "burst"):
        st, e2 = probe_start.get(ph, []), probe_e2e.get(ph, [])
        stuck = sum(1 for x in e2 if x == float("inf"))
        print(f"probe       {ph:<6} unrelated workflow: start p50 {pf(pct(st, 50), 0)} ms   start->result p50 "
              f"{pf(pct([x for x in e2 if x != float('inf')], 50), 0)} ms  p95 "
              f"{pf(pct([x for x in e2 if x != float('inf')], 95), 0)} ms" + (f"  ({stuck} never finished)" if stuck else ""))
    m["probe"] = {ph: dict(start_p50=pct(probe_start.get(ph, []), 50),
                           e2e_p50=pct([x for x in probe_e2e.get(ph, []) if x != float("inf")], 50),
                           e2e_p95=pct([x for x in probe_e2e.get(ph, []) if x != float("inf")], 95))
                  for ph in ("settle", "burst")}

    os.makedirs("results", exist_ok=True)
    if calibrating:
        return _finish_calibrate(cfg, m, run_id)

    loop = (peak or 0) > LOOP_RATIO and m["rejected"] > REJECT_GATE
    if paused > 30:
        print(f"\nRESULT: INVALID (the test was paused for {paused:.0f} s in total: a sleeping host freezes Docker and "
              f"every timer and backoff with it. Keep the machine awake, e.g. `caffeinate -i make historytasks ...`)")
        return 2
    if stalled:
        print(f"\nNOTE: {unfinished} parents never finished within the stall window; they were terminated.")
    counted = m["sched_throttled"] > 1.0
    task_ok = m["task_refused"] <= 2.0                      # refused task attempts, not the pod's housekeeping calls
    healthy = not loop and task_ok and counted and not unfinished
    ns_limit = settings.get("history.taskSchedulerGlobalNamespaceMaxQPS")
    # Too low (playbook 5.4): nothing refused, tasks waiting, and completions pinned at the configured limit.
    pinned = bool(ns_limit) and m["tasks_completed"] <= 1.3 * float(ns_limit)
    slow = not loop and task_ok and counted and pinned
    m["pinned_at_limit"] = pinned
    verdicts = {
        "loop": (loop, "the write-reject loop surfaced: indicator peak > 5 with > 10 refused calls/s"),
        "healthy": (healthy, "task attempts refused <= 2/s, no loop, scheduler throttling > 0, backlog drained"),
        "loop+counted": (loop and counted, "the throttling panel shows throttling, yet the loop still runs "
                                           "(shadow mode counts and holds nothing back)"),
        "slow": (slow, "nothing refused and no loop, tasks held at the scheduler, completions pinned at the "
                       "configured limit: the limit, not the database, is the constraint"),
    }
    expect = SCENARIOS[scenario]["expect"]
    if scenario == "zero":
        verdicts["loop"] = (loop and not counted, "limiter on and live, yet nothing throttled and the loop runs: the "
                                                 "0 limits fell back to the persistence number")
    ok, why = verdicts[expect]
    print(f"\nRESULT: {'PASS' if ok else 'FAIL'} (expect={expect}: {why})")
    out = f"results/{run_id}.json"
    with open(out, "w") as f:
        json.dump(dict(cfg=cfg, scenario=scenario, settings=settings, calls_per_task_used=cpt, metrics=m,
                       loop=loop, passed=ok), f, indent=1, default=str)
    print(f"wrote {out}")
    return 0 if ok else 1


async def open_workflows(client, tq: str, limit: int) -> List[str]:
    out: List[str] = []
    try:
        async for wf in client.list_workflows(f'TaskQueue = "{tq}" AND ExecutionStatus = "Running" '
                                              f'AND WorkflowType = "ParentWorkflow"', limit=limit):
            out.append(wf.id)
    except Exception:  # noqa: BLE001
        pass
    return out


async def terminate_open(client, tq: str) -> None:
    from temporalio.api.batch.v1 import BatchOperationTermination
    from temporalio.api.workflowservice.v1 import StartBatchOperationRequest

    try:
        await client.workflow_service.start_batch_operation(StartBatchOperationRequest(
            namespace=client.namespace, visibility_query=f'TaskQueue = "{tq}" AND ExecutionStatus = "Running"',
            job_id=str(uuid.uuid4()), reason="historytasks cleanup", termination_operation=BatchOperationTermination()))
    except Exception as e:  # noqa: BLE001
        print(f"  (cleanup failed: {type(e).__name__}: {e})")


def _finish_calibrate(cfg: dict, m: dict, run_id: str) -> int:
    cpt = m["calls_per_task"]
    if not cpt or m["rejected"] > 2.0:
        print(f"\nCALIBRATION FAILED: calls per task {cpt}, refused {m['rejected']:.1f}/s. It must be measured during "
              f"ordinary work, without refusals: lower --cal-rate.")
        return 1
    supported = cfg["budget"] / cpt
    out = dict(when=time.strftime("%Y-%m-%d %H:%M:%S"), run_id=run_id, budget=cfg["budget"], calls_per_task=round(cpt, 2),
               tasks_supported=round(supported, 1), taskSchedulerGlobalNamespaceMaxQPS=int(supported * 0.75),
               taskSchedulerGlobalMaxQPS=int(supported))
    with open(CALIBRATION, "w") as f:
        json.dump(out, f, indent=1)
    print(f"\ncalls per task = {m['reached']:.1f} DB calls reached / {m['tasks_completed']:.1f} tasks completed "
          f"= {cpt:.2f}")
    print(f"budget {cfg['budget']} / {cpt:.2f} = {supported:.0f} tasks/s  ->  "
          f"history.taskSchedulerGlobalNamespaceMaxQPS {out['taskSchedulerGlobalNamespaceMaxQPS']} (75%), "
          f"history.taskSchedulerGlobalMaxQPS {out['taskSchedulerGlobalMaxQPS']}")
    print(f"wrote {CALIBRATION} (used by --scenario fix / shadow / toolow)")
    return 0


def cmd_indicator(window: int) -> int:
    m = readings(window, time.time())
    print(f"last {window} s:  loop indicator {m['ratio'] if m['ratio'] is None else round(m['ratio'], 1)}  "
          f"(cleared {m['cleared'] or 0:.1f}/s, cache miss {m['cache_miss'] or 0:.1f}/s)")
    print(f"  refused {m['rejected']:.1f}/s  by op {m['rejected_by_op']}")
    print(f"  GetWorkflowExecution attempted {m['gwe_attempted']:.1f}/s, reached {m['gwe_attempted'] - m['gwe_rejected']:.1f}/s")
    print(f"  tasks completed {m['tasks_completed']:.1f}/s, attempts {m['task_attempts']:.1f}/s, scheduler throttled "
          f"{m['sched_throttled']:.1f}/s, Get*Tasks loading {m['load_rate']:.1f}/s")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(prog="lab.historytasks", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--budget", type=int, default=300, help="history.persistenceMaxQPS for the run (the DB budget)")
        p.add_argument("--parents", type=int, default=400)
        p.add_argument("--children", type=int, default=8)
        p.add_argument("--rate", type=float, default=100.0, help="parent starts/s during the burst")
        p.add_argument("--workers", type=int, default=1)
        p.add_argument("--settle-s", type=int, default=75)
        p.add_argument("--max-drain-s", type=int, default=900)
        p.add_argument("--stall-s", type=int, default=180, help="stop waiting when no parent finishes for this long")
        p.add_argument("--ns-limit", type=int, default=None, help="override taskSchedulerGlobalNamespaceMaxQPS")
        p.add_argument("--all-limit", type=int, default=None, help="override taskSchedulerGlobalMaxQPS")
        p.add_argument("--run-id", default=None)

    r = sub.add_parser("run", help="run a scenario")
    r.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    common_args(r)
    c = sub.add_parser("calibrate", help="measure DB calls per task under ordinary (unrefused) work")
    common_args(c)
    c.add_argument("--cal-parents", type=int, default=60)
    c.add_argument("--cal-rate", type=float, default=0.5, help="parent starts/s: slow enough that nothing is refused")
    i = sub.add_parser("indicator", help="the playbook's readings for the last window")
    i.add_argument("--window", type=int, default=120)
    sub.add_parser("reset", help="remove the test's dynamic-config block")
    a = ap.parse_args()
    if a.cmd == "reset":
        print("removed" if remove_block() else "no managed block found")
        sys.exit(0)
    if a.cmd == "indicator":
        sys.exit(cmd_indicator(a.window))
    cfg = vars(a)
    cfg.setdefault("cal_parents", 60)
    cfg.setdefault("cal_rate", 0.5)
    rc = asyncio.run(run(cfg, "calibrate" if a.cmd == "calibrate" else a.scenario))
    # Results are written and the dynamic-config block is removed. Exit hard: one run hung for hours at interpreter
    # shutdown (SDK runtime threads waiting on cancelled long-polls), which blocked every run queued after it.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(rc)


if __name__ == "__main__":
    main()
