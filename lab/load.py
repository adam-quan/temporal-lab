"""Open-loop load generator for the Temporal lab.

    python -m lab.load ramp  --shape noop --rates 10,50,100,500 --step-seconds 60
    python -m lab.load ramp  --shape signal --keys 1 --rate 500 --duration 60      # hot key
    python -m lab.load smoke                                                       # 1 of everything

Why "open loop" matters
-----------------------
A closed-loop generator ("start, wait, start again") slows down exactly when the server
slows down, and hides the problem (coordinated omission). This one schedules operation k of
a step at `step_start + k/rate` no matter what. Every latency is measured from that
*intended* time, so a stalled server shows up as latency instead of as a quiet drop in rate.
If the generator itself cannot keep up (its own scheduling lag grows) the report says so:
in that case use --processes N or run more loadgen containers.

Measured per step
-----------------
  rpc_*        time the StartWorkflowExecution call itself took
  intended_*   start-call completion measured from the intended send time (rpc + queueing in the generator)
  e2e_*        intended time -> workflow result available (only with --await; long-polls the result)
  lag_*        how late the generator was in sending (generator health)

Workflows are started BY TYPE NAME with a plain dict; the worker's data converter builds the
dataclass. So `--params '{"n": 500, "spec": {"work_ms": 200}}'` needs no imports here.
"""
import argparse
import asyncio
import json
import math
import multiprocessing as mp
import os
import queue as queue_mod
import sys
import time
import uuid
from collections import Counter, defaultdict
from datetime import timedelta
from typing import Any, Dict, List, Optional

from lab import common

# ----------------------------------------------------------------------------------
# Shapes: workflow type + default params + whether to wait for results by default.
# ----------------------------------------------------------------------------------
SHAPES: Dict[str, Dict[str, Any]] = {
    "noop": dict(wf="NoopWorkflow", params={}, await_result=True),
    "chain": dict(wf="ChainWorkflow", params={"n": 5, "spec": {"work_ms": 50}}, await_result=True),
    "fanout": dict(wf="FanoutWorkflow", params={"n": 100, "spec": {"work_ms": 50}}, await_result=True),
    "timer": dict(wf="TimerWorkflow", params={"min_s": 60, "max_s": 300}, await_result=False),
    "longlived": dict(wf="LongLivedWorkflow", params={"iterations": 1000}, await_result=False),
    "parent": dict(wf="ParentWorkflow", params={"n": 100}, await_result=True),
    "anatomy": dict(wf="AnatomyWorkflow", params={}, await_result=False),
    "poison": dict(wf="PoisonWorkflow", params={"mode": "raise"}, await_result=False),
    "evolving": dict(wf="EvolvingWorkflow", params={}, await_result=False),
    # Each operation is a signal-with-start to one of --keys workflow IDs (keys=1 => hot key).
    "signal": dict(wf="SignalSinkWorkflow", params={}, await_result=False, mode="signal", exec_timeout_s=900),
}
# Shapes whose params contain a `call` block (where --activity-queue is injected).
CALL_SHAPES = {"chain", "fanout", "longlived", "parent", "anatomy", "evolving"}


def deep_merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in over.items():
        out[k] = deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def err_label(e: BaseException) -> str:
    """Stable label for an exception: class name plus gRPC status if it has one."""
    status = getattr(e, "status", None)
    name = type(e).__name__
    return f"{name}:{status.name}" if status is not None and hasattr(status, "name") else name


def pct(sorted_vals: List[float], p: float) -> Optional[float]:
    if not sorted_vals:
        return None
    k = max(0, min(len(sorted_vals) - 1, math.ceil(p / 100.0 * len(sorted_vals)) - 1))
    return sorted_vals[k]


def build_params(cfg: dict) -> dict:
    shape = SHAPES[cfg["shape"]]
    params = dict(shape["params"])
    if cfg.get("params"):
        raw = cfg["params"]
        raw = open(raw[1:]).read() if raw.startswith("@") else raw
        params = deep_merge(params, json.loads(raw))
    if cfg.get("activity_queue") and cfg["shape"] in CALL_SHAPES:
        params = deep_merge(params, {"call": {"task_queue": cfg["activity_queue"]}})
    if cfg["shape"] == "noop" and cfg.get("payload_bytes"):
        params["payload"] = "x" * cfg["payload_bytes"]
    return params


def search_attrs(run_id: str, shape: str):
    from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes

    return TypedSearchAttributes(
        [
            SearchAttributePair(SearchAttributeKey.for_keyword("LabRunId"), run_id),
            SearchAttributePair(SearchAttributeKey.for_keyword("LabShape"), shape),
        ]
    )


# ----------------------------------------------------------------------------------
# Per-process generator
# ----------------------------------------------------------------------------------
class StepStats:
    def __init__(self) -> None:
        self.sent = 0
        self.ok = 0
        self.err: Counter = Counter()
        self.rpc_ms: List[float] = []
        self.intended_ms: List[float] = []
        self.lag_ms: List[float] = []
        self.e2e_ms: List[float] = []
        self.e2e_done = 0
        self.e2e_err: Counter = Counter()
        self.e2e_unfinished = 0

    def export(self) -> dict:
        r = lambda xs: [round(x, 1) for x in xs]  # noqa: E731
        return dict(
            sent=self.sent, ok=self.ok, err=dict(self.err),
            rpc_ms=r(self.rpc_ms), intended_ms=r(self.intended_ms), lag_ms=r(self.lag_ms),
            e2e_ms=r(self.e2e_ms), e2e_done=self.e2e_done, e2e_err=dict(self.e2e_err),
            e2e_unfinished=self.e2e_unfinished,
        )


async def _child(cfg: dict, idx: int, nprocs: int, t0_wall: float, q) -> None:
    shape = SHAPES[cfg["shape"]]
    wf_type = shape["wf"]
    mode = shape.get("mode", "start")
    params = build_params(cfg)
    do_await = cfg["await_result"] if cfg["await_result"] is not None else shape["await_result"]
    run_id = cfg["run_id"]
    task_queue = cfg["task_queue"]
    keys = cfg["keys"]
    payload = "x" * cfg["payload_bytes"]

    base = common.env_int("METRICS_PORT_BASE")
    client = await common.connect(
        metrics_port=(base + idx) if (base and idx < 4) else None, id_prefix=f"loadgen-{idx}"
    )
    sa = None if cfg["no_search_attrs"] else search_attrs(run_id, cfg["shape"])

    steps = [(r / nprocs, cfg["step_seconds"]) for r in cfg["rates"]]
    stats = [StepStats() for _ in steps]
    start_sem = asyncio.Semaphore(max(1, cfg["max_inflight"] // nprocs))
    result_sem = asyncio.Semaphore(cfg["max_result_waiters"])
    tasks: set = set()
    waiters: set = set()
    state = {"step": 0, "inflight": 0, "lag_max": 0.0}

    exec_timeout = cfg.get("exec_timeout_s") or shape.get("exec_timeout_s")
    common_kw: Dict[str, Any] = dict(task_queue=task_queue)
    if sa is not None:
        common_kw["search_attributes"] = sa
    if cfg.get("wft_timeout_s"):
        common_kw["task_timeout"] = timedelta(seconds=cfg["wft_timeout_s"])
    if exec_timeout:
        common_kw["execution_timeout"] = timedelta(seconds=exec_timeout)

    async def wait_result(handle, step_i: int, intended: float) -> None:
        st = stats[step_i]
        try:
            async with result_sem:
                await asyncio.wait_for(handle.result(), timeout=cfg["result_timeout_s"])
            st.e2e_ms.append((time.monotonic() - intended) * 1000.0)
            st.e2e_done += 1
        except asyncio.CancelledError:
            st.e2e_unfinished += 1
            raise
        except asyncio.TimeoutError:
            st.e2e_unfinished += 1
        except Exception as e:  # workflow failed / terminated / RPC error
            st.e2e_err[err_label(e)] += 1

    async def start_one(seq: int, step_i: int, intended: float) -> None:
        st = stats[step_i]
        try:
            key = ((seq * nprocs) + idx) % keys if keys else None
            wf_id = f"{run_id}-{cfg['shape']}-k{key}" if keys else f"{run_id}-{cfg['shape']}-{idx}-{seq}"
            t_send = time.monotonic()
            try:
                if mode == "signal":
                    handle = await client.start_workflow(
                        wf_type, params, id=wf_id, start_signal="push", start_signal_args=[payload], **common_kw
                    )
                else:
                    handle = await client.start_workflow(wf_type, params, id=wf_id, **common_kw)
            except Exception as e:
                st.err[err_label(e)] += 1
                return
            t_done = time.monotonic()
            st.ok += 1
            st.rpc_ms.append((t_done - t_send) * 1000.0)
            st.intended_ms.append((t_done - intended) * 1000.0)
            if do_await and mode != "signal":
                w = asyncio.create_task(wait_result(handle, step_i, intended))
                waiters.add(w)
                w.add_done_callback(waiters.discard)
        finally:
            state["inflight"] -= 1
            start_sem.release()

    async def ticker() -> None:
        k = 0
        while True:
            await asyncio.sleep(1.0)
            k += 1
            tot = lambda f: sum(f(s) for s in stats)  # noqa: E731
            q.put(("tick", idx, dict(
                k=k, step=state["step"], sent=tot(lambda s: s.sent), ok=tot(lambda s: s.ok),
                err=tot(lambda s: sum(s.err.values())), done=tot(lambda s: s.e2e_done),
                inflight=state["inflight"], waiting=len(waiters), lag_max=state["lag_max"],
            )))
            state["lag_max"] = 0.0

    # Synchronise the start across processes using wall-clock time.
    t_start = time.monotonic() + max(0.0, t0_wall - time.time())
    await asyncio.sleep(max(0.0, t_start - time.monotonic()))
    tick_task = asyncio.create_task(ticker())

    seq = 0
    step_start = t_start
    for step_i, (rate, dur) in enumerate(steps):
        state["step"] = step_i
        if rate <= 0:
            await asyncio.sleep(dur)
            step_start += dur
            continue
        interval = 1.0 / rate
        n_ops = int(rate * dur)
        for k in range(n_ops):
            intended = step_start + k * interval
            now = time.monotonic()
            if intended > now:
                await asyncio.sleep(intended - now)
            lag = (time.monotonic() - intended) * 1000.0
            stats[step_i].lag_ms.append(lag)
            state["lag_max"] = max(state["lag_max"], lag)
            await start_sem.acquire()  # back-pressure: if the server is slow this blocks and LAG grows (visible)
            state["inflight"] += 1
            stats[step_i].sent += 1
            t = asyncio.create_task(start_one(seq, step_i, intended))
            tasks.add(t)
            t.add_done_callback(tasks.discard)
            seq += 1
            if k % 20 == 19:
                await asyncio.sleep(0)  # let started tasks make progress when we are catching up
        step_start += dur
        # If the last op of the step finished early, wait for the step's wall-clock end.
        remaining = step_start - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(remaining)

    # Drain: outstanding start calls, then result waiters (bounded).
    if tasks:
        await asyncio.wait(set(tasks), timeout=cfg["drain_s"])
    if waiters:
        await asyncio.wait(set(waiters), timeout=cfg["drain_s"])
    for w in list(waiters):
        w.cancel()
    if waiters:
        await asyncio.gather(*list(waiters), return_exceptions=True)
    tick_task.cancel()
    q.put(("final", idx, [s.export() for s in stats]))


def child_main(cfg: dict, idx: int, nprocs: int, t0_wall: float, q) -> None:
    try:
        asyncio.run(_child(cfg, idx, nprocs, t0_wall, q))
    except BaseException as e:  # noqa: BLE001 - report every failure to the parent
        q.put(("fatal", idx, f"{type(e).__name__}: {e}"))
        raise


# ----------------------------------------------------------------------------------
# Parent: orchestrates processes, prints live progress, aggregates, writes results.
# ----------------------------------------------------------------------------------
def fmt(x: Optional[float], w: int = 8) -> str:
    return f"{'-':>{w}}" if x is None else f"{x:>{w}.1f}"


def summarize(cfg: dict, finals: Dict[int, List[dict]]) -> List[dict]:
    rows = []
    for si, rate in enumerate(cfg["rates"]):
        agg: Dict[str, Any] = defaultdict(list)
        sent = ok = done = unfin = 0
        err: Counter = Counter()
        e2e_err: Counter = Counter()
        for steps in finals.values():
            s = steps[si]
            sent += s["sent"]; ok += s["ok"]; done += s["e2e_done"]; unfin += s["e2e_unfinished"]
            err.update(s["err"]); e2e_err.update(s["e2e_err"])
            for k in ("rpc_ms", "intended_ms", "lag_ms", "e2e_ms"):
                agg[k].extend(s[k])
        for k in list(agg):
            agg[k].sort()
        dur = cfg["step_seconds"]
        rows.append(dict(
            step=si, target_rate=rate, duration_s=dur, sent=sent, ok=ok, errors=sum(err.values()),
            achieved_rate=round(ok / dur, 1),
            rpc_p50=pct(agg["rpc_ms"], 50), rpc_p99=pct(agg["rpc_ms"], 99), rpc_p999=pct(agg["rpc_ms"], 99.9),
            intended_p50=pct(agg["intended_ms"], 50), intended_p99=pct(agg["intended_ms"], 99),
            e2e_p50=pct(agg["e2e_ms"], 50), e2e_p99=pct(agg["e2e_ms"], 99), e2e_p999=pct(agg["e2e_ms"], 99.9),
            e2e_done=done, e2e_unfinished=unfin, e2e_errors=dict(e2e_err),
            lag_p99=pct(agg["lag_ms"], 99), lag_max=(agg["lag_ms"][-1] if agg["lag_ms"] else None),
            error_breakdown=dict(err),
        ))
    return rows


def print_table(cfg: dict, rows: List[dict]) -> None:
    print("\n" + "=" * 132)
    print(f"RESULTS run_id={cfg['run_id']} shape={cfg['shape']} processes={cfg['processes']} "
          f"step={cfg['step_seconds']}s await={cfg['await_effective']}")
    print("=" * 132)
    print(f"{'step':>4} {'target/s':>9} {'sent':>8} {'ok':>8} {'err':>6} {'ach/s':>8} | "
          f"{'rpc p50':>8} {'p99':>8} {'p99.9':>8} | {'e2e p50':>8} {'p99':>8} {'p99.9':>8} {'done':>7} {'unfin':>6} | "
          f"{'lag p99':>8}")
    for r in rows:
        print(f"{r['step']:>4} {r['target_rate']:>9.1f} {r['sent']:>8} {r['ok']:>8} {r['errors']:>6} {r['achieved_rate']:>8.1f} | "
              f"{fmt(r['rpc_p50'])} {fmt(r['rpc_p99'])} {fmt(r['rpc_p999'])} | "
              f"{fmt(r['e2e_p50'])} {fmt(r['e2e_p99'])} {fmt(r['e2e_p999'])} {r['e2e_done']:>7} {r['e2e_unfinished']:>6} | "
              f"{fmt(r['lag_p99'])}")
    print("(all latencies in ms; rpc = StartWorkflowExecution call; e2e = intended start -> result)")
    for r in rows:
        if r["error_breakdown"]:
            print(f"  step {r['step']} start errors: {r['error_breakdown']}")
        if r["e2e_errors"]:
            print(f"  step {r['step']} result errors: {r['e2e_errors']}")
        if r["lag_p99"] is not None and r["lag_p99"] > 250:
            print(f"  WARNING step {r['step']}: generator lag p99 = {r['lag_p99']:.0f} ms -> the LOAD GENERATOR is "
                  f"saturated (or blocked by --max-inflight); raise --processes / --max-inflight before trusting these numbers")
        if r["target_rate"] and r["achieved_rate"] < 0.95 * r["target_rate"]:
            print(f"  NOTE step {r['step']}: achieved {r['achieved_rate']}/s < 95% of target {r['target_rate']}/s")


async def final_counts(cfg: dict) -> None:
    """Ask visibility how the run's workflows ended up (eventually consistent)."""
    client = await common.connect(id_prefix="loadgen-count")
    q = f'LabRunId = "{cfg["run_id"]}"'
    try:
        res = await client.count_workflows(q + " GROUP BY ExecutionStatus")
        groups = {" ".join(map(str, getattr(g, "values", []))): g.count for g in getattr(res, "groups", [])}
        print(f"visibility: total={res.count} by status={groups or '(no grouping support)'}")
    except Exception as e:  # noqa: BLE001
        try:
            res = await client.count_workflows(q)
            print(f"visibility: total={res.count} (GROUP BY unavailable: {err_label(e)})")
        except Exception as e2:  # noqa: BLE001
            print(f"visibility count failed: {err_label(e2)}: {e2}")


def run_ramp(cfg: dict) -> int:
    shape = SHAPES[cfg["shape"]]
    cfg["await_effective"] = cfg["await_result"] if cfg["await_result"] is not None else shape["await_result"]
    nprocs = cfg["processes"]
    total = len(cfg["rates"]) * cfg["step_seconds"]
    print(f"run_id={cfg['run_id']} shape={cfg['shape']} workflow={shape['wf']} task_queue={cfg['task_queue']}")
    print(f"params={json.dumps(build_params(cfg))}")
    print(f"ramp={cfg['rates']} x {cfg['step_seconds']}s  processes={nprocs}  await={cfg['await_effective']}  "
          f"keys={cfg['keys'] or 'unique-ids'}")

    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    t0 = time.time() + 3.0 + 0.3 * nprocs  # give processes time to import + connect
    procs = [ctx.Process(target=child_main, args=(cfg, i, nprocs, t0, q), daemon=True) for i in range(nprocs)]
    for p in procs:
        p.start()

    finals: Dict[int, List[dict]] = {}
    ticks: Dict[int, Dict[int, dict]] = defaultdict(dict)
    last_totals = dict(sent=0, ok=0, err=0, done=0)
    deadline = t0 + total + cfg["drain_s"] * 2 + cfg["result_timeout_s"] + 30
    fatal = None
    try:
        while len(finals) < nprocs and time.time() < deadline:
            try:
                kind, idx, payload = q.get(timeout=1.0)
            except queue_mod.Empty:
                if not any(p.is_alive() for p in procs) and q.empty():
                    break
                continue
            if kind == "final":
                finals[idx] = payload
            elif kind == "fatal":
                fatal = f"process {idx}: {payload}"
                print(f"FATAL {fatal}", file=sys.stderr)
            elif kind == "tick":
                ticks[payload["k"]][idx] = payload
                if len(ticks[payload["k"]]) == nprocs:
                    t = ticks.pop(payload["k"]).values()
                    tot = {k: sum(x[k] for x in t) for k in ("sent", "ok", "err", "done")}
                    d = {k: tot[k] - last_totals[k] for k in tot}
                    last_totals = tot
                    print(f"t={payload['k']:>4}s step={max(x['step'] for x in t)} | sent/s={d['sent']:>6} ok/s={d['ok']:>6} "
                          f"err/s={d['err']:>5} done/s={d['done']:>6} | inflight={sum(x['inflight'] for x in t):>5} "
                          f"awaiting={sum(x['waiting'] for x in t):>6} | lag_max={max(x['lag_max'] for x in t):>7.0f}ms")
    except KeyboardInterrupt:
        print("\ninterrupted: stopping generators (partial results are discarded)")
        for p in procs:
            p.terminate()
        return 130
    for p in procs:
        p.join(timeout=5)

    if len(finals) < nprocs:
        print(f"ERROR: only {len(finals)}/{nprocs} generator processes reported results. {fatal or ''}", file=sys.stderr)
        return 1
    rows = summarize(cfg, finals)
    print_table(cfg, rows)

    os.makedirs("results", exist_ok=True)
    out = os.path.join("results", f"{cfg['run_id']}.jsonl")
    with open(out, "w") as f:
        f.write(json.dumps(dict(meta={k: v for k, v in cfg.items()}, params=build_params(cfg))) + "\n")
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {out}")
    if cfg["count_after_s"] >= 0:
        time.sleep(cfg["count_after_s"])
        asyncio.run(final_counts(cfg))
    return 0


# ----------------------------------------------------------------------------------
# smoke: run one small instance of every workflow type and check the answer.
# ----------------------------------------------------------------------------------
async def run_smoke(cfg: dict) -> int:
    client = await common.connect(id_prefix="smoke")
    run = f"smoke-{uuid.uuid4().hex[:6]}"
    tq = cfg["task_queue"]
    aq = cfg.get("activity_queue")
    call = {"task_queue": aq} if aq else {}
    results: List[tuple] = []

    async def case(name: str, coro) -> None:
        t = time.monotonic()
        try:
            detail = await asyncio.wait_for(coro, timeout=90)
            results.append((name, True, f"{detail} ({time.monotonic() - t:.1f}s)"))
        except Exception as e:  # noqa: BLE001
            results.append((name, False, f"{err_label(e)}: {str(e)[:100]}"))

    def start(name, wf, params, **kw):
        return client.start_workflow(wf, params, id=f"{run}-{name}", task_queue=tq, **kw)

    async def simple(name, wf, params, expect):
        h = await start(name, wf, params)
        r = await h.result()
        assert r == expect, f"expected {expect!r} got {r!r}"
        return f"result={r!r}"

    async def anatomy():
        h = await start("anatomy", "AnatomyWorkflow", {"timer_s": 1, "call": call})
        await asyncio.sleep(2.5)
        stage = await h.query("stage")
        n = await h.execute_update("add_note", "hello")
        await h.signal("proceed")
        r = await h.result()
        return f"stage@2.5s={stage!r} update->{n} result={r!r}"

    async def sink():
        h = await start("sink", "SignalSinkWorkflow", {"expected": 3}, start_signal="push", start_signal_args=["a"])
        await h.signal("push", "b")
        await h.signal("push", "c")
        r = await h.result()
        assert r == 3, r
        return f"counted {r} signals"

    async def evolving():
        h = await start("evolving", "EvolvingWorkflow", {"call": call})
        await asyncio.sleep(2.0)
        await h.signal("proceed")
        r = await h.result()
        return f"result={r!r}  (worker variant decides the A/B order)"

    async def poison_raise():
        h = await start("poison", "PoisonWorkflow", {"mode": "raise"}, task_timeout=timedelta(seconds=10))
        await asyncio.sleep(4)
        d = await h.describe()
        await h.terminate(reason="smoke test cleanup")
        assert d.status is not None and d.status.name == "RUNNING", d.status
        return f"status={d.status.name} (workflow TASK keeps failing; workflow is still Running)"

    async def poison_fail():
        h = await start("poison-fail", "PoisonWorkflow", {"mode": "fail"})
        try:
            await h.result()
        except Exception as e:  # noqa: BLE001
            return f"failed as designed: {type(e).__name__}"
        raise AssertionError("workflow should have failed")

    sync_call = {"activity": "work_sync", **call}
    await case("noop", simple("noop", "NoopWorkflow", {}, "ok"))
    await case("chain (async activity)", simple("chain", "ChainWorkflow", {"n": 3, "call": call}, 3))
    await case("chain (sync activity)", simple("chain-sync", "ChainWorkflow", {"n": 3, "call": sync_call}, 3))
    await case("fanout", simple("fanout", "FanoutWorkflow", {"n": 20, "call": call}, 20))
    await case("timer", _timer(client, run, tq))
    await case("longlived + continue-as-new", simple("longlived", "LongLivedWorkflow",
               {"iterations": 5, "sleep_ms": 10, "can_mode": "every", "can_every": 2, "call": call}, 5))
    await case("parent/child", simple("parent", "ParentWorkflow", {"n": 5, "call": call}, 5))
    await case("anatomy (signal/query/update)", anatomy())
    await case("signal sink", sink())
    await case("evolving", evolving())
    await case("poison: raise (task failure loop)", poison_raise())
    await case("poison: fail (workflow failure)", poison_fail())

    print(f"\nSMOKE run={run} task_queue={tq} activity_queue={aq}")
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name:<36} {detail}")
    bad = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(bad)}/{len(results)} passed")
    return 1 if bad else 0


async def _timer(client, run: str, tq: str) -> str:
    h = await client.start_workflow("TimerWorkflow", {"min_s": 1, "max_s": 2}, id=f"{run}-timer", task_queue=tq)
    r = await h.result()
    assert 1 <= r <= 2, r
    return f"slept {r:.2f}s"


# ----------------------------------------------------------------------------------
def parse(argv: Optional[List[str]] = None) -> dict:
    ap = argparse.ArgumentParser(prog="lab.load", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--task-queue", default=os.environ.get("TASK_QUEUE", "lab-tq"), help="queue the WORKFLOW runs on")
        p.add_argument("--activity-queue", default=None, help="route activities to this queue (split worker fleets)")

    r = sub.add_parser("ramp", help="run a rate ramp")
    common_args(r)
    r.add_argument("--shape", required=True, choices=sorted(SHAPES))
    r.add_argument("--rates", type=lambda s: [float(x) for x in s.split(",")], help="comma list of starts/sec, one per step")
    r.add_argument("--rate", type=float, help="single rate (with --duration); shorthand for --rates R --step-seconds D")
    r.add_argument("--duration", type=int, help="seconds for --rate")
    r.add_argument("--step-seconds", type=int, default=60)
    r.add_argument("--processes", type=int, default=1, help="generator processes (each does rate/N)")
    r.add_argument("--params", default=None, help="JSON (or @file) deep-merged over the shape's default params")
    r.add_argument("--keys", type=int, default=0, help="cycle workflow IDs over K keys (1 = hot key). 0 = unique IDs")
    r.add_argument("--payload-bytes", type=int, default=0, help="signal / noop payload size")
    r.add_argument("--await", dest="await_result", action=argparse.BooleanOptionalAction, default=None,
                   help="wait for each workflow result (e2e latency). Default depends on shape")
    r.add_argument("--max-inflight", type=int, default=2000, help="cap on concurrent start RPCs (all processes)")
    r.add_argument("--max-result-waiters", type=int, default=20000)
    r.add_argument("--result-timeout-s", type=float, default=120.0)
    r.add_argument("--drain-s", type=float, default=30.0, help="how long to wait for stragglers after the last step")
    r.add_argument("--wft-timeout-s", type=float, default=None, help="workflow task timeout (default server: 10s)")
    r.add_argument("--exec-timeout-s", type=float, default=None, help="workflow execution timeout (leak guard)")
    r.add_argument("--run-id", default=None)
    r.add_argument("--no-search-attrs", action="store_true", help="do not set LabRunId/LabShape (if not registered)")
    r.add_argument("--count-after-s", type=int, default=5, help="seconds to wait then count by status in visibility (-1 = skip)")

    s = sub.add_parser("smoke", help="run one instance of every workflow type and check results")
    common_args(s)

    a = ap.parse_args(argv)
    cfg = vars(a)
    if cfg["cmd"] == "ramp":
        if cfg["rate"] is not None:
            cfg["rates"] = [cfg["rate"]]
            cfg["step_seconds"] = cfg["duration"] or cfg["step_seconds"]
        if not cfg["rates"]:
            ap.error("give --rates or --rate")
        cfg["run_id"] = cfg["run_id"] or f"{cfg['shape']}-{time.strftime('%H%M%S')}-{uuid.uuid4().hex[:4]}"
    return cfg


def main() -> None:
    cfg = parse()
    if cfg["cmd"] == "smoke":
        sys.exit(asyncio.run(run_smoke(cfg)))
    sys.exit(run_ramp(cfg))


if __name__ == "__main__":
    main()
