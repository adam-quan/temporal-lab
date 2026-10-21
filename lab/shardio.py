"""Shard IO concurrency test: reproduce writes queuing inside shards, walk the playbook's decision, verify the fix.

Follows the playbook
https://github.com/tsurdilo/temporal-server-operations/blob/main/playbooks/shard-io-concurrency.md
(and its alerts 34j / 34f). Run it through `make shardio`, which applies the server-side settings first:

    make shardio ARGS=list                          # the scenarios
    make shardio ARGS="run --scenario issue"        # shardIOConcurrency 1: writes queue in the shards (Steps 1-5 say RAISE)
    make shardio ARGS="run --scenario raise2"       # Step 6/7: 2, compared with the last `issue` run
    make shardio ARGS="run --scenario raise4"       # Step 6/7: 4, and Step 4 says stop here (no pool room for 5)
    make shardio ARGS="run --scenario slowdb"       # Step 2 trap: the database is the slow part (Postgres CPU-capped)
    make shardio ARGS="run --scenario slowdb4"      # ...and raising anyway does not help
    make shardio ARGS="run --scenario pool"         # Step 3 trap: maxConns 4 = ceiling 1, raised to 4 anyway
    make shardio ARGS=readings                      # the playbook's panels for the last few minutes
    make shardio ARGS=reset                         # remove the test's dynamic-config block (after a crash)

What a run does (inside the loadgen container; tools/shardio.sh does the server side around it)
------------------------------------------------------------------------------------------------
`history.shardIOConcurrency` is read when a history pod takes ownership of a shard, so every scenario needs a
history restart: tools/shardio.sh writes the value (`prepare`), recreates history (with the scenario's SQL pool
size) and caps Postgres' CPU when the scenario says so, runs this module, then puts everything back.

1. Wait until history owns every shard again after the restart.
2. Start --workflows SignalSinkWorkflows on a task queue with NO worker: they sit open and only collect signals.
3. Load: plain SignalWorkflowExecution calls at --rate per second for --load-s seconds, open loop (sent on schedule
   whatever the server does), spread over all the workflows. A signal is one write of execution state: it takes an
   IO slot on the workflow's shard, so with 4 shards every shard gets a quarter of the writes. Plain signals skip
   the frontend's search-attribute lock that caps starts in this lab, so the shards are the first thing to fill up.
4. Read the playbook's panels over the steady part of the load and walk Steps 1-5 (Step 7 against the last `issue`
   run for the raise scenarios). The verdict checks that the scenario showed what it is meant to show.

The lab needs few shards per history pod for writes to queue inside a shard: `make reset SHARDS=4` (the .env default).
"""
import argparse
import asyncio
import glob
import json
import multiprocessing as mp
import os
import sys
import time
import uuid
from collections import defaultdict
from datetime import timedelta
from typing import Dict, List, Optional

from lab import common
from lab.historytasks import apply_block, remove_block
from lab.hotshard import detector, prom, prom1, shape_of
from lab.workerperf import pct

BEGIN = "# >>> lab.shardio managed block: removed when the run ends (`make shardio ARGS=reset` after a crash)"
END = "# <<< lab.shardio"
H = 'service_name="history"'

# The playbook's rules of thumb (not server limits).
SEM_ORANGE_MS, SEM_RED_MS = 20.0, 100.0      # Step 1: Shard IO Semaphore Latency (High Priority)
DB_FAST_MS, DB_SLOW_MS = 10.0, 50.0          # Step 2: Persistence Write Latency bands

SCENARIOS: Dict[str, dict] = {
    "issue": dict(title="shardIOConcurrency 1 (the default): writes queue inside the shards",
                  sio=1, expect="raise"),
    "raise2": dict(title="Step 6/7: shardIOConcurrency 2, the first step up", sio=2, expect="better"),
    "raise4": dict(title="Step 6/7: shardIOConcurrency 4, after which Step 1 says stop raising", sio=4,
                   expect="better+stop"),
    "slowdb": dict(title="Step 2 trap: the database is the slow part (Postgres capped at 0.25 CPU)",
                   sio=1, db_cpus=0.25, expect="fix-db"),
    "slowdb4": dict(title="Step 2 trap, raised anyway: shardIOConcurrency 4 on the same slow database",
                    sio=4, db_cpus=0.25, expect="no-gain"),
    "pool": dict(title="Step 3 trap: maxConns 4 (ceiling 4/4 shards = 1), shardIOConcurrency raised to 4 anyway",
                 sio=4, max_conns=4, expect="pool"),
}


# ----------------------------------------------------------------------------------
# readings: the Shard IO Concurrency dashboard's panels as numbers
# ----------------------------------------------------------------------------------
def readings(w: int, at: float) -> dict:
    win = f"{w}s"

    def q(quant: float, expr: str, by: str = "le") -> Optional[float]:
        v = prom1(f"histogram_quantile({quant}, sum by ({by}) (rate({expr}[{win}])))", at)
        return None if v is None else v * 1000

    def rate(expr: str) -> float:
        return prom1(f"sum(rate({expr}[{win}]))", at) or 0.0

    d: dict = {}
    sem = 'semaphore_latency_bucket{operation="ShardInfo",priority="0"}'
    d["sem_p99_ms"], d["sem_p50_ms"] = q(0.99, sem), q(0.50, sem)          # Step 1
    d["sem_low_p99_ms"] = q(0.99, 'semaphore_latency_bucket{operation="ShardInfo",priority="1"}')
    d["sem_failures"] = rate('semaphore_failures{operation="ShardInfo"}')
    d["sem_requests"] = rate('semaphore_requests{operation="ShardInfo"}')
    dl = prom(f"histogram_quantile(0.99, sum by (instance, le) (rate(dd_shard_io_semaphore_latency_bucket{{{H}}}"
              f"[{max(w, 150)}s])))", at)                                    # section 5 (once a minute)
    d["deadlock_check_ms"] = max(dl.values()) * 1000 if dl else None
    for op in ("UpdateWorkflowExecution", "CreateWorkflowExecution", "AddTasks"):   # Step 2
        d[f"write_p99_ms_{op}"] = q(0.99, f'persistence_latency_bucket{{operation="{op}"}}')
    d["write_p99_ms"] = max((v for k, v in d.items() if k.startswith("write_p99_ms_") and v is not None), default=None)
    d["write_p50_ms"] = q(0.50, 'persistence_latency_bucket{operation="UpdateWorkflowExecution"}')
    ceil = prom(f'persistence_sql_max_open_conn{{db_kind="main"}} / on(instance) group_left() '
                f'numshards_gauge{{operation="ShardController"}}', at)                # Step 3
    d["ceiling"] = min(ceil.values()) if ceil else None
    d["max_conns"] = prom1(f'max(persistence_sql_max_open_conn{{{H},db_kind="main"}})', at)
    d["shards_per_pod"] = prom1(f'max(numshards_gauge{{{H},operation="ShardController"}})', at)
    util = f'persistence_sql_in_use{{{H},db_kind="main"}} / persistence_sql_max_open_conn{{{H},db_kind="main"}}'
    d["pool_util_max"] = prom1(f"max(max_over_time(({util})[{win}:5s]))", at)          # Step 4
    d["pool_util_avg"] = prom1(f"max(avg_over_time(({util})[{win}:5s]))", at)
    d["resource_exhausted"] = rate(f"persistence_errors_resource_exhausted{{{H}}}")
    d["hot"] = detector(max(w, 60), at)                                                 # Step 1 item 4
    d["hot_shape"] = shape_of(d["hot"])
    d["signal_p99_ms_history"] = q(0.99, f'service_latency_bucket{{{H},operation="SignalWorkflowExecution"}}')
    d["signals_history"] = rate(f'service_requests{{{H},operation="SignalWorkflowExecution"}}')
    return d


def walk(m: dict) -> dict:
    """Steps 1-5 of the playbook on one set of readings."""
    s = {}
    sem = m["sem_p99_ms"] or 0.0
    s["step1"] = "red" if sem >= SEM_RED_MS else "orange" if sem >= SEM_ORANGE_MS else "low"
    s["hot"] = m["hot"].get("max_over_p50") is not None and not m["hot_shape"].startswith(("flat", "no data"))
    wr = m["write_p99_ms"]
    s["step2"] = None if wr is None else "fast" if wr < DB_FAST_MS else "moderate" if wr <= DB_SLOW_MS else "slow"
    c = m["ceiling"]
    s["step3"] = None if c is None else "unset" if c == 0 else "below2" if c < 2 else "ok"
    limit = (1 - 1 / c) if c and c >= 1 else None
    s["pool_limit"] = limit
    s["step4"] = None if limit is None or m["pool_util_max"] is None else m["pool_util_max"] <= limit
    if m["resource_exhausted"] > 0.1:
        s["step4"] = False
    if s["step1"] == "low":
        dec = "nothing to do: writes are not waiting for IO slots"
    elif s["hot"]:
        dec = "fix the hot shard first (Hot Shard playbook)"
    elif s["step2"] == "slow":
        dec = "DO NOT RAISE: the database is the slow part, fix it first"
    elif s["step3"] == "below2":
        dec = "DO NOT RAISE YET: the pool cannot serve 2 writes per shard; raise maxConns or add history pods"
    elif s["step4"] is False:
        dec = "DO NOT RAISE (further): no pool room for +1; raise maxConns first"
    else:
        dec = "RAISE history.shardIOConcurrency by 1"
    s["decision"] = dec
    return s


# ----------------------------------------------------------------------------------
# load: open-loop plain signals from several processes
# ----------------------------------------------------------------------------------
async def _blast(idx: int, nproc: int, run_id: str, n_wf: int, rate: float, t0: float, t1: float, cap: int, q) -> None:
    client = await common.connect(id_prefix=f"shardio-load-{idx}")
    handles = [client.get_workflow_handle(f"{run_id}-sink-{i}") for i in range(n_wf)]
    per = rate / nproc
    lat: List[float] = []
    errs: Dict[str, int] = defaultdict(int)
    sent = ok = skipped = 0
    inflight = set()

    async def one(i: int) -> None:
        nonlocal ok
        t = time.monotonic()
        try:
            await handles[i].signal("push", "")
            ok += 1
            lat.append((time.monotonic() - t) * 1000)
        except Exception as e:  # noqa: BLE001
            errs[type(e).__name__] += 1

    k = 0
    while True:
        when = t0 + k / per
        if when >= t1:
            break
        await asyncio.sleep(max(0.0, when - time.time()))
        if len(inflight) >= cap:
            skipped += 1                       # the generator's own limit: counted, never hidden
        else:
            task = asyncio.create_task(one((idx + k * nproc) % n_wf))
            inflight.add(task)
            task.add_done_callback(inflight.discard)
            sent += 1
        k += 1
    if inflight:
        await asyncio.wait(set(inflight), timeout=60)
    q.put(dict(sent=sent, ok=ok, skipped=skipped, errors=dict(errs), lat=lat))


def _child(*args) -> None:
    asyncio.run(_blast(*args))


async def start_sinks(client, run_id: str, tq: str, n: int) -> None:
    sem = asyncio.Semaphore(100)

    async def one(i: int) -> None:
        async with sem:
            await client.start_workflow("SignalSinkWorkflow", {}, id=f"{run_id}-sink-{i}", task_queue=tq,
                                        execution_timeout=timedelta(minutes=10))   # they expire on their own
    await asyncio.gather(*(one(i) for i in range(n)))


async def wait_for_shards(timeout_s: float = 180) -> Optional[float]:
    """After a history restart: until every shard is owned again and stays owned for 10 s."""
    deadline, steady_since = time.time() + timeout_s, None
    while time.time() < deadline:
        owned = prom1(f'sum(numshards_gauge{{{H},operation="ShardController"}})')
        total = prom1('max(numshards_gauge{operation="ShardController"})')
        if owned and total and owned >= total:
            steady_since = steady_since or time.time()
            if time.time() - steady_since >= 10:
                return owned
        else:
            steady_since = None
        await asyncio.sleep(2)
    return None


async def wait_for_quiet(limit: float = 20.0, timeout_s: float = 900) -> Optional[float]:
    """Earlier runs leave work behind (their sink workflows time out 10 minutes later). Measure on a quiet history."""
    deadline, said = time.time() + timeout_s, False
    while time.time() < deadline:
        busy = prom1(f"sum(rate(task_requests{{{H}}}[1m]))") or 0.0
        if busy < limit:
            return busy
        if not said:
            print(f"waiting for history to go quiet: {busy:.0f} history tasks/s (< {limit:.0f} wanted)")
            said = True
        await asyncio.sleep(10)
    return None


# ----------------------------------------------------------------------------------
# run
# ----------------------------------------------------------------------------------
async def run(cfg: dict) -> int:
    sc = SCENARIOS[cfg["scenario"]]
    run_id = cfg["run_id"] or f"shardio-{cfg['scenario']}-{time.strftime('%H%M%S')}-{uuid.uuid4().hex[:4]}"
    tq = f"sio-{run_id}"
    print(f"shardio run_id={run_id} scenario={cfg['scenario']}: {sc['title']}")
    owned = await wait_for_shards()
    if owned is None:
        print("history does not own all shards (did it restart?)")
        return 2
    client = await common.connect(id_prefix="shardio")
    quiet = await wait_for_quiet()
    if quiet is None:
        print("history is still busy with earlier work (e.g. a previous run's workflows timing out); try again later")
        return 2
    if owned > 16:
        print(f"WARNING: {owned:.0f} shards on this history pod. IO slots are per shard, so with many shards the "
              f"writes rarely queue inside one shard. Use `make reset SHARDS=4` (see the guide).")
    print(f"history owns {owned:.0f} shards; starting {cfg['workflows']} signal-sink workflows (no worker)")
    await start_sinks(client, run_id, tq, cfg["workflows"])
    await asyncio.sleep(5)

    nproc = cfg["processes"]
    t0 = time.time() + 3
    t1 = t0 + cfg["load_s"]
    print(f"load: {cfg['rate']:.0f} signals/s for {cfg['load_s']} s from {nproc} processes "
          f"(at most {cfg['max_inflight']} in flight each)")
    ctx = mp.get_context("spawn")
    qq = ctx.Queue()
    procs = [ctx.Process(target=_child, args=(i, nproc, run_id, cfg["workflows"], cfg["rate"], t0, t1,
                                               cfg["max_inflight"], qq)) for i in range(nproc)]
    for p in procs:
        p.start()
    last = 0.0
    while time.time() < t1:
        await asyncio.sleep(1)
        now = time.time()
        if now - last >= 20 and now > t0 + 15:
            r = readings(15, now)
            print(f"  t={now - t0:>4.0f}s  signals {r['signals_history']:>6.0f}/s  IO slot wait p99 "
                  f"{r['sem_p99_ms'] or 0:>7.1f} ms  DB write p99 {r['write_p99_ms'] or 0:>6.1f} ms  "
                  f"pool {100 * (r['pool_util_max'] or 0):>4.0f}%")
            last = now
    res = [qq.get(timeout=120) for _ in procs]
    for p in procs:
        p.join(timeout=10)
    t_end = time.time()
    if t_end - t1 > 150:
        print(f"\nRESULT: INVALID (the test was paused: keep the machine awake, `caffeinate -dimsu make shardio ...`)")
        return 2

    w = max(30, int(cfg["load_s"] - 30))                  # skip the first 30 s: ramp-up
    m = readings(w, t1)
    lat = [x for r in res for x in r["lat"]]
    errs: Dict[str, int] = defaultdict(int)
    for r in res:
        for k, v in r["errors"].items():
            errs[k] += v
    m.update(rate=cfg["rate"], load_s=cfg["load_s"], sent=sum(r["sent"] for r in res), ok=sum(r["ok"] for r in res),
             skipped=sum(r["skipped"] for r in res), errors=dict(errs), sig_p50=pct(lat, 50), sig_p99=pct(lat, 99),
             window=[t1 - w, t1])
    m["achieved"] = m["ok"] / cfg["load_s"]
    s = walk(m)
    report(cfg, sc, run_id, m, s)
    return verdict(cfg, sc, run_id, m, s)


# ----------------------------------------------------------------------------------
# report and verdict
# ----------------------------------------------------------------------------------
def pf(x, d=1) -> str:
    return "-" if x is None else f"{x:.{d}f}"


def report(cfg, sc, run_id, m, s) -> None:
    print("\n" + "=" * 100)
    print(f"SHARD IO CONCURRENCY TEST  scenario={cfg['scenario']}  history.shardIOConcurrency={sc['sio']}"
          + (f"  Postgres CPU cap {sc['db_cpus']}" if sc.get("db_cpus") else "")
          + (f"  maxConns {sc['max_conns']}" if sc.get("max_conns") else ""))
    print("=" * 100)
    print(f"load       {m['rate']:.0f}/s target -> {m['achieved']:.0f}/s signals accepted"
          + (f"; {m['skipped']} not sent (the generator's in-flight cap was full)" if m["skipped"] else "")
          + f"; client signal latency p50 {pf(m['sig_p50'], 0)} ms, p99 {pf(m['sig_p99'], 0)} ms"
          + (f"; errors {m['errors']}" if m["errors"] else ""))
    print(f"\nStep 1     Shard IO Semaphore Latency (High Priority) p99 {pf(m['sem_p99_ms'])} ms (p50 "
          f"{pf(m['sem_p50_ms'])})  -> {dict(low='below 20 ms', orange='20 ms or more', red='above 100 ms')[s['step1']]}"
          f";  low priority p99 {pf(m['sem_low_p99_ms'])} ms")
    print(f"           Shard IO Semaphore Failures {m['sem_failures']:.2f}/s;  deadlock check p99 "
          f"{pf(m['deadlock_check_ms'])} ms (alert 34j at 20 s)")
    hot = m["hot"]
    print(f"           hot shard? per-shard RPS p50 {pf(hot.get('shard_rps_p50'))}, max {pf(hot.get('shard_rps_max'))}"
          f"  -> {m['hot_shape']}")
    print(f"Step 2     Persistence Write Latency p99: UpdateWorkflowExecution "
          f"{pf(m['write_p99_ms_UpdateWorkflowExecution'])} ms, CreateWorkflowExecution "
          f"{pf(m['write_p99_ms_CreateWorkflowExecution'])}, AddTasks {pf(m['write_p99_ms_AddTasks'])}  -> highest "
          f"{pf(m['write_p99_ms'])} ms: {dict(fast='under 10 ms, the database answers quickly', moderate='10-50 ms, some room', slow='above 50 ms, THE DATABASE IS THE SLOW PART').get(s['step2'], '-')}")
    if s["step2"] == "slow" and (m["pool_util_max"] or 0) >= 0.95:
        print("           (the connection pool is full, see Step 4: this latency includes waiting for a connection, so the "
              "'database' to fix may be the pool itself)")
    print(f"Step 3     ceiling = maxConns {pf(m['max_conns'], 0)} / {pf(m['shards_per_pod'], 0)} shards per pod = "
          f"{pf(m['ceiling'], 2)}  -> "
          f"{dict(unset='maxConns not set', below2='BELOW 2: the pool cannot serve 2 writes per shard', ok='the highest value shardIOConcurrency may go').get(s['step3'], '-')}")
    print(f"Step 4     pool utilization max {pf(100 * (m['pool_util_max'] or 0), 0)}% (avg "
          f"{pf(100 * (m['pool_util_avg'] or 0), 0)}%) vs the limit for +1, 1 - 1/ceiling = "
          f"{pf(100 * s['pool_limit'], 0) if s['pool_limit'] is not None else '-'}%;  resource exhausted "
          f"{m['resource_exhausted']:.2f}/s  -> {'room for +1' if s['step4'] else 'NO room for +1' if s['step4'] is False else '-'}")
    print(f"\nStep 5     {s['decision']}")


def last_result(scenario: str) -> Optional[dict]:
    files = sorted(glob.glob(f"results/shardio-{scenario}-*.json"), key=os.path.getmtime)
    for f in reversed(files):
        with open(f) as fh:
            d = json.load(fh)
        if d.get("passed") is not None:
            return d
    return None


def step7(m: dict, prev: dict) -> dict:
    """Step 7: compare with the readings before the change."""
    p = prev["metrics"]
    out = dict(
        sem_lower=(m["sem_p99_ms"] or 0) < (p["sem_p99_ms"] or 0),
        failures_ok=m["sem_failures"] <= max(p["sem_failures"], 0.01),
        write_ok=(m["write_p99_ms"] or 0) <= DB_SLOW_MS,
        pool_ok=(m["pool_util_max"] or 0) < 0.98 and m["resource_exhausted"] <= 0.1,
        deadlock_ok=(m["deadlock_check_ms"] or 0) < 1000,
        throughput=(m["achieved"] / p["achieved"]) if p.get("achieved") else None,
    )
    print(f"\nStep 7     vs the last `issue` run ({prev['run_id']}):")
    print(f"           IO slot wait p99 {pf(p['sem_p99_ms'])} -> {pf(m['sem_p99_ms'])} ms "
          f"({'lower' if out['sem_lower'] else 'NOT lower: roll back'});  failures {p['sem_failures']:.2f} -> "
          f"{m['sem_failures']:.2f}/s;  DB write p99 {pf(p['write_p99_ms'])} -> {pf(m['write_p99_ms'])} ms "
          f"({'under 50 ms' if out['write_ok'] else 'ABOVE 50 ms: roll back'});  pool max "
          f"{pf(100 * (p['pool_util_max'] or 0), 0)}% -> {pf(100 * (m['pool_util_max'] or 0), 0)}%"
          f" ({'ok' if out['pool_ok'] else 'AT THE LIMIT: roll back'})")
    print(f"           signals accepted {p['achieved']:.0f}/s -> {m['achieved']:.0f}/s"
          + (f" ({out['throughput']:.2f}x)" if out["throughput"] else ""))
    return out


def verdict(cfg, sc, run_id, m, s) -> int:
    expect = sc["expect"]
    extra: dict = {}
    if expect == "raise":
        ok = s["decision"].startswith("RAISE") and s["step1"] in ("orange", "red")
        why = "Steps 1-5 land on RAISE: writes wait >= 20 ms for IO slots, the database is fast, the pool has room"
    elif expect in ("better", "better+stop"):
        prev = last_result("issue")
        if not prev:
            ok, why = False, "run `make shardio ARGS=\"run --scenario issue\"` first: Step 7 compares with it"
        else:
            c = step7(m, prev)
            extra["step7"] = c
            ok = c["sem_lower"] and c["write_ok"] and c["pool_ok"] and (c["throughput"] or 0) >= 0.97
            why = "Step 7: IO slot wait lower, database still under 50 ms, pool not exhausted, throughput kept or up"
            if expect == "better+stop":
                ok = ok and s["step1"] == "low"
                why += "; and Step 1 now reads below 20 ms, so the next pass through Steps 1-5 stops here"
    elif expect == "fix-db":
        ok = s["step2"] == "slow" and s["step1"] in ("orange", "red")
        why = "writes wait for IO slots AND the database is above 50 ms: Step 2 stops you (fix the database)"
    elif expect == "no-gain":
        prev = last_result("slowdb")
        if not prev:
            ok, why = False, "run the `slowdb` scenario first"
        else:
            gain = m["achieved"] / prev["metrics"]["achieved"] if prev["metrics"]["achieved"] else 0
            extra["gain_vs_slowdb"] = gain
            print(f"\nvs slowdb   signals accepted {prev['metrics']['achieved']:.0f}/s -> {m['achieved']:.0f}/s "
                  f"({gain:.2f}x);  DB write p99 {pf(prev['metrics']['write_p99_ms'])} -> {pf(m['write_p99_ms'])} ms")
            ok = gain < 1.25 and s["step2"] == "slow"
            why = "4 IO slots per shard on a slow database: under 1.25x the throughput, and the database still slow"
    elif expect == "pool":
        ok = s["step3"] == "below2" and (m["pool_util_max"] or 0) >= 0.95
        why = "Step 3: ceiling below 2, and with 4 IO slots per shard the 4-connection pool runs full"
    else:
        ok, why = False, "unknown expectation"
    print(f"\nRESULT: {'PASS' if ok else 'FAIL'} ({why})")
    os.makedirs("results", exist_ok=True)
    out = f"results/{run_id}.json"
    with open(out, "w") as f:
        json.dump(dict(cfg=cfg, scenario=cfg["scenario"], run_id=run_id, sio=sc["sio"], metrics=m, steps=s,
                       passed=ok, **extra), f, indent=1, default=str)
    print(f"wrote {out}")
    return 0 if ok else 1


# ----------------------------------------------------------------------------------
# commands used by tools/shardio.sh
# ----------------------------------------------------------------------------------
def cmd_env(scenario: str) -> None:
    """Shell variables for tools/shardio.sh: the server-side settings of a scenario."""
    sc = SCENARIOS[scenario]
    print(f"SIO={sc['sio']} DB_CPUS={sc.get('db_cpus', 0)} MAX_CONNS={sc.get('max_conns', 20)}")


def cmd_readings(minutes: int) -> int:
    m = readings(minutes * 60, time.time())
    m.update(rate=0, achieved=0, skipped=0, errors={}, sig_p50=None, sig_p99=None)
    s = walk(m)
    report(dict(scenario="(live readings)"), dict(sio="?"), "-", m, s)
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(prog="lab.shardio", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    sub.add_parser("reset")
    e = sub.add_parser("env")
    e.add_argument("--scenario", required=True, choices=list(SCENARIOS))
    pr = sub.add_parser("prepare")
    pr.add_argument("--scenario", required=True, choices=list(SCENARIOS))
    rd = sub.add_parser("readings")
    rd.add_argument("--minutes", type=int, default=3)
    r = sub.add_parser("run")
    r.add_argument("--scenario", required=True, choices=list(SCENARIOS))
    r.add_argument("--rate", type=float, default=1000.0, help="signals/s")
    r.add_argument("--load-s", type=int, default=150)
    r.add_argument("--workflows", type=int, default=2000)
    r.add_argument("--processes", type=int, default=4)
    r.add_argument("--max-inflight", type=int, default=256, help="per process")
    r.add_argument("--run-id", default=None)
    a = ap.parse_args()
    if a.cmd == "list":
        for k, v in SCENARIOS.items():
            extra = "".join(f"  {n}={v[n]}" for n in ("db_cpus", "max_conns") if n in v)
            print(f"{k:<8} shardIOConcurrency={v['sio']}{extra}\n{'':<8} {v['title']}")
        sys.exit(0)
    if a.cmd == "env":
        cmd_env(a.scenario)
        sys.exit(0)
    if a.cmd == "prepare":
        sc = SCENARIOS[a.scenario]
        apply_block({"history.shardIOConcurrency": sc["sio"]}, BEGIN, END)
        print(f"dynamic config: history.shardIOConcurrency = {sc['sio']} (takes effect when history restarts)")
        sys.exit(0)
    if a.cmd == "reset":
        print("removed" if remove_block(BEGIN, END) else "no managed block found")
        sys.exit(0)
    if a.cmd == "readings":
        sys.exit(cmd_readings(a.minutes))
    rc = asyncio.run(run(vars(a)))
    sys.stdout.flush()
    os._exit(rc)   # see lab/historytasks.py: never hang at interpreter shutdown after results are written


if __name__ == "__main__":
    main()
