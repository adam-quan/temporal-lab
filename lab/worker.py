"""Configurable Temporal worker for the lab.

Every knob you will want to turn during the experiments is an environment variable
(see docker-compose.yml / .env). Unset or empty means "use the SDK default", so the
baseline run is the SDK's own defaults.

ROLE
  all       poll workflow + activity tasks on TASK_QUEUE (default)
  workflow  poll workflow tasks only        (pair with an activity-role fleet)
  activity  poll activity tasks only

Run:  python -m lab.worker
"""
import asyncio
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio.worker import Worker

from lab import common
from lab.activities import ALL_ACTIVITIES
from lab.workflows import ALL_WORKFLOWS

log = logging.getLogger("lab.worker")


def build_kwargs(role: str, executor: ThreadPoolExecutor) -> dict:
    """Translate env vars into Worker() keyword arguments (only those that are set)."""
    kw: dict = {"task_queue": os.environ.get("TASK_QUEUE", common.DEFAULT_TASK_QUEUE)}
    if role in ("all", "workflow"):
        kw["workflows"] = ALL_WORKFLOWS
    if role in ("all", "activity"):
        kw["activities"] = ALL_ACTIVITIES
        kw["activity_executor"] = executor

    # --- compute: slots -------------------------------------------------------------
    tuner = common.env_str("TUNER", "fixed")
    if tuner == "resource":
        # Resource-based slot suppliers: the worker admits tasks while CPU/memory are under target.
        # NOTE: mutually exclusive with max_concurrent_* (the SDK errors if both are given), and
        # `temporal_worker_task_slots_available` is NOT reported for resource-based suppliers.
        from temporalio.worker import WorkerTuner

        kw["tuner"] = WorkerTuner.create_resource_based(
            target_memory_usage=common.env_float("TUNER_TARGET_MEM", 0.8),
            target_cpu_usage=common.env_float("TUNER_TARGET_CPU", 0.9),
        )
    else:
        for env, arg in (
            ("MAX_CONCURRENT_WORKFLOW_TASKS", "max_concurrent_workflow_tasks"),
            ("MAX_CONCURRENT_ACTIVITIES", "max_concurrent_activities"),
        ):
            v = common.env_int(env)
            if v is not None:
                kw[arg] = v

    # --- memory: sticky workflow cache ---------------------------------------------
    v = common.env_int("MAX_CACHED_WORKFLOWS")
    if v is not None:
        kw["max_cached_workflows"] = v

    # --- IO: pollers ------------------------------------------------------------------
    if common.env_str("POLLER_AUTOSCALE", "0") == "1":
        try:
            from temporalio.worker import PollerBehaviorAutoscaling
        except ImportError:
            sys.exit("POLLER_AUTOSCALE=1 needs a temporalio release with PollerBehaviorAutoscaling "
                     "and Temporal Server >= 1.28")
        kw["workflow_task_poller_behavior"] = PollerBehaviorAutoscaling()
        kw["activity_task_poller_behavior"] = PollerBehaviorAutoscaling()
    else:
        for env, arg in (
            ("MAX_CONCURRENT_WORKFLOW_TASK_POLLS", "max_concurrent_workflow_task_polls"),
            ("MAX_CONCURRENT_ACTIVITY_TASK_POLLS", "max_concurrent_activity_task_polls"),
        ):
            v = common.env_int(env)
            if v is not None:
                kw[arg] = v

    # --- misc ----------------------------------------------------------------------------
    v = common.env_float("STICKY_QUEUE_TIMEOUT_S")
    if v is not None:
        kw["sticky_queue_schedule_to_start_timeout"] = timedelta(seconds=v)
    for env, arg in (
        ("MAX_ACTIVITIES_PER_SECOND", "max_activities_per_second"),
        ("MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND", "max_task_queue_activities_per_second"),
    ):
        v = common.env_float(env)
        if v is not None:
            kw[arg] = v
    if common.env_str("DISABLE_EAGER_ACTIVITIES", "0") == "1":
        kw["disable_eager_activity_execution"] = True
    if common.env_str("DEBUG_MODE", "0") == "1":
        # Disables the SDK's workflow deadlock detector (~2 s without yielding), so a CPU-bound
        # workflow runs until the SERVER's workflow-task timeout fires instead (Phase 3A-4).
        kw["debug_mode"] = True
    kw["graceful_shutdown_timeout"] = timedelta(seconds=common.env_float("GRACEFUL_SHUTDOWN_S", 5.0))
    return kw


LOOP_PROBE_S = 0.1


async def watch_event_loop(state: dict) -> None:
    """Event-loop lag probe: sleep 100 ms and record how late we wake up. A blocking call in `async def`
    code (time.sleep, requests.get, CPU loops) shows up here before anything else."""
    while True:
        t = time.monotonic()
        state["tick"] = t
        await asyncio.sleep(LOOP_PROBE_S)
        state["max_lag"] = max(state["max_lag"], (time.monotonic() - t - LOOP_PROBE_S) * 1000.0)


def instrument_loop_busy(loop, state: dict) -> bool:
    """Event-loop utilization: time the loop spends idle in select() vs wall time (like Node.js ELU).
    A loop that is ~100% busy while the process uses little CPU is blocked on synchronous I/O.
    Wraps the default SelectorEventLoop's private selector; returns False where that is not available (uvloop)."""
    sel = getattr(loop, "_selector", None)
    if sel is None:
        return False
    orig = sel.select

    def select(timeout=None):
        t = time.perf_counter()
        try:
            return orig(timeout)
        finally:
            state["idle_s"] += time.perf_counter() - t

    sel.select = select
    return True


def report_process_stats(runtime, loop_state: dict, every_s: float = 2.0) -> None:
    """Export this process's CPU, memory and event-loop lag as SDK custom metrics (no `temporal_` prefix):

      worker_process_cpu_cores   CPU used by the whole process (Python + the SDK's Rust core threads), in cores.
                                 A GIL-bound Python worker tops out near 1.0 for Python work.
      worker_process_rss_bytes   resident memory
      worker_event_loop_lag_ms   worst asyncio event-loop lag in the last interval (0 = healthy)
      worker_event_loop_busy_ratio  fraction of the interval the event loop was NOT idle (0..1)

    Step 1 of every schedule-to-start diagnosis is "check Worker CPU and memory", and cAdvisor cannot see
    containers on Docker Desktop. Runs on a thread so a blocked event loop cannot freeze the numbers.
    """
    meter = runtime.metric_meter
    cpu = meter.create_gauge_float("worker_process_cpu_cores", "CPU used by the worker process, in cores")
    rss = meter.create_gauge("worker_process_rss_bytes", "resident memory of the worker process", "bytes")
    lag = meter.create_gauge_float("worker_event_loop_lag_ms", "worst asyncio event-loop lag in the last interval", "ms")
    busy = meter.create_gauge_float("worker_event_loop_busy_ratio", "fraction of time the event loop was not idle")
    page = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096

    def loop() -> None:
        last_cpu, last_wall, last_idle = time.process_time(), time.monotonic(), loop_state["idle_s"]
        while True:
            time.sleep(every_s)
            now_cpu, now_wall, now_idle = time.process_time(), time.monotonic(), loop_state["idle_s"]
            cpu.set((now_cpu - last_cpu) / (now_wall - last_wall))
            if loop_state["busy_enabled"]:
                busy.set(min(1.0, max(0.0, 1.0 - (now_idle - last_idle) / (now_wall - last_wall))))
            last_cpu, last_wall, last_idle = now_cpu, now_wall, now_idle
            # If the loop is stuck right now the probe has not returned yet: count the ongoing stall too.
            ongoing = (now_wall - loop_state["tick"] - LOOP_PROBE_S) * 1000.0 if loop_state["tick"] else 0.0
            lag.set(max(loop_state["max_lag"], ongoing, 0.0))
            loop_state["max_lag"] = 0.0
            try:
                with open("/proc/self/statm") as f:
                    rss.set(int(f.read().split()[1]) * page)
            except OSError:
                pass  # not Linux: CPU only

    threading.Thread(target=loop, name="process-stats", daemon=True).start()


async def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    role = common.env_str("ROLE", "all")
    assert role in ("all", "workflow", "activity"), f"bad ROLE {role!r}"

    # Threads for *sync* activities. The SDK requires threads >= max_concurrent_activities.
    max_act = common.env_int("MAX_CONCURRENT_ACTIVITIES")
    threads = common.env_int("ACTIVITY_THREADS") or (max_act or 100)
    if common.env_str("TUNER", "fixed") != "resource" and threads < (max_act or 100):
        sys.exit(
            f"ACTIVITY_THREADS={threads} < max_concurrent_activities={max_act or 100}: "
            "set MAX_CONCURRENT_ACTIVITIES <= ACTIVITY_THREADS (SDK requirement for sync activities)"
        )

    metrics_port = common.env_int("METRICS_PORT")
    runtime = common.make_runtime(metrics_port)
    loop_state = {"tick": 0.0, "max_lag": 0.0, "idle_s": 0.0}
    loop_state["busy_enabled"] = instrument_loop_busy(asyncio.get_running_loop(), loop_state)
    asyncio.get_running_loop().create_task(watch_event_loop(loop_state))
    if runtime is not None:
        report_process_stats(runtime, loop_state)
        client = await common.connect(id_prefix=f"worker-{role}", runtime=runtime)
    else:
        client = await common.connect(id_prefix=f"worker-{role}")

    with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="activity") as executor:
        kw = build_kwargs(role, executor)
        worker = Worker(client, identity=common.identity(f"worker-{role}"), **kw)

        printable = {k: (v if isinstance(v, (int, float, str, timedelta, bool)) else type(v).__name__) for k, v in kw.items()}
        log.info("worker up: role=%s namespace=%s address=%s metrics_port=%s threads=%d", role, common.NAMESPACE,
                 common.ADDRESS, metrics_port, threads)
        log.info("worker config: %s", printable)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            # SIGTERM (docker stop) = graceful drain; SIGKILL (docker kill / kill -9) cannot be caught.
            loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.shutdown()))
        await worker.run()
    log.info("worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
