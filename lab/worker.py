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
    client = await common.connect(metrics_port=metrics_port, id_prefix=f"worker-{role}")

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
