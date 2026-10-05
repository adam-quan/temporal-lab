"""Activities. Keep this file free of workflow code (see the SDK's file-organisation guidance).

Two flavours with identical behaviour, so you can compare them under load:

  work_async  - `async def`, runs ON the worker's asyncio event loop. Cheap to run thousands
                concurrently (asyncio.sleep), but any blocking call (cpu_ms > 0!) freezes
                the loop: pollers, heartbeats and workflow tasks on that worker all stall.
  work_sync   - plain `def`, runs in the ThreadPoolExecutor (ACTIVITY_THREADS). Safe to
                block, but concurrency is capped by the thread count, and CPU-bound work
                still fights over the GIL.
"""
import asyncio
import random
import time

from temporalio import activity
from temporalio.exceptions import ApplicationError

from lab.models import WorkSpec


def _burn_cpu(ms: int) -> None:
    """Pure-Python busy loop for `ms` milliseconds (holds the GIL between switch intervals)."""
    end = time.perf_counter() + ms / 1000.0
    x = 0
    while time.perf_counter() < end:
        x += 1


def _maybe_fail(spec: WorkSpec) -> None:
    attempt = activity.info().attempt
    if spec.fail_first_attempts and attempt <= spec.fail_first_attempts:
        raise ApplicationError(
            f"injected failure on attempt {attempt}/{spec.fail_first_attempts}",
            non_retryable=spec.non_retryable,
        )
    if spec.fail_rate and random.random() < spec.fail_rate:
        raise ApplicationError(f"injected random failure (rate={spec.fail_rate})", non_retryable=spec.non_retryable)


def _result(spec: WorkSpec) -> str:
    return "x" * spec.response_bytes if spec.response_bytes else "ok"


@activity.defn
async def work_async(spec: WorkSpec) -> str:
    _maybe_fail(spec)
    if spec.cpu_ms:
        _burn_cpu(spec.cpu_ms)  # INTENTIONAL: blocks the event loop. Watch what else on this worker slows down.
    remaining = spec.work_ms / 1000.0
    hb = spec.heartbeat_every_ms / 1000.0
    started = time.monotonic()
    if hb > 0:
        while remaining > 0:
            step = min(hb, remaining)
            await asyncio.sleep(step)
            remaining -= step
            activity.heartbeat(int((time.monotonic() - started) * 1000))
    else:
        await asyncio.sleep(remaining)
    return _result(spec)


@activity.defn
def work_sync(spec: WorkSpec) -> str:
    _maybe_fail(spec)
    if spec.cpu_ms:
        _burn_cpu(spec.cpu_ms)
    remaining = spec.work_ms / 1000.0
    hb = spec.heartbeat_every_ms / 1000.0
    started = time.monotonic()
    if hb > 0:
        while remaining > 0:
            step = min(hb, remaining)
            time.sleep(step)
            remaining -= step
            activity.heartbeat(int((time.monotonic() - started) * 1000))
    else:
        time.sleep(remaining)
    return _result(spec)


ALL_ACTIVITIES = [work_async, work_sync]
