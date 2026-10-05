"""Workflow definitions ONLY (no activity code in this file: the SDK sandbox re-imports
this module for every workflow instance, so keep it small).

Activities are referenced by *name* (strings) so this module never imports activity code.
All workflow code here must be deterministic: no wall-clock time, no `random`, no I/O.
"""
import asyncio
import dataclasses
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from lab import variant
    from lab.models import (
        AnatomyParams,
        CallCfg,
        ChainParams,
        ChurnParams,
        ChildParams,
        EvolvingParams,
        FanoutParams,
        LongLivedParams,
        NoopParams,
        ParentParams,
        PoisonParams,
        SinkParams,
        TimerParams,
        WorkSpec,
    )


def _td(seconds: Optional[float]) -> Optional[timedelta]:
    return None if seconds is None else timedelta(seconds=seconds)


async def _call(cfg: CallCfg, spec: WorkSpec, activity_id: Optional[str] = None):
    """Schedule one activity according to `cfg` (timeouts, retry policy, routing)."""
    return await workflow.execute_activity(
        cfg.activity,
        spec,
        task_queue=cfg.task_queue,
        start_to_close_timeout=_td(cfg.start_to_close_s),
        schedule_to_start_timeout=_td(cfg.schedule_to_start_s),
        schedule_to_close_timeout=_td(cfg.schedule_to_close_s),
        heartbeat_timeout=_td(cfg.heartbeat_timeout_s),
        retry_policy=RetryPolicy(
            initial_interval=timedelta(milliseconds=cfg.initial_interval_ms),
            backoff_coefficient=cfg.backoff,
            maximum_interval=timedelta(seconds=cfg.max_interval_s),
            maximum_attempts=cfg.max_attempts,
        ),
        activity_id=activity_id,
    )


# --------------------------------------------------------------------------------
# noop: start -> complete. Stresses the start path + one workflow task, nothing else.
# --------------------------------------------------------------------------------
@workflow.defn
class NoopWorkflow:
    @workflow.run
    async def run(self, p: NoopParams) -> str:
        return "ok"


# --------------------------------------------------------------------------------
# chain: N sequential activities. Also the vehicle for slow / hanging / heartbeating /
# flaky / big-payload activities (set `call` and `spec`).
# --------------------------------------------------------------------------------
@workflow.defn
class ChainWorkflow:
    @workflow.run
    async def run(self, p: ChainParams) -> int:
        for _ in range(p.n):
            await _call(p.call, p.spec)
        return p.n


# --------------------------------------------------------------------------------
# fanout: N parallel activities. Failures are counted, not raised, so retry storms
# and schedule_to_close timeouts do not fail the workflow. Returns #successes.
# --------------------------------------------------------------------------------
@workflow.defn
class FanoutWorkflow:
    @workflow.run
    async def run(self, p: FanoutParams) -> int:
        sem = asyncio.Semaphore(p.max_parallel or max(p.n, 1))

        async def one():
            async with sem:
                return await _call(p.call, p.spec)

        results = await asyncio.gather(*(one() for _ in range(p.n)), return_exceptions=True)
        ok = sum(1 for r in results if not isinstance(r, BaseException))
        if ok != p.n:
            workflow.logger.warning("fanout finished with failures", extra={"ok": ok, "n": p.n})
        return ok


# --------------------------------------------------------------------------------
# timer: sleep for a random duration. Timer-storm experiments.
# --------------------------------------------------------------------------------
@workflow.defn
class TimerWorkflow:
    @workflow.run
    async def run(self, p: TimerParams) -> float:
        d = workflow.random().uniform(p.min_s, p.max_s)  # deterministic: seeded per run
        await workflow.sleep(d)
        return d


# --------------------------------------------------------------------------------
# sink: counts signals. Signal firehose (many signals -> few workflows) and hot key
# (all load on one workflow ID). No timers on purpose: the ONLY history growth is the
# signals themselves. The load generator sets an execution timeout as a leak guard.
# --------------------------------------------------------------------------------
@workflow.defn
class SignalSinkWorkflow:
    @workflow.init
    def __init__(self, p: SinkParams) -> None:
        self._count = p.count_so_far
        self._stop = False

    @workflow.signal
    def push(self, payload: str = "") -> None:
        self._count += 1

    @workflow.signal
    def stop(self) -> None:
        self._stop = True

    @workflow.query
    def count(self) -> int:
        return self._count

    @workflow.run
    async def run(self, p: SinkParams) -> int:
        await workflow.wait_condition(
            lambda: self._stop
            or bool(p.expected and self._count >= p.expected)
            or (p.can_on_suggest and workflow.info().is_continue_as_new_suggested())
        )
        if self._stop or (p.expected and self._count >= p.expected):
            return self._count
        # Server suggested continue-as-new: carry the counter into a fresh history.
        await workflow.wait_condition(workflow.all_handlers_finished)
        workflow.continue_as_new(
            SinkParams(expected=p.expected, can_on_suggest=True, count_so_far=self._count)
        )


# --------------------------------------------------------------------------------
# churn: continue-as-new on every run, nothing else. Every run of one workflow ID lives on the
# same history shard, so a tight CAN loop is a single-ID hot shard (python -m lab.hotshard --kind can).
# --------------------------------------------------------------------------------
@workflow.defn
class ChurnWorkflow:
    @workflow.run
    async def run(self, p: ChurnParams) -> int:
        if p.remaining <= 0:
            return p.done
        workflow.continue_as_new(ChurnParams(remaining=p.remaining - 1, done=p.done + 1))


# --------------------------------------------------------------------------------
# longlived: activity + timer in a loop => history grows by ~8-11 events/iteration.
#   can_mode=never      -> run into the history-size warning and hard limit (Phase 4 #15)
#   can_mode=suggested  -> continue-as-new when the server says so
#   can_mode=every      -> continue-as-new every `can_every` iterations
# --------------------------------------------------------------------------------
@workflow.defn
class LongLivedWorkflow:
    @workflow.run
    async def run(self, p: LongLivedParams) -> int:
        done = p.done
        ran = 0
        while done < p.iterations:
            await _call(p.call, p.spec)
            if p.sleep_ms:
                await workflow.sleep(timedelta(milliseconds=p.sleep_ms))
            done += 1
            ran += 1
            want_can = (p.can_mode == "suggested" and workflow.info().is_continue_as_new_suggested()) or (
                p.can_mode == "every" and ran >= p.can_every
            )
            if want_can and done < p.iterations:
                workflow.continue_as_new(dataclasses.replace(p, done=done))
        return done


# --------------------------------------------------------------------------------
# parent/child: cross-shard traffic (each child ID hashes to a different history shard).
# --------------------------------------------------------------------------------
@workflow.defn
class ChildWorkflow:
    @workflow.run
    async def run(self, p: ChildParams) -> int:
        if p.work_ms:
            await _call(p.call, WorkSpec(work_ms=p.work_ms))
        return 1


_PARENT_CLOSE = {
    "terminate": workflow.ParentClosePolicy.TERMINATE,
    "abandon": workflow.ParentClosePolicy.ABANDON,
    "request_cancel": workflow.ParentClosePolicy.REQUEST_CANCEL,
}


@workflow.defn
class ParentWorkflow:
    @workflow.run
    async def run(self, p: ParentParams) -> int:
        sem = asyncio.Semaphore(p.max_parallel or max(p.n, 1))
        parent_id = workflow.info().workflow_id

        async def one(i: int):
            async with sem:
                return await workflow.execute_child_workflow(
                    ChildWorkflow.run,
                    ChildParams(work_ms=p.child_work_ms, call=p.call),
                    id=f"{parent_id}-c{i}",
                    parent_close_policy=_PARENT_CLOSE[p.parent_close_policy],
                )

        results = await asyncio.gather(*(one(i) for i in range(p.n)), return_exceptions=True)
        return sum(1 for r in results if not isinstance(r, BaseException))


# --------------------------------------------------------------------------------
# anatomy (Phase 1): activity -> timer -> wait for signal -> activity, plus a query and
# an update handler so the history shows every event family.
# --------------------------------------------------------------------------------
def _probe(step: str) -> None:
    # INTENTIONAL print(): NOT replay-safe, unlike workflow.logger. Every time this worker
    # replays the workflow (cache eviction, worker restart) the earlier lines print again
    # with replaying=True. That repetition IS the lesson about deterministic replay.
    print(
        f"[replay-probe] wf={workflow.info().workflow_id} step={step} replaying={workflow.unsafe.is_replaying()}",
        flush=True,
    )


@workflow.defn
class AnatomyWorkflow:
    def __init__(self) -> None:
        self._proceed = False
        self._stage = "init"
        self._notes: list[str] = []

    @workflow.signal
    def proceed(self) -> None:
        self._proceed = True

    @workflow.query
    def stage(self) -> str:
        return self._stage

    @workflow.update
    def add_note(self, note: str) -> int:
        self._notes.append(note)
        return len(self._notes)

    @workflow.run
    async def run(self, p: AnatomyParams) -> str:
        _probe("start")
        self._stage = "activity-1"
        await _call(p.call, p.spec, activity_id="anatomy-1")
        _probe("after-activity-1")
        self._stage = "timer"
        await workflow.sleep(p.timer_s)
        _probe("after-timer")
        self._stage = "waiting-for-signal"
        await workflow.wait_condition(lambda: self._proceed)
        _probe("after-signal")
        self._stage = "activity-2"
        await _call(p.call, p.spec, activity_id="anatomy-2")
        self._stage = "done"
        await workflow.wait_condition(workflow.all_handlers_finished)
        return f"done (notes={len(self._notes)})"


# --------------------------------------------------------------------------------
# poison: workflows that break in different ways (Phase 4 #12).
#   raise                 -> RuntimeError = WORKFLOW TASK failure. The workflow stays Running and
#                            the task is retried forever with backoff. Nothing "fails".
#   raise_after_activity  -> same, but every retry first REPLAYS the completed activity.
#   cpu                   -> busy loop; the SDK's deadlock detector (~2 s without yielding)
#                            fails the workflow task ("Potential deadlock detected").
#   fail                  -> ApplicationError = the WORKFLOW fails, once, and is done.
# --------------------------------------------------------------------------------
@workflow.defn
class PoisonWorkflow:
    @workflow.run
    async def run(self, p: PoisonParams) -> str:
        if p.mode == "raise":
            raise RuntimeError("poison pill: raising a plain exception fails the workflow TASK, not the workflow")
        if p.mode == "raise_after_activity":
            await _call(CallCfg(), WorkSpec(work_ms=10))
            raise RuntimeError("poison pill after an activity: each retry replays the activity result")
        if p.mode == "cpu":
            x = 0
            for i in range(p.spin_iters):
                x += i
            return str(x)
        if p.mode == "fail":
            raise ApplicationError("business failure: this fails the workflow once", non_retryable=True)
        return "unknown mode"


# --------------------------------------------------------------------------------
# evolving (Phase 4 #11): the SAME workflow type with different code per deployment.
#   v1: A, B, wait for signal, C
#   v2: B, A, wait for signal, C      <- swapping the order breaks replay of v1 histories
#   v3: v2 guarded by workflow.patched("swap-ab") <- old histories keep A,B; new ones run B,A
# Start executions under v1, let them park at the signal, redeploy with v2, then signal them.
# --------------------------------------------------------------------------------
@workflow.defn
class EvolvingWorkflow:
    def __init__(self) -> None:
        self._go = False

    @workflow.signal
    def proceed(self) -> None:
        self._go = True

    @workflow.run
    async def run(self, p: EvolvingParams) -> str:
        v = variant.VARIANT
        if v == "v1":
            order = ["A", "B"]
        elif v == "v2":
            order = ["B", "A"]
        else:
            order = ["B", "A"] if workflow.patched("swap-ab") else ["A", "B"]
        for name in order:
            await _call(p.call, p.spec, activity_id=f"step-{name}")
        await workflow.wait_condition(lambda: self._go)
        await _call(p.call, p.spec, activity_id="step-C")
        return f"{v}:{''.join(order)}C"


ALL_WORKFLOWS = [
    NoopWorkflow,
    ChainWorkflow,
    FanoutWorkflow,
    TimerWorkflow,
    SignalSinkWorkflow,
    ChurnWorkflow,
    LongLivedWorkflow,
    ChildWorkflow,
    ParentWorkflow,
    AnatomyWorkflow,
    PoisonWorkflow,
    EvolvingWorkflow,
]
