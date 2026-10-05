"""Shared dataclasses: the *parameters* of workflows and activities.

Workflows and activities take a single dataclass argument. The load generator
never imports these: it starts workflows *by type name* with a plain dict, and the
worker's data converter turns the dict into the dataclass using the type hints.
Anything the dict omits falls back to the defaults below. That is what lets
`--params '{"n": 500, "spec": {"work_ms": 200}}'` work for every experiment.
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class WorkSpec:
    """What an activity DOES when it runs."""

    work_ms: int = 50              # simulated I/O wait (asyncio.sleep / time.sleep)
    cpu_ms: int = 0                # pure-Python busy loop (holds the GIL; in async activities it blocks the event loop!)
    heartbeat_every_ms: int = 0    # 0 = never heartbeat
    payload_in: str = ""           # ballast carried in the activity input
    response_bytes: int = 0        # size of the string the activity returns
    fail_rate: float = 0.0         # probability [0,1] of raising a retryable error
    fail_first_attempts: int = 0   # deterministically fail attempts 1..N (retryable unless non_retryable)
    non_retryable: bool = False    # make injected failures non-retryable
    tag: str = ""                  # free label, shows up in activity logs


@dataclass
class CallCfg:
    """HOW a workflow calls its activity: type, timeouts, retry policy, routing."""

    activity: str = "work_async"   # work_async | work_sync
    start_to_close_s: float = 30.0
    schedule_to_start_s: Optional[float] = None
    schedule_to_close_s: Optional[float] = None
    heartbeat_timeout_s: Optional[float] = None
    max_attempts: int = 0          # 0 = unlimited (the server default)
    initial_interval_ms: int = 1000
    backoff: float = 2.0
    max_interval_s: float = 100.0
    task_queue: Optional[str] = None   # None = same queue as the workflow. Set for split fleets.


# ----------------------------- workflow params ---------------------------------
@dataclass
class NoopParams:
    payload: str = ""


@dataclass
class ChainParams:
    n: int = 5
    call: CallCfg = field(default_factory=CallCfg)
    spec: WorkSpec = field(default_factory=WorkSpec)


@dataclass
class FanoutParams:
    n: int = 100
    max_parallel: int = 0          # 0 = schedule all n at once
    call: CallCfg = field(default_factory=CallCfg)
    spec: WorkSpec = field(default_factory=WorkSpec)


@dataclass
class TimerParams:
    min_s: float = 60.0
    max_s: float = 300.0


@dataclass
class SinkParams:
    """Receives signals. Used for the signal-firehose and hot-key experiments."""

    expected: int = 0              # complete after this many signals (0 = wait for `stop`)
    can_on_suggest: bool = False   # continue-as-new when the server suggests it
    count_so_far: int = 0          # carried across continue-as-new


@dataclass
class ChurnParams:
    """Continue-as-new immediately, `remaining` more times (hot-shard experiment, Kind 2)."""

    remaining: int = 1_000_000
    done: int = 0              # carried across continue-as-new


@dataclass
class LongLivedParams:
    iterations: int = 1000
    sleep_ms: int = 100            # timer between iterations (0 = none)
    can_mode: str = "never"        # never | suggested | every
    can_every: int = 500           # iterations per run when can_mode == "every"
    done: int = 0                  # carried across continue-as-new
    call: CallCfg = field(default_factory=CallCfg)
    spec: WorkSpec = field(default_factory=lambda: WorkSpec(work_ms=10))


@dataclass
class ChildParams:
    work_ms: int = 0
    call: CallCfg = field(default_factory=CallCfg)


@dataclass
class ParentParams:
    n: int = 100
    max_parallel: int = 0          # 0 = start all children at once
    child_work_ms: int = 0         # 0 = child does no activity at all
    parent_close_policy: str = "terminate"   # terminate | abandon | request_cancel
    call: CallCfg = field(default_factory=CallCfg)


@dataclass
class AnatomyParams:
    """activity -> timer -> wait for signal -> activity (Phase 1)."""

    timer_s: float = 5.0
    call: CallCfg = field(default_factory=CallCfg)
    spec: WorkSpec = field(default_factory=lambda: WorkSpec(work_ms=100))


@dataclass
class PoisonParams:
    mode: str = "raise"            # raise | raise_after_activity | cpu | fail
    spin_iters: int = 150_000_000  # cpu mode: pure-Python loop iterations (~5-10 s)


@dataclass
class EvolvingParams:
    call: CallCfg = field(default_factory=CallCfg)
    spec: WorkSpec = field(default_factory=lambda: WorkSpec(work_ms=50))
