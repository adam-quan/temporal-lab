"""Introspection helpers for the lab.

  python -m lab.probe history <workflow-id> [--run-id R]
        Annotated event-history timeline: who produced each event, time since start,
        gap since the previous event, plus derived latencies (workflow-task and activity
        schedule->start) and totals. This is the Phase 1 "anatomy of a workflow" tool.

  python -m lab.probe count [--run-id R | --query Q]
        Count workflows in visibility, grouped by ExecutionStatus.

  python -m lab.probe shard <workflow-id> --shards N
        Which history shard owns this workflow ID? (needs `pyfarmhash`)
        VERIFY against the server:  make cli-tdbg ARGS="workflow describe -w <id>"

  python -m lab.probe shard-dist --pattern 'order-{i}' --count 100000 --shards 512
        How evenly does your workflow-ID scheme spread over the shards?
"""
import argparse
import asyncio
import statistics
import sys
import uuid
from collections import Counter
from typing import Dict, List, Optional

from lab import common

# Who produces each event, and what it means. "cmd" = a Command the worker returned when it
# completed a workflow task; the server turns it into the event. Wording is deliberately terse.
LEGEND: Dict[str, str] = {
    "WorkflowExecutionStarted": "server: StartWorkflowExecution accepted (frontend -> history shard)",
    "WorkflowTaskScheduled": "server: history queued a workflow task (transfer task -> matching)",
    "WorkflowTaskStarted": "server: a worker's long-poll was matched to the task",
    "WorkflowTaskCompleted": "worker->server: commands in the NEXT events were produced by this task",
    "WorkflowTaskFailed": "worker->server: task failed (exception / non-determinism); will be retried",
    "WorkflowTaskTimedOut": "server: worker did not answer within the workflow task timeout",
    "ActivityTaskScheduled": "cmd: ScheduleActivityTask",
    "ActivityTaskStarted": "server: activity worker matched (written LAZILY on completion when retries are possible)",
    "ActivityTaskCompleted": "activity worker->server: result",
    "ActivityTaskFailed": "activity worker->server: attempt failed and retry policy is exhausted / non-retryable",
    "ActivityTaskTimedOut": "server: a timeout fired (start-to-close / schedule-to-start / heartbeat)",
    "TimerStarted": "cmd: StartTimer",
    "TimerFired": "server: timer queue fired it (fire time is server wall-clock)",
    "TimerCanceled": "cmd: CancelTimer",
    "WorkflowExecutionSignaled": "client via frontend: signal delivered",
    "WorkflowExecutionUpdateAccepted": "worker: update handler accepted",
    "WorkflowExecutionUpdateCompleted": "worker: update handler finished",
    "MarkerRecorded": "cmd: RecordMarker (SDK uses these for patched() / side effects)",
    "WorkflowExecutionCompleted": "cmd: CompleteWorkflowExecution",
    "WorkflowExecutionFailed": "cmd: FailWorkflowExecution",
    "WorkflowExecutionContinuedAsNew": "cmd: ContinueAsNewWorkflowExecution (closes this run)",
    "WorkflowExecutionTerminated": "client: terminate",
    "StartChildWorkflowExecutionInitiated": "cmd: StartChildWorkflowExecution",
    "ChildWorkflowExecutionStarted": "server: child accepted (child lives on ANOTHER shard)",
    "ChildWorkflowExecutionCompleted": "server: child result delivered to parent",
}

SCALAR_FIELDS = ("identity", "attempt", "scheduled_event_id", "started_event_id", "timer_id", "signal_name",
                 "activity_id", "marker_name", "cause", "workflow_task_completed_event_id", "timeout_type",
                 "retry_state", "initiated_event_id", "workflow_id")


def camel(enum_name: str) -> str:
    return "".join(p.capitalize() for p in enum_name.replace("EVENT_TYPE_", "").split("_"))


def brief(attrs) -> str:
    """One-line summary of the interesting fields of an event's attributes message."""
    parts: List[str] = []
    try:
        for f in attrs.DESCRIPTOR.fields:
            n = f.name
            if n in SCALAR_FIELDS:
                v = getattr(attrs, n)
                if f.enum_type is not None:
                    v = f.enum_type.values_by_number[v].name.split("_", 2)[-1] if v else ""
                if v in ("", 0):
                    continue
                parts.append(f"{n}={v}")
            elif n in ("activity_type", "workflow_type") and attrs.HasField(n):
                parts.append(f"{n}={getattr(attrs, n).name}")
            elif n == "task_queue" and attrs.HasField(n):
                tq = getattr(attrs, n)
                sticky = tq.kind == 2  # TASK_QUEUE_KIND_STICKY
                parts.append("task_queue=STICKY" if sticky else f"task_queue={tq.name}")
            elif n == "failure" and attrs.HasField(n):
                parts.append(f"failure={getattr(attrs, n).message[:70]!r}")
    except Exception as e:  # never let formatting kill the timeline
        parts.append(f"<unformattable: {type(e).__name__}>")
    return " ".join(parts)


async def cmd_history(args) -> int:
    from temporalio.api.enums.v1 import EventType

    client = await common.connect(id_prefix="probe")
    handle = client.get_workflow_handle(args.workflow_id, run_id=args.run_id)
    history = await handle.fetch_history()
    events = list(history.events)
    if not events:
        print("no events")
        return 1

    t0 = events[0].event_time.ToNanoseconds()
    prev = t0
    sched_at: Dict[int, int] = {}
    wft_s2s: List[float] = []
    act_s2s: List[float] = []
    counts: Counter = Counter()
    print(f"{'id':>4} {'+ms':>9} {'gap':>8}  {'event':<38} details")
    for ev in events:
        name = camel(EventType.Name(ev.event_type))
        counts[name] += 1
        t = ev.event_time.ToNanoseconds()
        which = ev.WhichOneof("attributes")
        attrs = getattr(ev, which) if which else None
        details = brief(attrs) if attrs is not None else ""
        print(f"{ev.event_id:>4} {(t - t0) / 1e6:>9.1f} {(t - prev) / 1e6:>8.1f}  {name:<38} {details}")
        if args.legend and name in LEGEND:
            print(f"{'':>24}   ^ {LEGEND[name]}")
        prev = t
        if name in ("WorkflowTaskScheduled", "ActivityTaskScheduled"):
            sched_at[ev.event_id] = t
        elif name == "WorkflowTaskStarted" and attrs is not None and attrs.scheduled_event_id in sched_at:
            wft_s2s.append((t - sched_at[attrs.scheduled_event_id]) / 1e6)
        elif name == "ActivityTaskStarted" and attrs is not None and attrs.scheduled_event_id in sched_at:
            act_s2s.append((t - sched_at[attrs.scheduled_event_id]) / 1e6)

    size = sum(e.ByteSize() for e in events)
    print("\n--- summary " + "-" * 60)
    print(f"events={len(events)}  approx_history_bytes={size}  workflow_tasks={counts['WorkflowTaskScheduled']}  "
          f"wft_failed={counts['WorkflowTaskFailed']}  wft_timed_out={counts['WorkflowTaskTimedOut']}")
    if wft_s2s:
        print(f"workflow task schedule->start ms: min={min(wft_s2s):.1f} max={max(wft_s2s):.1f} "
              f"(first task is the start-path cost; later ones show sticky vs normal queue)")
    if act_s2s:
        print(f"activity schedule->start ms:      min={min(act_s2s):.1f} max={max(act_s2s):.1f} "
              f"(includes retry backoff if the activity was retried!)")
    top = ", ".join(f"{k}={v}" for k, v in counts.most_common(8))
    print(f"event mix: {top}")
    return 0


async def cmd_count(args) -> int:
    client = await common.connect(id_prefix="probe")
    q = args.query or (f'LabRunId = "{args.run_id}"' if args.run_id else "")
    if not q:
        print("give --run-id or --query")
        return 2
    try:
        res = await client.count_workflows(q + " GROUP BY ExecutionStatus")
        for g in getattr(res, "groups", []):
            print(f"  {' '.join(map(str, g.values)):<14} {g.count}")
        print(f"total {res.count}")
    except Exception:
        res = await client.count_workflows(q)
        print(f"total {res.count}  (GROUP BY not supported by this visibility store)")
    return 0


def _fingerprint32():
    try:
        import farmhash  # pyfarmhash
    except ImportError:
        sys.exit("pip install pyfarmhash  (only needed for shard mapping)")
    return farmhash.fingerprint32


def shard_of(fp32, namespace_id: str, workflow_id: str, num_shards: int) -> int:
    # Mirrors the server: farm.Fingerprint32(namespaceID + "_" + workflowID) % numShards + 1
    return fp32(f"{namespace_id}_{workflow_id}") % num_shards + 1


async def cmd_shard(args) -> int:
    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest

    client = await common.connect(id_prefix="probe")
    resp = await client.workflow_service.describe_namespace(DescribeNamespaceRequest(namespace=common.NAMESPACE))
    ns_id = resp.namespace_info.id
    shard = shard_of(_fingerprint32(), ns_id, args.workflow_id, args.shards)
    print(f"namespace_id={ns_id}\nworkflow_id={args.workflow_id}\nnum_shards={args.shards}\n=> history shard {shard}")
    print("verify: make cli-tdbg ARGS=\"workflow describe -w " + args.workflow_id + "\"")
    return 0


def cmd_shard_dist(args) -> int:
    fp32 = _fingerprint32()
    ns_id = args.namespace_id or str(uuid.uuid4())
    counts = Counter(shard_of(fp32, ns_id, args.pattern.format(i=i), args.shards) for i in range(args.count))
    per = [counts.get(s, 0) for s in range(1, args.shards + 1)]
    mean = statistics.mean(per)
    print(f"pattern={args.pattern!r} ids={args.count} shards={args.shards}  (namespace_id={ns_id})")
    print(f"per-shard workflows: mean={mean:.1f} stdev={statistics.pstdev(per):.1f} min={min(per)} max={max(per)} "
          f"empty_shards={sum(1 for x in per if x == 0)}")
    print(f"hottest shards: {[(s, c) for s, c in counts.most_common(5)]}")
    print(f"max/mean = {max(per) / mean:.2f}   (1.0 = perfectly even; a single hot ID is always 1 shard regardless of N)")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(prog="lab.probe", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    h = sub.add_parser("history")
    h.add_argument("workflow_id")
    h.add_argument("--run-id", default=None)
    h.add_argument("--legend", action=argparse.BooleanOptionalAction, default=True, help="print who-produced-it notes")
    c = sub.add_parser("count")
    c.add_argument("--run-id")
    c.add_argument("--query")
    s = sub.add_parser("shard")
    s.add_argument("workflow_id")
    s.add_argument("--shards", type=int, required=True)
    d = sub.add_parser("shard-dist")
    d.add_argument("--pattern", default="wf-{i}")
    d.add_argument("--count", type=int, default=100_000)
    d.add_argument("--shards", type=int, required=True)
    d.add_argument("--namespace-id", default=None)
    a = ap.parse_args()
    if a.cmd == "shard-dist":
        sys.exit(cmd_shard_dist(a))
    fn = {"history": cmd_history, "count": cmd_count, "shard": cmd_shard}[a.cmd]
    sys.exit(asyncio.run(fn(a)))


if __name__ == "__main__":
    main()
