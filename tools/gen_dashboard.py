#!/usr/bin/env python3
"""Generate config/grafana/dashboards/temporal-lab.json.

Panels are defined as data so the whole dashboard is reviewable in one screen and easy to
extend when you add an experiment:  python3 tools/gen_dashboard.py

Conventions
  * SDK (worker/client) latency histograms are in MILLISECONDS (Python Core SDK default).
  * Server latency histograms (service_latency, persistence_latency, ...) are in SECONDS.
  * Metric names come from the Temporal docs / SDK metrics reference. Server metric names
    change between releases: if a panel is empty, run `make metric-names` and adjust here.
"""
import json
import os

DS = {"type": "prometheus", "uid": "prom"}
R = "$__rate_interval"

_panels = []
_id = [0]
_pos = {"x": 0, "y": 0, "row_h": 0}


def row(title: str) -> None:
    _id[0] += 1
    _pos["y"] += _pos["row_h"]
    _pos["x"], _pos["row_h"] = 0, 0
    _panels.append({"id": _id[0], "type": "row", "title": title, "collapsed": False,
                    "gridPos": {"h": 1, "w": 24, "x": 0, "y": _pos["y"]}, "panels": []})
    _pos["y"] += 1


def ts(title, targets, unit="short", desc="", w=8, h=8, stack=False, minv=None):
    """targets: list of (expr, legend)."""
    if _pos["x"] + w > 24:
        _pos["y"] += _pos["row_h"]
        _pos["x"], _pos["row_h"] = 0, 0
    _id[0] += 1
    custom = {"lineWidth": 1, "fillOpacity": 10 if stack else 4, "showPoints": "never", "spanNulls": True}
    if stack:
        custom["stacking"] = {"mode": "normal", "group": "A"}
    defaults = {"unit": unit, "custom": custom}
    if minv is not None:
        defaults["min"] = minv
    _panels.append({
        "id": _id[0], "type": "timeseries", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": _pos["x"], "y": _pos["y"]},
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "targets": [{"refId": chr(65 + i), "datasource": DS, "expr": e, "legendFormat": lg, "editorMode": "code"}
                    for i, (e, lg) in enumerate(targets)],
    })
    _pos["x"] += w
    _pos["row_h"] = max(_pos["row_h"], h)


def q(p, metric, by="le", sel="", window=R):
    return f"histogram_quantile({p}, sum by ({by}) (rate({metric}_bucket{sel}[{window}])))"


# =============================================================================
row("1. Load generator (client view)")
ts("Client requests/s by operation", [(f'sum by (operation) (rate(temporal_request{{job="loadgen"}}[{R}]))', "{{operation}}")],
   "reqps", "What the load generator actually sent. Compare with the target rate in the loadgen output.")
ts("Client request FAILURES/s by status", [(f'sum by (operation, status_code) (rate(temporal_request_failure{{job="loadgen"}}[{R}]))',
                                              "{{operation}} {{status_code}}")], "reqps",
   "RESOURCE_EXHAUSTED here = the server is rate limiting. The SDK retries these transparently, so they show up as latency first.")
ts("Client StartWorkflow latency p50 / p99 (ms)", [
    (q(0.5, "temporal_request_latency", "le", '{job="loadgen",operation=~"StartWorkflowExecution|SignalWithStartWorkflowExecution"}'), "p50"),
    (q(0.99, "temporal_request_latency", "le", '{job="loadgen",operation=~"StartWorkflowExecution|SignalWithStartWorkflowExecution"}'), "p99"),
], "ms", "Includes SDK-internal retries. The loadgen table reports the same thing per step.")

row("2. SDK workers: the queueing signal (schedule-to-start) and worker health")
ts("Workflow task schedule→start p50 / p99 (ms)", [
    (q(0.5, "temporal_workflow_task_schedule_to_start_latency", "le, task_queue"), "p50 {{task_queue}}"),
    (q(0.99, "temporal_workflow_task_schedule_to_start_latency", "le, task_queue"), "p99 {{task_queue}}"),
], "ms", "Should be ~0. Rising = not enough workflow-task capacity (slots, pollers, CPU, cache misses).")
ts("Activity schedule→start p50 / p99 (ms)", [
    (q(0.5, "temporal_activity_schedule_to_start_latency", "le, task_queue"), "p50 {{task_queue}}"),
    (q(0.99, "temporal_activity_schedule_to_start_latency", "le, task_queue"), "p99 {{task_queue}}"),
], "ms", "The #1 autoscaling signal. Rising while server latency is flat = add/unblock ACTIVITY workers.")
ts("Task slots available (sum / min over workers)", [
    ("sum by (worker_type) (temporal_worker_task_slots_available)", "sum {{worker_type}}"),
    ("min by (worker_type) (temporal_worker_task_slots_available)", "min {{worker_type}}"),
], "short", "Hits 0 = saturated (min) . Not reported with the resource-based tuner: use slots_used.", minv=0)
ts("Task slots used", [("sum by (worker_type) (temporal_worker_task_slots_used)", "{{worker_type}}")], "short")
ts("Sticky cache hit / miss per second + size", [
    (f"sum(rate(temporal_sticky_cache_hit[{R}]))", "hit/s"),
    (f"sum(rate(temporal_sticky_cache_miss[{R}]))", "miss/s"),
    ("sum(temporal_sticky_cache_size)", "cached workflows"),
], "short", "Misses force a full history replay. Shrink MAX_CACHED_WORKFLOWS and watch this and the replay panel.")
ts("Workflow task execution vs REPLAY latency p99 (ms)", [
    (q(0.99, "temporal_workflow_task_execution_latency"), "execution p99"),
    (q(0.99, "temporal_workflow_task_replay_latency"), "replay p99"),
], "ms", "Replay latency climbing = cache misses on long histories.")
ts("Activity execution latency p99 (ms)", [(q(0.99, "temporal_activity_execution_latency", "le, activity_type"), "{{activity_type}}")], "ms")
ts("Task failures/s", [
    (f"sum by (failure_reason) (rate(temporal_workflow_task_execution_failed[{R}]))", "wft failed {{failure_reason}}"),
    (f"sum(rate(temporal_activity_execution_failed[{R}]))", "activity attempts failed"),
], "reqps", "wft failures = workflow-code bugs / non-determinism / deadlock detector. Activity failures are normal retries.")
ts("Pollers and empty polls", [
    ("sum by (poller_type) (temporal_num_pollers)", "pollers {{poller_type}}"),
    (f"sum(rate(temporal_workflow_task_queue_poll_empty[{R}]))", "wf empty polls/s"),
    (f"sum(rate(temporal_workflow_task_queue_poll_succeed[{R}]))", "wf successful polls/s"),
    (f"sum(rate(temporal_activity_poll_no_task[{R}]))", "activity empty polls/s"),
], "short", "Lots of empty polls = idle/over-provisioned pollers. Zero pollers = nothing will ever be dequeued.")
ts("Workflows completed / failed per second (worker view)", [
    (f"sum by (workflow_type) (rate(temporal_workflow_completed[{R}]))", "completed {{workflow_type}}"),
    (f"sum by (workflow_type) (rate(temporal_workflow_failed[{R}]))", "failed {{workflow_type}}"),
], "reqps")
ts("Worker process CPU (cores)", [("worker_process_cpu_cores", "{{job}} {{instance}}")], "short",
   "Exported by lab/worker.py itself (cAdvisor cannot see containers on Docker Desktop). A Python worker is "
   "GIL-bound near 1.0 for Python work: step 1 of the schedule-to-start diagnosis.", minv=0)
ts("Worker process memory (RSS)", [("worker_process_rss_bytes", "{{job}} {{instance}}")], "bytes",
   "Grows with the sticky cache (MAX_CACHED_WORKFLOWS) and with in-flight activity payloads.", minv=0)
ts("Worker event-loop busy (utilization)", [("worker_event_loop_busy_ratio", "{{job}} {{instance}}")], "percentunit",
   "Fraction of time the asyncio loop was not idle (lab/worker.py). Near 100% while worker CPU is low = the loop is "
   "blocked on synchronous I/O inside async code.", minv=0)
ts("Worker event-loop lag (ms)", [("worker_event_loop_lag_ms", "{{job}} {{instance}}")], "ms",
   "Worst asyncio lag per 2 s (lab/worker.py probe). Healthy is ~0-15 ms. Hundreds of ms = something blocks the "
   "event loop: a blocking call or CPU work inside an `async def` activity.", minv=0)

row("3. Temporal server")
ts("Cluster membership: live replicas per service", [('count by (role) (up{job="temporal"} == 1)', "{{role}}")], "short",
   "Watch this drop and recover during kill/restart experiments.", minv=0)
ts("Frontend requests/s by operation", [(f'sum by (operation) (rate(service_requests{{service_name="frontend"}}[{R}]))', "{{operation}}")], "reqps", stack=True)
ts("Frontend p99 latency by operation (s)", [(q(0.99, "service_latency", "le, operation", '{service_name="frontend"}'), "{{operation}}")], "s")
ts("History p99 latency by operation (s)", [(q(0.99, "service_latency", "le, operation", '{service_name="history"}'), "{{operation}}")], "s",
   "History owns the shards and does the DB writes. This is where a slow DB shows up first.")
ts("Matching p99 latency by operation (s)", [(q(0.99, "service_latency", "le, operation", '{service_name="matching"}'), "{{operation}}")], "s")
ts("Server errors/s by service and type", [(f"sum by (service_name, error_type) (rate(service_error_with_type[{R}]))", "{{service_name}} {{error_type}}")],
   "reqps", "ResourceExhausted, ShardOwnershipLost, ... are the interesting ones.")
ts("Persistence p99 latency by operation (s)", [(q(0.99, "persistence_latency", "le, operation"), "{{operation}}")], "s",
   "Every workflow state transition costs at least one of these. First derivative of 'the DB is the bottleneck'.")
ts("Persistence requests/s by service", [(f"sum by (service_name) (rate(persistence_requests[{R}]))", "{{service_name}}")], "reqps", stack=True)
ts("Persistence errors/s", [(f"sum by (service_name, error_type) (rate(persistence_error_with_type[{R}]))", "{{service_name}} {{error_type}}")], "reqps")
ts("History task queue latency p99 (s)  [task_latency_queue]", [(q(0.99, "task_latency_queue", "le, operation"), "{{operation}}")], "s",
   "Time internal history tasks (transfer/timer) wait before processing. Timer storms show up here.")
ts("Lock wait p99 (s)  [lock_latency, if present]", [(q(0.99, "lock_latency", "le, operation"), "{{operation}}")], "s",
   "Per-workflow / per-shard lock contention (signal firehose, hot key).")
ts("Matching poll_success/s (sync vs backlog)", [
    (f"sum(rate(poll_success[{R}]))", "poll_success"),
    (f"sum(rate(poll_success_sync[{R}]))", "poll_success_sync (matched without touching DB)"),
], "reqps", "sync match rate falling = tasks going through the DB-backed backlog path.")
ts("Inter-service client errors/s", [(f"sum by (service_name, service_role) (rate(client_errors[{R}]))", "{{service_name}}→{{service_role}}")], "reqps",
   "Indicates connectivity problems between server roles (kill/pause experiments).")
ts("Go goroutines by service", [('sum by (role) (go_goroutines{job="temporal"})', "{{role}}")], "short")

row("4. Infrastructure")
ts("Container CPU cores by compose service", [(
    f'sum by (container_label_com_docker_compose_service) (rate(container_cpu_usage_seconds_total{{container_label_com_docker_compose_service!=""}}[{R}]))',
    "{{container_label_com_docker_compose_service}}")], "short", "Which container saturates first?", stack=True)
ts("Container memory (working set)", [(
    'sum by (container_label_com_docker_compose_service) (container_memory_working_set_bytes{container_label_com_docker_compose_service!=""})',
    "{{container_label_com_docker_compose_service}}")], "bytes")
ts("Postgres connections by database", [('sum by (datname) (pg_stat_database_numbackends{datname=~"temporal.*"})', "{{datname}}")], "short",
   "Each Temporal service holds a pool (SQL_MAX_CONNS). Scaling history hosts multiplies this.")
ts("Postgres transactions/s (commit + rollback)", [
    (f'sum by (datname) (rate(pg_stat_database_xact_commit{{datname=~"temporal.*"}}[{R}]))', "commit {{datname}}"),
    (f'sum by (datname) (rate(pg_stat_database_xact_rollback{{datname=~"temporal.*"}}[{R}]))', "rollback {{datname}}"),
], "ops")
ts("Postgres rows inserted / updated per second", [
    (f'sum(rate(pg_stat_database_tup_inserted{{datname=~"temporal.*"}}[{R}]))', "inserted/s"),
    (f'sum(rate(pg_stat_database_tup_updated{{datname=~"temporal.*"}}[{R}]))', "updated/s"),
], "ops", "Write amplification: compare with client starts/s.")
ts("Prometheus scrape targets up", [('sum by (job) (up)', "{{job}}")], "short", "If a panel is empty, check that its job is up.", minv=0)

# Re-flow: rows must be first-class in gridPos order; already sequential by construction.
dashboard = {
    "id": None, "uid": "temporal-lab", "title": "Temporal Lab", "tags": ["temporal", "lab"],
    "timezone": "browser", "schemaVersion": 39, "version": 1, "editable": True,
    "refresh": "5s", "time": {"from": "now-15m", "to": "now"},
    "graphTooltip": 1, "templating": {"list": []},
    # chaos/chaos.sh posts Grafana annotations tagged "chaos": they show up as red markers on every panel.
    "annotations": {"list": [{
        "name": "chaos events", "enable": True, "iconColor": "red",
        "datasource": {"type": "grafana", "uid": "-- Grafana --"},
        "target": {"type": "tags", "tags": ["chaos"], "limit": 200, "matchAny": False},
    }]},
    "panels": _panels,
}

out = os.path.join(os.path.dirname(__file__), "..", "config", "grafana", "dashboards", "temporal-lab.json")
with open(out, "w") as f:
    json.dump(dashboard, f, indent=1)
print(f"wrote {os.path.normpath(out)}: {sum(1 for p in _panels if p['type'] == 'timeseries')} panels in "
      f"{sum(1 for p in _panels if p['type'] == 'row')} rows")
