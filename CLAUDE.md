```markdown
# Temporal Self-Hosted Cluster Runtime Investigation

## Project Overview

- **Purpose:** A reusable investigation process and runbook for diagnosing runtime issues (latency, backlog, errors, stuck workflows) in a **self-hosted Temporal cluster**.
- **Core principle:** Work from the outside in. Confirm the symptom, localize it to a layer (client, worker, frontend, matching, history, persistence, visibility), then find the root cause.
- **Architecture in scope:** Four server services (frontend, history, matching, internal worker), a persistence store, a visibility store, and SDK workers.
- **Not yet specified by the user:**
  - Persistence backend (Postgres, MySQL, or Cassandra)
  - Visibility store (Elasticsearch/OpenSearch or SQL)
  - Deployment method (Helm on Kubernetes, VMs, etc.)
  - The specific symptom being investigated
- **Scope decision:** The user confirmed self-hosted. Temporal Cloud is out of scope, since server services can't be inspected there.

## Current State & Completed Work

Two deliverables have been produced:

1. **General 8-phase investigation process:**
   - Define the symptom and scope
   - Quick health check
   - Localize the layer with metrics
   - Check logs
   - Drill into executions
   - Check configuration and limits
   - Map common causes to fixes
   - Verify, mitigate, document
2. **Self-hosted runbook (11 sections):**
   - Prerequisites (metrics, logs, CLI access, config in version control)
   - Triage (first 5 minutes)
   - Persistence analysis
   - Frontend analysis
   - Matching and worker analysis
   - History, shards, and queues
   - Visibility store
   - Log patterns
   - Execution analysis
   - Configuration review
   - Cause-to-fix table and closing the loop

**Key decisions and conclusions reached:**

- Check **persistence first**. It is the most common root cause of cluster-wide degradation, and frontend, history, and matching symptoms are often downstream effects.
- High schedule-to-start latency with a healthy server usually means a **worker-side bottleneck** (slots, pollers, CPU, sticky cache).
- **Do not raise rate limits** unless the layer below has headroom. For `SystemOverloaded`, fix the underlying bottleneck instead.
- Shard count is fixed at cluster creation, so hot shards are mitigated at the application level (spread load, Continue-As-New).
- **Mitigate before root-causing** when customers are impacted.

## Next Steps & Remaining Tasks

- [ ] Gather environment details: persistence backend, visibility store, deployment method, server version, shard count.
- [ ] Identify the concrete symptom, its onset time, and its blast radius (workflow, task queue, namespace, or cluster-wide).
- [ ] Verify prerequisites exist: Prometheus scraping all four services, SDK metrics from workers, searchable structured logs, `temporal` and `tdbg` CLI access, dynamic config in version control.
- [ ] Confirm exact metric and config key names against the running server's `/metrics` output and version docs.
- [ ] Build a targeted runbook for the user's stack:
  - Exact PromQL queries and alert thresholds
  - Dashboard panels
  - Backend-specific DB checks
- [ ] Add alerts for: persistence p99 latency, schedule-to-start latency, task queue backlog age, resource-exhausted rate, history queue lag, pod restarts.
- [ ] Set up replayer tests against production histories before workflow code deploys.
- [ ] Write a postmortem template (timeline, root cause, metric and log evidence).

## Technical Context & Constraints

**Investigation order:**
1. Scope the symptom and onset.
2. Health check and pod status (restarts, OOMKills, CPU throttling).
3. Persistence latency and errors.
4. Frontend errors by `resource_exhausted_cause`.
5. Worker-side schedule-to-start and slot metrics.
6. History queue lag and hot shards.
7. Specific workflow inspection.
8. Config and recent-change review.

**CLI commands:**
```bash
temporal operator cluster health
temporal operator cluster describe
temporal task-queue describe --task-queue <tq> -n <ns> [--task-queue-type activity]
temporal workflow describe -w <id> -n <ns>
temporal workflow show -w <id> -n <ns> --output json
temporal workflow list -n <ns> --query '<visibility query>'
tdbg shard describe --shard-id <n>
tdbg workflow describe -w <id> -r <run> -n <ns>
tdbg dlq list
```
Some `tdbg` flags vary by server version.

**Key metrics:**
- Server: `persistence_latency`, `persistence_errors`, `persistence_requests`, `service_requests`, `service_errors`, `service_errors_resource_exhausted` (label `resource_exhausted_cause`), `service_latency`
- SDK: `temporal_workflow_task_schedule_to_start_latency`, `temporal_activity_schedule_to_start_latency`, `temporal_worker_task_slots_available`, `temporal_num_pollers`

**Interpreting `resource_exhausted_cause`:**
- RPS or namespace limit: raise limits only if downstream capacity exists.
- Concurrent limit: too many pollers or long-running requests.
- SystemOverloaded: backpressure from persistence or CPU, so fix the bottleneck.

**Worker decision logic:**
- High schedule-to-start with slots exhausted: scale out or raise slots.
- High schedule-to-start with slots free and low pollers: tune pollers or enable poller autoscaling.
- Zero pollers: worker fleet is down or misconfigured.
- Growing backlog with healthy workers: increase matching partitions.
- High sticky latency: cache too small or workers restarting.

**Dynamic config keys to audit** (verify per version):
- `frontend.namespaceRPS`, `frontend.globalNamespaceRPS`
- `frontend.persistenceMaxQPS`, `history.persistenceMaxQPS`, `matching.persistenceMaxQPS`
- `matching.numTaskqueueReadPartitions`, `matching.numTaskqueueWritePartitions`
- `history.cacheMaxSize`
- `limit.historySize.*`, `limit.historyCount.*`

**History limits:**
- Defaults warn at ~10K events / 10 MB and terminate at ~50K events / 50 MB.
- Mitigation: Continue-As-New.

**Backend-specific checks:**
- Postgres/MySQL: CPU, IOPS, connection count, lock waits, slow queries, replication lag, vacuum and bloat (Postgres).
- Cassandra: compactions, tombstones, GC pauses, dropped mutations, large partitions.
- Connection math: `maxConns × replicas × services` must stay under the DB connection limit, including during rolling deploys.
- Elasticsearch/OpenSearch: cluster health, indexing latency, write thread pool rejections, disk watermarks.

**Log patterns to search:**
- `Persistent store operation failure`
- `ShardOwnershipLost`
- `context deadline exceeded`
- Shard acquisition errors
- History size and count warnings
- Ringpop suspect/faulty messages
- Rate-limit messages

Pivot on `namespace`, `wf-id`, `wf-run-id`, and `shard-id`.

**Operational rules:**
- Mitigate before root-causing if customers are impacted.
- Confirm recovery with the same metrics used to detect the problem.
- Frontend load imbalance can come from L4 balancers pinning long-lived gRPC connections. Use L7/gRPC-aware or client-side balancing.
- Visibility problems should not block workflow execution. If workflows are also slow, look elsewhere.
```