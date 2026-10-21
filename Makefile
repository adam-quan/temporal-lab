# Temporal internals lab. `make help` lists targets.
DC      ?= docker compose
ARGS    ?=
SVC     ?= history
N       ?= 2
TQ      ?= lab-tq
V       ?= v2
SHARDS  ?= 4
WF      ?=
RUN     ?=
PATTERN ?= .
# Host ports: shell env wins, then .env, then the default.
envport = $(or $($(1)),$(shell sed -n 's/^$(1)=//p' .env 2>/dev/null),$(2))
UI_PORT   := $(call envport,UI_HOST_PORT,8080)
GF_PORT   := $(call envport,GRAFANA_HOST_PORT,3000)
PROM_PORT := $(call envport,PROM_HOST_PORT,9090)
TOXI_PORT := $(call envport,TOXIPROXY_HOST_PORT,8474)

.DEFAULT_GOAL := help
.PHONY: help up down nuke reset ps logs scale workers split unsplit deploy-variant smoke load probe history \
	    count shard hotshard workerperf historytasks persistenceqps shardio cli cli-tdbg tq tq-watch metric-names dashboard cleanup urls chaos

help: ## this list
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

# ---------------------------------------------------------------- lifecycle
up: ## build and start everything (first run pulls images, ~2-3 min)
	$(DC) up -d --build
	@$(MAKE) --no-print-directory urls
	@echo "next: make smoke"

down: ## stop everything, keep data
	$(DC) --profile split --profile tools down

nuke: ## stop everything and DELETE all data (database, prometheus)
	$(DC) --profile split --profile tools down -v --remove-orphans

reset: ## wipe and rebuild with a new shard count:  make reset SHARDS=512
	$(DC) --profile split --profile tools down -v --remove-orphans
	NUM_HISTORY_SHARDS=$(SHARDS) $(DC) up -d --build
	@echo "cluster rebuilt with numHistoryShards=$(SHARDS) (fixed until the next reset)"

ps: ## container status
	$(DC) ps

logs: ## follow logs:  make logs SVC=history
	$(DC) logs -f --tail=100 $(SVC)

urls: ## where things are
	@echo "  Temporal UI   http://localhost:$(UI_PORT)   (namespace: lab)"
	@echo "  Grafana       http://localhost:$(GF_PORT)   (dashboard: Temporal Lab)"
	@echo "  Prometheus    http://localhost:$(PROM_PORT)"
	@echo "  Toxiproxy API http://localhost:$(TOXI_PORT)"
	@echo "  Frontend gRPC localhost:7233"

# ---------------------------------------------------------------- topology
scale: ## scale a service:  make scale SVC=history N=3   (or SVC=sdk-worker)
	$(DC) up -d --no-recreate --scale $(SVC)=$(N) $(SVC)

workers: ## recreate SDK workers so they pick up W_* changes from .env or your shell
	$(DC) up -d --force-recreate --no-deps sdk-worker

split: ## switch to separate workflow-worker and activity-worker fleets (queues lab-wf / lab-act)
	$(DC) stop sdk-worker
	$(DC) --profile split up -d --no-deps sdk-worker-wf sdk-worker-act
	@echo "use: make load ARGS=\"ramp --shape chain --task-queue lab-wf --activity-queue lab-act --rate 50 --duration 60\""

unsplit: ## back to a single combined worker fleet
	$(DC) --profile split stop sdk-worker-wf sdk-worker-act
	$(DC) up -d --no-deps sdk-worker

deploy-variant: ## "deploy" EvolvingWorkflow code:  make deploy-variant V=v2   (v1|v2|v3)
	EVOLVING_VARIANT=$(V) $(DC) up -d --force-recreate --no-deps sdk-worker

# ---------------------------------------------------------------- load and inspection
smoke: ## one of every workflow type; run this first:  make smoke   (split fleet: ARGS="--task-queue lab-wf --activity-queue lab-act")
	$(DC) run --rm --no-deps loadgen smoke $(ARGS)

load: ## run the generator:  make load ARGS="ramp --shape noop --rates 10,50,100 --step-seconds 60"
	$(DC) run --rm --no-deps --name loadgen loadgen $(ARGS)

probe: ## any probe subcommand:  make probe ARGS="count --run-id noop-123456-ab12"
	$(DC) run --rm --no-deps --entrypoint python loadgen -m lab.probe $(ARGS)

history: ## annotated event history:  make history WF=<workflow-id>
	$(DC) run --rm --no-deps --entrypoint python loadgen -m lab.probe history $(WF)

count: ## workflows of a run by status:  make count RUN=<run_id>
	$(DC) run --rm --no-deps --entrypoint python loadgen -m lab.probe count --run-id $(RUN)

shard: ## which history shard owns an ID:  make shard WF=<workflow-id> SHARDS=512
	$(DC) run --rm --no-deps --entrypoint python loadgen -m lab.probe shard $(WF) --shards $(SHARDS)

hotshard: ## hot-shard test:  make hotshard ARGS="run --kind restart"   (detector | shard <id>; see experiments/hot-shard.md)
	$(DC) run --rm --no-deps --entrypoint python loadgen -m lab.hotshard $(ARGS)

workerperf: ## worker-performance test:  make workerperf ARGS="run --scenario slots [--fix]"   (list; see experiments/worker-performance.md)
	$(DC) run --rm --no-deps --name loadgen --entrypoint python loadgen -m lab.workerperf $(ARGS)

historytasks: ## history task processing test:  make historytasks ARGS="run --scenario loop"   (calibrate | indicator | reset; see experiments/history-task-processing.md)
	$(DC) run --rm --no-deps --entrypoint python loadgen -m lab.historytasks $(ARGS)

persistenceqps: ## history persistence QPS test:  make persistenceqps ARGS="run --scenario throttle"   (list | reset; see experiments/history-persistence-qps.md)
	$(DC) run --rm --no-deps --entrypoint python loadgen -m lab.persistenceqps $(ARGS)

shardio: ## shard IO concurrency test (needs SHARDS=4): make shardio ARGS="run --scenario issue"  (experiments/shard-io-concurrency.md)
	@tools/shardio.sh $(ARGS)

cli: ## temporal CLI:  make cli ARGS="workflow list --limit 5"
	$(DC) exec admin-tools temporal $(ARGS)

cli-tdbg: ## tdbg (server debugger):  make cli-tdbg ARGS="workflow describe -w <id>"
	$(DC) exec admin-tools tdbg --address frontend:7233 --namespace lab $(ARGS)

tq: ## task queue stats (pollers, backlog):  make tq TQ=lab-tq
	$(DC) exec admin-tools temporal task-queue describe --task-queue $(TQ)

tq-watch: ## refresh task queue stats every 2s
	@while true; do clear; date; $(DC) exec -T admin-tools temporal task-queue describe --task-queue $(TQ); sleep 2; done

cleanup: ## terminate every workflow of a run:  make cleanup RUN=<run_id>
	$(DC) exec admin-tools temporal workflow terminate --query 'LabRunId="$(RUN)"' --reason "lab cleanup" --yes

# ---------------------------------------------------------------- observability
metric-names: ## list metric names Prometheus has seen:  make metric-names PATTERN=schedule_to_start
	@curl -fsS http://localhost:$(PROM_PORT)/api/v1/label/__name__/values | python3 -c "import sys,json,re; [print(n) for n in json.load(sys.stdin)['data'] if re.search('$(PATTERN)', n)]"

dashboard: ## regenerate the Grafana dashboard JSON from tools/gen_dashboard.py
	python3 tools/gen_dashboard.py

chaos: ## fault injection help
	@chaos/chaos.sh help
