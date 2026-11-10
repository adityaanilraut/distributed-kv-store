CLUSTER ?= portfolio-kv
KUBE = kubectl --context kind-$(CLUSTER) -n raft-demo
BENCH_ARGS ?=

.PHONY: demo test bench dashboards
demo:
	kind get clusters | grep -qx '$(CLUSTER)' || kind create cluster --name '$(CLUSTER)'
	docker build -t portfolio-raft:local .
	kind load docker-image --name '$(CLUSTER)' portfolio-raft:local
	kubectl --context kind-$(CLUSTER) create namespace raft-demo --dry-run=client -o yaml | kubectl --context kind-$(CLUSTER) apply -f -
	$(KUBE) apply -f deploy/raft.yaml
	$(KUBE) rollout restart statefulset/raft
	$(KUBE) rollout status statefulset/raft --timeout=180s

test:
	go test -race -v ./...

dashboards:
	$(KUBE) apply -k deploy/monitoring
	$(KUBE) rollout status deployment/grafana --timeout=120s

bench:
	@mkdir -p results
	@./deploy/bench.sh '$(CLUSTER)' $(BENCH_ARGS)
