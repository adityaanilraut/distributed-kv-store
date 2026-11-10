# Distributed Key-Value Store

A Go key-value service that implements Raft election, log replication, and snapshot installation for five fixed voting replicas. A write is acknowledged after a majority has persisted its log entry and the leader has applied it. Reads go through the same ordered log.

## Quick start

Install Docker, kind, kubectl, Go 1.27, and optionally grpcurl.

```sh
make demo
kubectl --context kind-portfolio-kv -n raft-demo get pods,pvc
kubectl --context kind-portfolio-kv -n raft-demo port-forward pod/raft-0 50051:50051 8080:8080
```

In another terminal, run the commands below against the leader. If the forwarded replica is a follower, `/status` returns a one-based leader ID. Forward pod `raft-(ID-1)` to reach it.

```sh
curl -s localhost:8080/status
grpcurl -plaintext -import-path api -proto kv.proto \
  -d '{"key":"hello","value":"d29ybGQ="}' localhost:50051 kv.v1.KeyValue/Put
grpcurl -plaintext -import-path api -proto kv.proto \
  -d '{"key":"hello"}' localhost:50051 kv.v1.KeyValue/Get
```

Values use base64 in the gRPC JSON representation. Followers reject proposals with `FailedPrecondition` and a `leader-id` trailer when known. A deadline or connection failure leaves a write's outcome unknown, so clients should resolve its outcome before retrying.

## Architecture

```text
gRPC client → leader → local bbolt log
                    ├→ four peer HTTP links → bbolt on each follower
                    └→ quorum commit → apply to key-value map → response
```

Terms, votes, commit indexes, log entries, and snapshots survive restarts. A lagging replica can install a snapshot and then receive the remaining suffix. The leader batches client proposals and sends entries through independent peer workers. Membership is fixed. The local demo trusts its network and exposes a crash endpoint inside the cluster for benchmarks.

## Verification

- `make test` runs the race detector and a five-node fault test checked with Porcupine.
- `make bench` measures acknowledged 128-byte writes and client-visible recovery after leader crashes.
- `make dashboards` installs Prometheus and Grafana. Forward Grafana with `kubectl --context kind-portfolio-kv -n raft-demo port-forward deployment/grafana 3000:3000`, then open http://localhost:3000/d/raft.
- [Results and methodology](results/README.md)

Remove the local demo with `kind delete cluster --name portfolio-kv`.
