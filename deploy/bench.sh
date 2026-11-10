#!/bin/sh
set -eu
cluster=$1
shift
kubectl --context "kind-$cluster" -n raft-demo delete pod raft-bench --ignore-not-found
kubectl --context "kind-$cluster" -n raft-demo run raft-bench \
  --image=portfolio-raft:local --image-pull-policy=Never --restart=Never \
  --overrides='{"spec":{"serviceAccountName":"raft-bench"}}' \
  --env="TESTED_COMMIT=$(git rev-parse HEAD)" --command -- /usr/local/bin/bench "$@"
kubectl --context "kind-$cluster" -n raft-demo wait --for=jsonpath='{.status.phase}'=Running pod/raft-bench --timeout=60s
log=$(mktemp)
trap 'rm -f "$log"' EXIT
kubectl --context "kind-$cluster" -n raft-demo logs -f pod/raft-bench | tee "$log"
tail -n 1 "$log" | python3 -c 'import json,sys; v=json.load(sys.stdin); assert v["writes"] and v["recovery_ms"]; print(json.dumps(v, indent=2))' > results/kubernetes.json
