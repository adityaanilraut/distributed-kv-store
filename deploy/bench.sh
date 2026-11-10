#!/bin/sh
set -eu
cluster=$1
namespace=$2
shift 2
kube() { kubectl --context "kind-$cluster" -n "$namespace" "$@"; }
kube delete pod raft-bench --ignore-not-found
kube run raft-bench \
  --image=portfolio-raft:local --image-pull-policy=Never --restart=Never \
  --overrides='{"spec":{"serviceAccountName":"raft-bench"}}' \
  --env="TESTED_COMMIT=$(git rev-parse HEAD)" --command -- /usr/local/bin/bench "$@"
kube wait --for=jsonpath='{.status.phase}'=Running pod/raft-bench --timeout=60s
log=$(mktemp)
report=$(mktemp results/.kubernetes.XXXXXX)
trap 'unlink "$log"; if [ -f "$report" ]; then unlink "$report"; fi' EXIT
kube logs -f pod/raft-bench | tee "$log"
kube wait --for=jsonpath='{.status.phase}'=Succeeded pod/raft-bench --timeout=10s
tail -n 1 "$log" | python3 -c 'import json,sys; v=json.load(sys.stdin); assert v["writes"] and v["recovery_ms"]; print(json.dumps(v, indent=2))' > "$report"
mv "$report" results/kubernetes.json
