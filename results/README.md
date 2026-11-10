# Results

Five Raft replicas ran in kind on an Apple Silicon Mac with 8 GB RAM. Each replica used its own persistent volume. The client ran inside the cluster with 96 workers, 128-byte values, and three 30-second windows.

| Run | Acknowledged writes | Failed write attempts | Acknowledged writes/sec |
| --- | ---: | ---: | ---: |
| 1 | 65,746 | 20,882 | 2,192 |
| 2 | 83,122 | 30,998 | 2,771 |
| 3 | 76,645 | 13,834 | 2,555 |

Median throughput was **2,555 acknowledged writes/sec**, below the **15,000 writes/sec** target. The load generator counts failed gRPC attempts separately and does not retry an ambiguous write as part of the throughput count.

Across 20 forced leader crashes, **p95 recovery was 477 ms**, within the 500 ms p95 target. The slowest recovery was **1,015 ms**. Recovery is measured from the crash request to the first acknowledged write from a surviving replica. The harness waits for all five replicas between trials.

- [Raw Kubernetes benchmark output](kubernetes.json)
- `make demo NAMESPACE=raft-benchmark && make bench NAMESPACE=raft-benchmark` runs the benchmark in a separate namespace.
- `make test` runs the race detector and the Porcupine history check across leader loss, a minority partition, snapshot catch-up, and a full-cluster restart.
