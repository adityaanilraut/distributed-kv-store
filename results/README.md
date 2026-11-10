# Results

Run `make test` for a race-enabled, five-node Porcupine history with a leader crash, minority partition, offline snapshot catch-up, and full-cluster restart. Run `make demo && make bench` for the Kubernetes workload. Results include only writes acknowledged within the timed load window. Recovery measures elapsed time from the crash request through the first acknowledged write to a surviving replica. The crash harness waits for the fifth replica between trials.
