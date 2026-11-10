// Bench runs from a pod so client traffic and faults use the cluster network.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adityaanilraut/distributed-kv-store/api"
	"github.com/adityaanilraut/distributed-kv-store/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const payloadBytes = 128

type target struct {
	host   string
	client api.KeyValueClient
	conn   *grpc.ClientConn
}

var httpClient = &http.Client{Timeout: 350 * time.Millisecond}

func getStatus(t target) raft.Status {
	var state raft.Status
	resp, err := httpClient.Get("http://" + t.host + ":8080/status")
	if err == nil {
		defer resp.Body.Close()
		_ = json.NewDecoder(resp.Body).Decode(&state)
	}
	return state
}

func ready(targets []target) (int, bool) {
	type reply struct {
		index int
		state raft.Status
	}
	replies := make(chan reply, len(targets))
	for i, target := range targets {
		go func() { replies <- reply{i, getStatus(target)} }()
	}
	best, healthy, term := -1, 0, uint64(0)
	states := make([]raft.Status, len(targets))
	for range targets {
		res := <-replies
		states[res.index] = res.state
		if res.state.Healthy {
			healthy++
			if res.state.ID != 0 && res.state.ID == res.state.Leader && res.state.Term > term {
				best, term = res.index, res.state.Term
			}
		}
	}
	if best >= 0 {
		agree := 0
		for _, state := range states {
			if state.Healthy && state.Term == term && state.Leader == states[best].ID {
				agree++
			}
		}
		if agree < 3 {
			best = -1
		}
	}
	return best, healthy == len(targets)
}

func waitHealthy(targets []target, deadline time.Time) (int, error) {
	for time.Now().Before(deadline) {
		index, allHealthy := ready(targets)
		if index >= 0 && allHealthy {
			return index, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return -1, fmt.Errorf("cluster did not recover all five nodes")
}

type writeResult struct {
	Acknowledged uint64  `json:"acknowledged"`
	Errors       uint64  `json:"errors"`
	WritesPerSec float64 `json:"writes_per_sec"`
}

func benchmark(targets []target, initialLeader, workers int, duration time.Duration) writeResult {
	payload := make([]byte, payloadBytes)
	var success, failures atomic.Uint64
	var leader atomic.Int32
	leader.Store(int32(initialLeader))
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	started := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if index, _ := ready(targets); index >= 0 {
					leader.Store(int32(index))
				}
			}
		}
	}()
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; ctx.Err() == nil; seq++ {
				req := &api.PutRequest{Key: fmt.Sprintf("%d/%d", worker, seq%32), Value: payload}
				call, stop := context.WithTimeout(ctx, time.Second)
				_, err := targets[leader.Load()].client.Put(call, req)
				stop()
				if err == nil {
					success.Add(1)
				} else if time.Since(started) < duration {
					failures.Add(1)
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
	count := success.Load()
	return writeResult{count, failures.Load(), float64(count) / duration.Seconds()}
}

func failover(targets []target, old int) (float64, int, error) {
	started := time.Now()
	resp, err := httpClient.Post("http://"+targets[old].host+":8080/fault/crash", "text/plain", nil)
	if err != nil {
		return 0, 0, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return 0, 0, fmt.Errorf("fault endpoint returned %s", resp.Status)
	}
	failed := 0
	for time.Since(started) < 15*time.Second {
		replies := make(chan error, len(targets)-1)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
		for i, target := range targets {
			if i == old {
				continue
			}
			go func() {
				_, err := target.client.Put(ctx, &api.PutRequest{Key: "failover", Value: []byte("ok")})
				replies <- err
			}()
		}
		for range len(targets) - 1 {
			if <-replies == nil {
				cancel()
				return float64(time.Since(started).Microseconds()) / 1000, failed, nil
			}
			failed++
		}
		cancel()
		time.Sleep(10 * time.Millisecond)
	}
	return 0, failed, fmt.Errorf("no client write succeeded after 15 seconds")
}

func connect(targets []target) error {
	for i := range targets {
		t := &targets[i]
		if t.conn != nil {
			_ = t.conn.Close()
		}
		conn, err := grpc.NewClient("passthrough:///"+t.host+":50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		t.conn, t.client = conn, api.NewKeyValueClient(conn)
	}
	return nil
}

// Replace the crashed pod after recording recovery to avoid accumulating
// Kubernetes restart backoff across trials. The StatefulSet retains its PVC.
func recycle(host string) error {
	const account = "/var/run/secrets/kubernetes.io/serviceaccount/"
	ca, err := os.ReadFile(account + "ca.crt")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	token, err := os.ReadFile(account + "token")
	if err != nil {
		return err
	}
	namespace, err := os.ReadFile(account + "namespace")
	if err != nil {
		return err
	}
	certs := x509.NewCertPool()
	if !certs.AppendCertsFromPEM(ca) {
		return fmt.Errorf("invalid Kubernetes CA")
	}
	pod, _, _ := strings.Cut(host, ".")
	url := "https://kubernetes.default.svc/api/v1/namespaces/" + string(namespace) + "/pods/" + pod
	req, err := http.NewRequest(http.MethodDelete, url, bytes.NewBufferString("{}"))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+string(token))
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: certs, MinVersion: tls.VersionTLS12},
	}}
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("pod replacement returned %s", res.Status)
	}
	return nil
}

func main() {
	hosts := flag.String("hosts", "raft-0.raft,raft-1.raft,raft-2.raft,raft-3.raft,raft-4.raft", "comma-separated peer hostnames")
	runs := flag.Int("runs", 3, "number of throughput runs")
	duration := flag.Duration("duration", 30*time.Second, "duration of each throughput run")
	trials := flag.Int("trials", 20, "leader crash trials")
	workers := flag.Int("workers", 96, "concurrent write clients")
	flag.Parse()

	var targets []target
	for _, host := range strings.Split(*hosts, ",") {
		targets = append(targets, target{host: host})
	}
	if len(targets) != 5 || *duration <= 0 || *runs < 1 || *trials < 1 || *workers < 1 {
		log.Fatal("benchmark requires five peers and positive duration, runs, trials and workers")
	}
	if err := connect(targets); err != nil {
		log.Fatal(err)
	}
	var writes []writeResult
	var recoveries []float64
	probeErrors := 0
	for run := range *runs {
		leader, err := waitHealthy(targets, time.Now().Add(time.Minute))
		if err != nil {
			log.Fatal(err)
		}
		writes = append(writes, benchmark(targets, leader, *workers, *duration))
		log.Printf("throughput run %d: %.0f writes/sec", run+1, writes[run].WritesPerSec)
	}
	for trial := range *trials {
		leader, err := waitHealthy(targets, time.Now().Add(4*time.Minute))
		if err != nil {
			log.Fatal(err)
		}
		if err := connect(targets); err != nil {
			log.Fatal(err)
		}
		ms, failures, err := failover(targets, leader)
		if err != nil {
			log.Fatal(err)
		}
		probeErrors += failures
		recoveries = append(recoveries, ms)
		log.Printf("crash trial %d: %.1f ms", trial+1, ms)
		if err := recycle(targets[leader].host); err != nil {
			log.Fatal(err)
		}
	}
	sorted := slices.Clone(recoveries)
	slices.Sort(sorted)
	result := map[string]any{
		"tested_commit": os.Getenv("TESTED_COMMIT"), "go": runtime.Version(),
		"environment": runtime.GOOS + "/" + runtime.GOARCH, "cpus_visible": runtime.NumCPU(),
		"writes": writes, "duration_seconds": duration.Seconds(), "workers": *workers,
		"payload_bytes": payloadBytes, "recovery_ms": recoveries, "failed_recovery_probes": probeErrors,
		"recovery_p95_ms": sorted[int(math.Ceil(.95*float64(len(sorted))))-1],
		"timestamp_utc":   time.Now().UTC(),
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		log.Fatal(err)
	}
}
