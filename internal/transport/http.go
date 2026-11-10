package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/adityaanilraut/distributed-kv-store/internal/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HTTP struct {
	Peers  map[uint64]string
	Client *http.Client
}

func New(peers map[uint64]string) *HTTP {
	return &HTTP{
		Peers: peers,
		Client: &http.Client{Transport: &http.Transport{
			MaxIdleConns: 32, MaxIdleConnsPerHost: 4, IdleConnTimeout: time.Minute,
		}},
	}
}

func call[Request, Response any](ctx context.Context, t *HTTP, peer uint64, path string, data Request) (Response, error) {
	var result Response
	body, err := json.Marshal(data)
	if err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.Peers[peer]+path, bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := t.Client.Do(req)
	if err != nil {
		return result, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return result, fmt.Errorf("peer %d returned HTTP %d", peer, res.StatusCode)
	}
	reader := io.LimitReader(res.Body, 4096)
	err = json.NewDecoder(reader).Decode(&result)
	if err == nil {
		_, err = io.Copy(io.Discard, reader)
	}
	return result, err
}

func (t *HTTP) Vote(ctx context.Context, id uint64, req raft.VoteRequest) (raft.VoteResponse, error) {
	return call[raft.VoteRequest, raft.VoteResponse](ctx, t, id, "/raft/vote", req)
}
func (t *HTTP) Append(ctx context.Context, id uint64, req raft.AppendRequest) (raft.AppendResponse, error) {
	return call[raft.AppendRequest, raft.AppendResponse](ctx, t, id, "/raft/append", req)
}
func (t *HTTP) Install(ctx context.Context, id uint64, req raft.SnapshotRequest) (raft.AppendResponse, error) {
	return call[raft.SnapshotRequest, raft.AppendResponse](ctx, t, id, "/raft/snapshot", req)
}

func handler[Request, Response any](maxBytes int64, fn func(Request) Response) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req Request
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		decoder := json.NewDecoder(r.Body)
		if decoder.Decode(&req) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(fn(req))
	}
}

func Handler(node *raft.Node, registry *prometheus.Registry, enableCrash bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/vote", handler(1024, node.HandleVote))
	mux.HandleFunc("POST /raft/append", handler(4<<20, node.HandleAppend))
	mux.HandleFunc("POST /raft/snapshot", handler(128<<20, node.HandleInstall))
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		status := node.Status()
		w.Header().Set("Content-Type", "application/json")
		if !status.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(status)
	})
	if enableCrash {
		mux.HandleFunc("POST /fault/crash", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_ = http.NewResponseController(w).Flush()
			os.Exit(88)
		})
	}
	return mux
}
