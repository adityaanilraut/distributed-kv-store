package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/adityaanilraut/distributed-kv-store/api"
	"github.com/adityaanilraut/distributed-kv-store/internal/raft"
	"github.com/adityaanilraut/distributed-kv-store/internal/transport"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func cluster() (uint64, []uint64, map[uint64]string, error) {
	if name := os.Getenv("POD_NAME"); name != "" {
		ordinal, err := strconv.ParseUint(strings.TrimPrefix(name, "raft-"), 10, 64)
		if err != nil || ordinal >= 5 {
			return 0, nil, nil, fmt.Errorf("invalid StatefulSet pod name %q", name)
		}
		peers := make(map[uint64]string)
		ids := make([]uint64, 0, 4)
		for i := uint64(0); i < 5; i++ {
			if i != ordinal {
				ids = append(ids, i+1)
				peers[i+1] = fmt.Sprintf("http://raft-%d.raft:8080", i)
			}
		}
		return ordinal + 1, ids, peers, nil
	}
	id, err := strconv.ParseUint(env("NODE_ID", "1"), 10, 64)
	if err != nil {
		return 0, nil, nil, err
	}
	var ids []uint64
	peers := make(map[uint64]string)
	for _, entry := range strings.Split(os.Getenv("PEERS"), ",") {
		pair := strings.SplitN(entry, "=", 2)
		if len(pair) != 2 {
			return 0, nil, nil, fmt.Errorf("expected peer ID=URL, got %q", entry)
		}
		peer, err := strconv.ParseUint(pair[0], 10, 64)
		if err != nil {
			return 0, nil, nil, err
		}
		ids = append(ids, peer)
		peers[peer] = pair[1]
	}
	return id, ids, peers, nil
}

func main() {
	data := flag.String("data", env("DATA_FILE", "raft.db"), "bbolt database")
	peerAddr := flag.String("peer", env("PEER_ADDR", ":8080"), "Raft HTTP address")
	grpcAddr := flag.String("grpc", env("GRPC_ADDR", ":50051"), "client gRPC address")
	flag.Parse()
	id, ids, addresses, err := cluster()
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(*data), 0700); err != nil {
		log.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	node, err := raft.Open(raft.Config{ID: id, Peers: ids, Path: *data, Register: registry}, transport.New(addresses))
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()
	peerServer := &http.Server{
		Addr: *peerAddr, Handler: transport.Handler(node, registry, os.Getenv("DEMO_FAULTS") == "1"),
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 4 * time.Second, WriteTimeout: 4 * time.Second,
	}
	listener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(16 << 10))
	api.RegisterKeyValueServer(server, &transport.KeyValue{Node: node})
	go func() {
		if err := peerServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatal(err)
		}
	}()
	log.Printf("node %d listens for peers on %s and clients on %s", id, *peerAddr, *grpcAddr)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals
	server.Stop()
	_ = peerServer.Close()
}
