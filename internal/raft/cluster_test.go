package raft

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

type testNetwork struct {
	mu    sync.RWMutex
	nodes [6]*Node
	group [6]int
}

type testLink struct {
	net *testNetwork
	id  uint64
}

func (l testLink) peer(id uint64) (*Node, error) {
	l.net.mu.RLock()
	defer l.net.mu.RUnlock()
	node := l.net.nodes[id]
	if node == nil || l.net.group[l.id] != l.net.group[id] {
		return nil, ErrUnavailable
	}
	return node, nil
}

func (l testLink) Vote(_ context.Context, id uint64, req VoteRequest) (VoteResponse, error) {
	peer, err := l.peer(id)
	if err != nil {
		return VoteResponse{}, err
	}
	return peer.HandleVote(req), nil
}
func (l testLink) Append(_ context.Context, id uint64, req AppendRequest) (AppendResponse, error) {
	peer, err := l.peer(id)
	if err != nil {
		return AppendResponse{}, err
	}
	return peer.HandleAppend(req), nil
}
func (l testLink) Install(_ context.Context, id uint64, req SnapshotRequest) (AppendResponse, error) {
	peer, err := l.peer(id)
	if err != nil {
		return AppendResponse{}, err
	}
	return peer.HandleInstall(req), nil
}

func (network *testNetwork) start(t *testing.T, dir string, id uint64) {
	t.Helper()
	peers := make([]uint64, 0, 4)
	for peer := uint64(1); peer <= 5; peer++ {
		if peer != id {
			peers = append(peers, peer)
		}
	}
	node, err := Open(Config{
		ID: id, Peers: peers, Path: filepath.Join(dir, fmt.Sprintf("%d.db", id)), SnapshotEvery: 16,
	}, testLink{network, id})
	if err != nil {
		t.Fatal(err)
	}
	network.mu.Lock()
	network.nodes[id] = node
	network.mu.Unlock()
}

func (network *testNetwork) stop(id uint64) {
	network.mu.Lock()
	node := network.nodes[id]
	network.nodes[id] = nil
	network.mu.Unlock()
	if node != nil {
		node.Close()
	}
}

func (network *testNetwork) partition(ids ...uint64) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.group = [6]int{}
	for _, id := range ids {
		network.group[id] = 1
	}
}

func (network *testNetwork) leaderIn(group int) *Node {
	network.mu.RLock()
	nodes, groups := network.nodes, network.group
	network.mu.RUnlock()
	var leader *Node
	var term uint64
	for id, node := range nodes {
		if node == nil || groups[id] != group {
			continue
		}
		status := node.Status()
		if status.ID == status.Leader && status.Healthy && status.Term > term {
			leader, term = node, status.Term
		}
	}
	return leader
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cluster did not reach expected state")
}

type observed struct {
	value   string
	unknown bool
}

func TestLinearizabilityWithFaults(t *testing.T) {
	network := &testNetwork{}
	dir := t.TempDir()
	for id := uint64(1); id <= 5; id++ {
		network.start(t, dir, id)
		defer network.stop(id)
	}
	started := time.Now()
	var mu sync.Mutex
	var history []porcupine.Operation

	invoke := func(node *Node, client int, op Operation, timeout time.Duration) Result {
		start := time.Since(started).Nanoseconds()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		res := node.Propose(ctx, op)
		end := time.Since(started).Nanoseconds()
		if res.Err == nil || (op.Kind != "get" && res.Err != ErrNotLeader && res.Err != ErrBusy) {
			mu.Lock()
			history = append(history, porcupine.Operation{
				ClientId: client, Input: op, Call: start, Return: end,
				Output: observed{string(res.Value), res.Err != nil},
			})
			mu.Unlock()
		}
		return res
	}
	work := func(iterations int) {
		var wg sync.WaitGroup
		for client := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range iterations {
					node := network.leaderIn(0)
					if node == nil {
						time.Sleep(10 * time.Millisecond)
						continue
					}
					op := Operation{Kind: "put", Key: "key", Value: []byte(fmt.Sprintf("%d/%d", client, i))}
					if i%4 == 1 {
						op.Kind = "get"
					}
					if i%7 == 6 {
						op.Kind = "delete"
					}
					invoke(node, client, op, 200*time.Millisecond)
				}
			}()
		}
		wg.Wait()
	}
	eventually(t, func() bool { return network.leaderIn(0) != nil })
	work(8)

	crashed := network.leaderIn(0).config.ID
	network.stop(crashed)
	eventually(t, func() bool { return network.leaderIn(0) != nil })
	work(8)
	network.start(t, dir, crashed)

	oldLeader := network.leaderIn(0)
	other := oldLeader.config.ID%5 + 1
	network.partition(oldLeader.config.ID, other)
	minorityDone := make(chan Result, 1)
	go func() {
		minorityDone <- invoke(oldLeader, 4, Operation{"put", "key", []byte("minority")}, 450*time.Millisecond)
	}()
	eventually(t, func() bool { return network.leaderIn(0) != nil })
	work(16)
	if result := <-minorityDone; result.Err == nil {
		t.Fatal("a minority acknowledged a write")
	}
	network.partition()
	eventually(t, func() bool {
		return network.nodes[other].Status().Snapshot > 0 && network.leaderIn(0) != nil
	})
	// Take one replica offline until the rest have compacted past its last index.
	leader := network.leaderIn(0)
	lagging := leader.config.ID%5 + 1
	before := network.nodes[lagging].Status().Commit
	network.stop(lagging)
	work(24)
	eventually(t, func() bool { return leader.Status().Snapshot > before })
	network.start(t, dir, lagging)
	eventually(t, func() bool { return network.nodes[lagging].Status().Snapshot > before })

	eventually(t, func() bool {
		node := network.leaderIn(0)
		return node != nil && invoke(node, 4, Operation{"put", "key", []byte("durable")}, time.Second).Err == nil
	})
	for id := uint64(1); id <= 5; id++ {
		network.stop(id)
	}
	for id := uint64(1); id <= 5; id++ {
		network.start(t, dir, id)
	}
	eventually(t, func() bool {
		node := network.leaderIn(0)
		if node == nil {
			return false
		}
		result := invoke(node, 4, Operation{Kind: "get", Key: "key"}, time.Second)
		return result.Err == nil && result.Found && string(result.Value) == "durable"
	})

	model := porcupine.NondeterministicModel{
		Init: func() []interface{} { return []interface{}{""} },
		Step: func(state, input, output interface{}) []interface{} {
			op, res := input.(Operation), output.(observed)
			switch op.Kind {
			case "get":
				if state == res.value {
					return []interface{}{state}
				}
				return nil
			case "delete":
				if res.unknown {
					return []interface{}{state, ""}
				}
				return []interface{}{""}
			default:
				if res.unknown {
					return []interface{}{state, string(op.Value)}
				}
				return []interface{}{string(op.Value)}
			}
		},
	}
	if len(history) < 100 {
		t.Fatalf("fault history was too small: %d operations", len(history))
	}
	result := porcupine.CheckOperationsTimeout(model.ToModel(), history, 5*time.Second)
	t.Logf("Porcupine checked %d operations: %s", len(history), result)
	if result != porcupine.Ok {
		t.Fatal("history did not pass linearizability checking")
	}
}
