package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	follower = iota
	candidate
	leader

	heartbeat = 30 * time.Millisecond
	maxBatch  = 256
)

var (
	ErrNotLeader   = errors.New("not leader")
	ErrUnavailable = errors.New("node unavailable")
	ErrBusy        = errors.New("proposal queue full")
)

type Operation struct {
	Kind, Key string
	Value     []byte
}

type Entry struct {
	Index, Term uint64
	Operation   Operation
}

type Result struct {
	Value  []byte
	Found  bool
	Index  uint64
	Leader uint64
	Err    error
}

type VoteRequest struct {
	Term, Candidate, LastIndex, LastTerm uint64
}
type VoteResponse struct {
	Term    uint64
	Granted bool
}
type AppendRequest struct {
	Term, Leader, PrevIndex, PrevTerm, Commit uint64
	Entries                                   []Entry
}
type AppendResponse struct {
	Term, Next uint64
	Success    bool
}
type SnapshotRequest struct {
	Term, Leader uint64
	Snapshot     Snapshot
}

type Transport interface {
	Vote(context.Context, uint64, VoteRequest) (VoteResponse, error)
	Append(context.Context, uint64, AppendRequest) (AppendResponse, error)
	Install(context.Context, uint64, SnapshotRequest) (AppendResponse, error)
}

type Config struct {
	ID            uint64
	Peers         []uint64
	Path          string
	SnapshotEvery uint64
	Register      prometheus.Registerer
}

type proposal struct {
	op      Operation
	ctx     context.Context
	started time.Time
	reply   chan Result
}

type progress struct {
	next, match uint64
	wake        chan struct{}
}

type Status struct {
	ID, Term, Leader, Commit, Applied, Snapshot uint64
	Healthy                                     bool
}

type Node struct {
	mu        sync.Mutex
	config    Config
	transport Transport
	store     *storage
	hard      hardState
	snapshot  Snapshot
	entries   []Entry
	values    map[string][]byte
	applied   uint64
	role      int
	leader    uint64
	votes     int
	deadline  time.Time
	failed    error
	peers     map[uint64]*progress
	waiters   map[uint64]proposal
	inbox     chan proposal
	stop      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	metrics   *metrics
}

func Open(cfg Config, transport Transport) (*Node, error) {
	seen := map[uint64]bool{cfg.ID: true}
	if cfg.ID == 0 || len(cfg.Peers) != 4 || transport == nil {
		return nil, fmt.Errorf("Raft requires a local ID, four peer IDs, and a transport")
	}
	for _, peer := range cfg.Peers {
		if peer == 0 || seen[peer] {
			return nil, fmt.Errorf("invalid or repeated peer ID %d", peer)
		}
		seen[peer] = true
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 8192
	}
	store, err := openStorage(cfg.Path)
	if err != nil {
		return nil, err
	}
	hard, snapshot, entries, err := store.load()
	if err != nil || hard.Commit < snapshot.Index || hard.Commit > snapshot.Index+uint64(len(entries)) {
		store.db.Close()
		return nil, fmt.Errorf("invalid persisted Raft state: commit=%d, load error=%v", hard.Commit, err)
	}
	n := &Node{
		config: cfg, transport: transport, store: store, hard: hard, snapshot: snapshot,
		entries: entries, values: maps.Clone(snapshot.Values), applied: snapshot.Index,
		peers: make(map[uint64]*progress), waiters: make(map[uint64]proposal),
		inbox: make(chan proposal, 4096), stop: make(chan struct{}), metrics: newMetrics(cfg.Register),
	}
	if n.values == nil {
		n.values = make(map[string][]byte)
	}
	n.apply()
	n.resetDeadline()
	for _, id := range cfg.Peers {
		p := &progress{wake: make(chan struct{}, 1)}
		n.peers[id] = p
		n.wg.Add(1)
		go n.replicate(id, p.wake)
	}
	n.wg.Add(1)
	go n.run()
	return n, nil
}

func (n *Node) Close() {
	n.closeOnce.Do(func() {
		close(n.stop)
		n.wg.Wait()
		n.mu.Lock()
		defer n.mu.Unlock()
		n.fail(ErrUnavailable)
		n.store.db.Close()
	})
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID: n.config.ID, Term: n.hard.Term, Leader: n.leader, Commit: n.hard.Commit,
		Applied: n.applied, Snapshot: n.snapshot.Index, Healthy: n.failed == nil,
	}
}

func (n *Node) Propose(ctx context.Context, op Operation) Result {
	req := proposal{op: op, ctx: ctx, started: time.Now(), reply: make(chan Result, 1)}
	select {
	case <-ctx.Done():
		return Result{Err: ctx.Err()}
	case <-n.stop:
		return Result{Err: ErrUnavailable}
	case n.inbox <- req:
	default:
		return Result{Err: ErrBusy}
	}
	select {
	case result := <-req.reply:
		return result
	case <-ctx.Done():
		return Result{Err: ctx.Err()}
	case <-n.stop:
		return Result{Err: ErrUnavailable}
	}
}

func (n *Node) run() {
	defer n.wg.Done()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	lastHeartbeat := time.Time{}
	for {
		select {
		case <-n.stop:
			return
		case now := <-tick.C:
			n.mu.Lock()
			if n.failed == nil {
				switch {
				case n.role == leader && now.Sub(lastHeartbeat) >= heartbeat:
					n.notifyPeers()
					lastHeartbeat = now
				case n.role != leader && now.After(n.deadline):
					n.startElection()
				}
				for index, req := range n.waiters {
					if req.ctx.Err() != nil {
						delete(n.waiters, index)
					}
				}
			}
			n.mu.Unlock()
		case first := <-n.inbox:
			batch := []proposal{first}
		drain:
			for len(batch) < maxBatch {
				select {
				case req := <-n.inbox:
					batch = append(batch, req)
				default:
					break drain
				}
			}
			n.addProposals(batch)
		}
	}
}

func (n *Node) addProposals(batch []proposal) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var pending []Entry
	for _, req := range batch {
		switch {
		case req.ctx.Err() != nil:
			continue
		case n.failed != nil:
			req.reply <- Result{Err: ErrUnavailable}
		case n.role != leader:
			req.reply <- Result{Err: ErrNotLeader, Leader: n.leader}
		default:
			index := n.lastIndex() + uint64(len(pending)) + 1
			pending = append(pending, Entry{index, n.hard.Term, req.op})
			n.waiters[index] = req
		}
	}
	if len(pending) != 0 && n.persist(0, pending, nil) {
		n.entries = append(n.entries, pending...)
		n.notifyPeers()
	}
}

func (n *Node) resetDeadline() {
	n.deadline = time.Now().Add(150*time.Millisecond + time.Duration(rand.IntN(150))*time.Millisecond)
}

func (n *Node) lastIndex() uint64 { return n.snapshot.Index + uint64(len(n.entries)) }

func (n *Node) termAt(index uint64) uint64 {
	if index == n.snapshot.Index {
		return n.snapshot.Term
	}
	if index < n.snapshot.Index || index > n.lastIndex() {
		return 0
	}
	return n.entries[index-n.snapshot.Index-1].Term
}

func (n *Node) persist(truncateAt uint64, entries []Entry, snapshot *Snapshot) bool {
	if err := n.store.save(n.hard, truncateAt, entries, snapshot); err != nil {
		n.fail(err)
		return false
	}
	return true
}

func (n *Node) fail(err error) {
	if n.failed == nil && !errors.Is(err, ErrUnavailable) {
		slog.Error("Raft storage failed", "node", n.config.ID, "error", err)
	}
	n.failed = err
	for index, req := range n.waiters {
		req.reply <- Result{Err: ErrUnavailable}
		delete(n.waiters, index)
	}
}

func (n *Node) becomeFollower(term, leaderID uint64) {
	if term > n.hard.Term {
		n.hard.Term, n.hard.Vote = term, 0
	}
	n.role, n.leader = follower, leaderID
	// Proposals already accepted can still commit, so a caller cannot safely retry.
	for index, req := range n.waiters {
		req.reply <- Result{Err: ErrUnavailable}
		delete(n.waiters, index)
	}
	n.resetDeadline()
}

func (n *Node) startElection() {
	n.role, n.leader, n.votes = candidate, 0, 1
	n.hard.Term++
	n.hard.Vote = n.config.ID
	n.resetDeadline()
	if !n.persist(0, nil, nil) {
		return
	}
	req := VoteRequest{n.hard.Term, n.config.ID, n.lastIndex(), n.termAt(n.lastIndex())}
	for id := range n.peers {
		n.wg.Add(1)
		go n.requestVote(id, req)
	}
}

func (n *Node) requestVote(id uint64, req VoteRequest) {
	defer n.wg.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	res, err := n.transport.Vote(ctx, id, req)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failed != nil || n.observeTerm(res.Term) || n.role != candidate || n.hard.Term != req.Term || !res.Granted {
		return
	}
	n.votes++
	if n.votes >= 3 {
		n.becomeLeader()
	}
}

func (n *Node) observeTerm(term uint64) bool {
	if term <= n.hard.Term {
		return false
	}
	n.becomeFollower(term, 0)
	n.persist(0, nil, nil)
	return true
}

func (n *Node) becomeLeader() {
	n.role, n.leader = leader, n.config.ID
	n.metrics.elections.Inc()
	for _, p := range n.peers {
		p.next, p.match = n.lastIndex()+1, 0
	}
	// A current-term entry allows the new leader to commit inherited entries safely.
	entry := Entry{Index: n.lastIndex() + 1, Term: n.hard.Term}
	if n.persist(0, []Entry{entry}, nil) {
		n.entries = append(n.entries, entry)
		n.notifyPeers()
	}
}

func (n *Node) notifyPeers() {
	for _, p := range n.peers {
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
}

func (n *Node) apply() {
	for n.applied < n.hard.Commit {
		n.applied++
		entry := n.entries[n.applied-n.snapshot.Index-1]
		op := entry.Operation
		result := Result{Index: n.applied, Leader: n.leader}
		switch op.Kind {
		case "get":
			result.Value, result.Found = n.values[op.Key]
		case "put":
			n.values[op.Key] = op.Value
		case "delete":
			delete(n.values, op.Key)
		}
		if req, ok := n.waiters[n.applied]; ok {
			n.metrics.latency.Observe(time.Since(req.started).Seconds())
			req.reply <- result
			delete(n.waiters, n.applied)
		}
	}
}

func (n *Node) compact() {
	if n.applied-n.snapshot.Index < n.config.SnapshotEvery {
		return
	}
	snap := Snapshot{n.applied, n.termAt(n.applied), maps.Clone(n.values)}
	if n.persist(0, nil, &snap) {
		n.entries = append([]Entry(nil), n.entries[snap.Index-n.snapshot.Index:]...)
		n.snapshot = snap
	}
}
