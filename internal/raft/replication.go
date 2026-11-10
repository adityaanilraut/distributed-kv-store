package raft

import (
	"context"
	"time"
)

func (n *Node) HandleVote(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	res := VoteResponse{Term: n.hard.Term}
	if n.failed != nil || n.peers[req.Candidate] == nil || req.Term < n.hard.Term {
		return res
	}
	changed := req.Term > n.hard.Term
	if changed {
		n.becomeFollower(req.Term, 0)
	}
	res.Term = n.hard.Term
	lastTerm := n.termAt(n.lastIndex())
	upToDate := req.LastTerm > lastTerm || (req.LastTerm == lastTerm && req.LastIndex >= n.lastIndex())
	if upToDate && (n.hard.Vote == 0 || n.hard.Vote == req.Candidate) {
		changed = changed || n.hard.Vote != req.Candidate
		n.hard.Vote = req.Candidate
		n.resetDeadline()
		res.Granted = true
	}
	if changed && !n.persist(0, nil, nil) {
		res.Granted = false
	}
	return res
}

func (n *Node) acceptLeader(term, id uint64) (accepted, changed bool) {
	if n.failed != nil || n.peers[id] == nil || term < n.hard.Term {
		return false, false
	}
	changed = term > n.hard.Term
	n.becomeFollower(term, id)
	return true, changed
}

func (n *Node) HandleAppend(req AppendRequest) AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	ok, changed := n.acceptLeader(req.Term, req.Leader)
	res := AppendResponse{Term: n.hard.Term}
	if !ok {
		return res
	}
	if req.PrevIndex < n.snapshot.Index || req.PrevIndex > n.lastIndex() || n.termAt(req.PrevIndex) != req.PrevTerm {
		switch {
		case req.PrevIndex < n.snapshot.Index:
			res.Next = n.snapshot.Index + 1
		case req.PrevIndex > n.lastIndex():
			res.Next = n.lastIndex() + 1
		default:
			conflict := n.termAt(req.PrevIndex)
			res.Next = req.PrevIndex
			for res.Next > n.snapshot.Index+1 && n.termAt(res.Next-1) == conflict {
				res.Next--
			}
		}
		if changed {
			n.persist(0, nil, nil)
		}
		return res
	}
	var cut uint64
	var additions []Entry
	for offset, entry := range req.Entries {
		index := req.PrevIndex + 1 + uint64(offset)
		if index > n.lastIndex() || n.termAt(index) != entry.Term {
			if index <= n.lastIndex() {
				if index <= n.hard.Commit {
					if changed {
						n.persist(0, nil, nil)
					}
					return res
				}
				cut = index
			}
			additions = req.Entries[offset:]
			break
		}
	}
	matched := req.PrevIndex + uint64(len(req.Entries))
	if commit := min(req.Commit, matched); commit > n.hard.Commit {
		n.hard.Commit = commit
		changed = true
	}
	if (changed || len(additions) > 0) && !n.persist(cut, additions, nil) {
		return res
	}
	if cut != 0 {
		n.entries = n.entries[:cut-n.snapshot.Index-1]
	}
	n.entries = append(n.entries, additions...)
	n.apply()
	n.compact()
	res.Next, res.Success = matched+1, n.failed == nil
	return res
}

func (n *Node) HandleInstall(req SnapshotRequest) AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	ok, changed := n.acceptLeader(req.Term, req.Leader)
	res := AppendResponse{Term: n.hard.Term}
	if !ok {
		return res
	}
	snap := req.Snapshot
	res.Next = snap.Index + 1
	if snap.Index <= n.hard.Commit {
		res.Success = !changed || n.persist(0, nil, nil)
		return res
	}
	var cut uint64
	var suffix []Entry
	if snap.Index <= n.lastIndex() && n.termAt(snap.Index) == snap.Term {
		suffix = append([]Entry(nil), n.entries[snap.Index-n.snapshot.Index:]...)
	} else {
		cut = n.snapshot.Index + 1
	}
	n.hard.Commit = snap.Index
	if !n.persist(cut, nil, &snap) {
		return res
	}
	n.entries, n.snapshot, n.applied = suffix, snap, snap.Index
	n.values = make(map[string][]byte, len(snap.Values))
	for key, value := range snap.Values {
		n.values[key] = value
	}
	res.Success = true
	return res
}

func (n *Node) replicate(id uint64, wake <-chan struct{}) {
	defer n.wg.Done()
	for {
		select {
		case <-n.stop:
			return
		case <-wake:
			for n.send(id) {
				select {
				case <-n.stop:
					return
				default:
				}
			}
		}
	}
}

func (n *Node) send(id uint64) bool {
	n.mu.Lock()
	if n.role != leader || n.failed != nil {
		n.mu.Unlock()
		return false
	}
	p := n.peers[id]
	term, leaderID, next := n.hard.Term, n.config.ID, p.next
	snapshot := n.snapshot
	install := next <= snapshot.Index
	var appendReq AppendRequest
	if !install {
		prev := next - 1
		end := min(n.lastIndex(), prev+maxBatch)
		entries := append([]Entry(nil), n.entries[prev-snapshot.Index:end-snapshot.Index]...)
		appendReq = AppendRequest{term, leaderID, prev, n.termAt(prev), n.hard.Commit, entries}
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	var res AppendResponse
	var err error
	if install {
		res, err = n.transport.Install(ctx, id, SnapshotRequest{term, leaderID, snapshot})
	} else {
		res, err = n.transport.Append(ctx, id, appendReq)
	}
	if err != nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failed != nil || n.observeTerm(res.Term) || n.role != leader || n.hard.Term != term {
		return false
	}
	if !res.Success {
		if res.Next == 0 || res.Next > n.lastIndex()+1 || res.Next == next {
			return false
		}
		p.next = res.Next
		return true
	}
	p.next = res.Next
	p.match = max(p.match, p.next-1)
	n.metrics.lag.WithLabelValues(label(id)).Set(float64(n.lastIndex() - p.match))
	n.advanceCommit()
	return p.next <= n.lastIndex()
}

func (n *Node) advanceCommit() {
	for index := n.lastIndex(); index > n.hard.Commit && n.termAt(index) == n.hard.Term; index-- {
		count := 1
		for _, p := range n.peers {
			if p.match >= index {
				count++
			}
		}
		if count < 3 {
			continue
		}
		n.hard.Commit = index
		if n.persist(0, nil, nil) {
			n.apply()
			n.compact()
			n.notifyPeers()
		}
		return
	}
}
