package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

type hardState struct {
	Term, Vote, Commit uint64
}

type Snapshot struct {
	Index, Term uint64
	Values      map[string][]byte
}

type storage struct{ db *bolt.DB }

var (
	metaBucket = []byte("metadata")
	logBucket  = []byte("log")
	stateKey   = []byte("state")
	snapKey    = []byte("snapshot")
)

func openStorage(path string) (*storage, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{metaBucket, logBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &storage{db}, nil
}

func indexKey(index uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, index)
}

func (s *storage) load() (state hardState, snap Snapshot, entries []Entry, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(metaBucket)
		if data := meta.Get(stateKey); data != nil {
			if err := json.Unmarshal(data, &state); err != nil {
				return err
			}
		}
		if data := meta.Get(snapKey); data != nil {
			if err := json.Unmarshal(data, &snap); err != nil {
				return err
			}
		}
		return tx.Bucket(logBucket).ForEach(func(key, value []byte) error {
			var entry Entry
			if err := json.Unmarshal(value, &entry); err != nil {
				return err
			}
			if entry.Index != snap.Index+uint64(len(entries))+1 || !bytes.Equal(key, indexKey(entry.Index)) {
				return fmt.Errorf("noncontiguous persistent log at index %d", entry.Index)
			}
			entries = append(entries, entry)
			return nil
		})
	})
	return
}

// save puts term, vote, commit and related log changes in one synced transaction.
func (s *storage) save(state hardState, truncateAt uint64, entries []Entry, snap *Snapshot) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		meta, log := tx.Bucket(metaBucket), tx.Bucket(logBucket)
		data, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err := meta.Put(stateKey, data); err != nil {
			return err
		}
		if truncateAt > 0 {
			c := log.Cursor()
			for k, _ := c.Last(); k != nil && binary.BigEndian.Uint64(k) >= truncateAt; k, _ = c.Prev() {
				if err := c.Delete(); err != nil {
					return err
				}
			}
		}
		if snap != nil {
			data, err := json.Marshal(snap)
			if err != nil {
				return err
			}
			if err := meta.Put(snapKey, data); err != nil {
				return err
			}
			for k, _ := log.Cursor().First(); k != nil && binary.BigEndian.Uint64(k) <= snap.Index; k, _ = log.Cursor().First() {
				if err := log.Delete(k); err != nil {
					return err
				}
			}
		}
		for _, entry := range entries {
			data, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if err := log.Put(indexKey(entry.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}
