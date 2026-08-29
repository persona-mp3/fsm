package main

import (
	db "fsm/database"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

/*
	A follower needs to check it's prevLogIndex and prevLogTerm against the leaders
	If it finds out that it's logs are not in-sync with the leader it sends logs
	out of sync. When the leader recvs it, it sends a snapshot
*/

func (l *Logs) _SnapshotFrom(startIndex, term uint64) []*Entry {
	l.mu.Lock()
	defer l.mu.Unlock()

	if startIndex > uint64(len(l.entries)) || len(l.entries) == 0 {
		return l.entries
	}

	for _, entry := range l.entries {
		if uint64(entry.Idx) == startIndex && term == entry.Term {
			return l.entries[startIndex:]
		}
	}

	// TODO: couldn't find anything so we just send them all our logs
	return l.entries
}

func TestSnapshot(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		gen := rapid.Custom(func(t *rapid.T) *Entry {
			return &Entry{
				Idx:  rapid.Int().Draw(t, "index"),
				Term: rapid.Uint64().Draw(t, "term"),
				Operation: rapid.SampledFrom(
					[]db.Operation{
						db.GetOps, db.SetOps, db.RemoveOps,
					}).Draw(t, "operation"),
				Key:   rapid.StringMatching(`[\x20-\x7E]+`).Draw(t, "key"),
				Value: rapid.StringMatching(`[\x20-\x7E]+`).Draw(t, "value"),
			}
		})

		uint64NumberGen := rapid.Uint64Range(0, 99)
		prevLogIndex := atomic.Uint64{}
		prevLogIndex.Store(uint64NumberGen.Draw(t, "prevLogIndex"))

		lastCommited := atomic.Uint64{}
		lastCommited.Store(uint64NumberGen.Draw(t, "lastCommited"))

		logEntries := rapid.SliceOfN(gen, 0, 100).Draw(t, "logEntries")
		logs := Logs{
			mu:               sync.Mutex{},
			entries:          logEntries,
			lastCommited:     &lastCommited,
			PreviousLogIndex: &prevLogIndex,
		}

		startIndex := uint64NumberGen.Draw(t, "startIndex")
		targetTerm := uint64NumberGen.Draw(t, "targetTerm")
		results := logs._SnapshotFrom(startIndex, targetTerm)

		var expectedResults []*Entry

		// first check if there's a logEntry with index if not we expect the whole thing
		if startIndex > uint64(len(logEntries)) || len(logEntries) == 0 {
			expectedResults = logEntries
		} else {
			found := false
			for _, e := range logEntries {
				if e.Term == targetTerm && uint64(e.Idx) == startIndex {
					expectedResults = logEntries[startIndex:]
					found = true
					break
				}
			}
			// we should just get all the logs then
			if !found {
				expectedResults = logEntries
			}
		}

		assert.Equal(t, results, expectedResults, "snapshot results differ")
	})

}
