package main

import (
	"fmt"
	db "fsm/database"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestSnapshot(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// 1. Generate sequential logs instead of completely random IDs
		numEntries := rapid.IntRange(0, 100).Draw(t, "numEntries")
		logEntries := make([]*Entry, numEntries)

		for i := range numEntries {
			logEntries[i] = &Entry{
				Idx:  i, // Sequential index makes slicing safe and predictable
				Term: rapid.Uint64Range(0, 20).Draw(t, fmt.Sprintf("term-%d", i)),
				Operation: rapid.SampledFrom([]db.Operation{
					db.GetOps, db.SetOps, db.RemoveOps,
				}).Draw(t, fmt.Sprintf("op-%d", i)),
				Key:   rapid.StringMatching(`[\x20-\x7E]+`).Draw(t, fmt.Sprintf("key-%d", i)),
				Value: rapid.StringMatching(`[\x20-\x7E]+`).Draw(t, fmt.Sprintf("val-%d", i)),
			}
		}

		uint64NumberGen := rapid.Uint64Range(0, 120)

		prevLogIndex := atomic.Uint64{}
		prevLogIndex.Store(uint64NumberGen.Draw(t, "prevLogIndex"))

		lastCommited := atomic.Uint64{}
		lastCommited.Store(uint64NumberGen.Draw(t, "lastCommited"))

		logs := Logs{
			entries:          logEntries,
			lastCommited:     &lastCommited,
			PreviousLogIndex: &prevLogIndex,
		}

		startIndex := uint64NumberGen.Draw(t, "startIndex")
		targetTerm := uint64NumberGen.Draw(t, "targetTerm")

		results := logs.SnapshotFrom(startIndex, targetTerm)

		var expectedResults []*Entry

		found := false
		// Search for the entry where both Term and Idx match
		for sliceIdx, e := range logEntries {
			if e.Term == targetTerm && uint64(e.Idx) == startIndex {
				expectedResults = logEntries[sliceIdx:]
				found = true
				break
			}
		}

		// If no matching Term/Idx combination is found, fallback
		if !found {
			expectedResults = logEntries
		}

		assert.Equal(t, expectedResults, results, "snapshot results differ")
	})
}
