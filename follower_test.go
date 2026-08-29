// Co-authored by Claude (claude.ai)

package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewJSONHandler(t.Output(), nil))
}

func TestVerifyLeader(t *testing.T) {
	tests := []struct {
		name          string
		reqId         string
		reqTerm       uint64
		currentLeader string
		currentTerm   uint64
		expected      RaftResult
	}{
		{
			name:          "rejects lower term",
			reqId:         "node-1",
			reqTerm:       1,
			currentLeader: "node-1",
			currentTerm:   5,
			expected:      RaftResultLowerTerm,
		},
		{
			name:          "accepts recognized leader on same term",
			reqId:         "leader-1",
			reqTerm:       5,
			currentLeader: "leader-1",
			currentTerm:   5,
			expected:      RaftResultAcked,
		},
		{
			name:          "accepts any leader when node has no leader yet",
			reqId:         "leader-1",
			reqTerm:       5,
			currentLeader: "",
			currentTerm:   5,
			expected:      RaftResultAcked,
		},
		{
			name:          "rejects unrecognized leader on same term",
			reqId:         "impostor-2",
			reqTerm:       5,
			currentLeader: "leader-1",
			currentTerm:   5,
			expected:      RaftResultRejectedLeader,
		},
		{
			name:          "accepts higher term with existing leader",
			reqId:         "new-leader",
			reqTerm:       10,
			currentLeader: "old-leader",
			currentTerm:   5,
			expected:      RaftResultAcked,
		},
		{
			name:          "accepts higher term with no existing leader",
			reqId:         "new-leader",
			reqTerm:       10,
			currentLeader: "",
			currentTerm:   5,
			expected:      RaftResultAcked,
		},
		{
			name:          "accepts higher term even when same node as current leader",
			reqId:         "leader-1",
			reqTerm:       10,
			currentLeader: "leader-1",
			currentTerm:   5,
			expected:      RaftResultAcked,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &AppendEntryRequest{
				Id:   tt.reqId,
				Term: tt.reqTerm,
			}
			result := verifyLeader(req, tt.currentLeader, tt.currentTerm)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInspectLogs(t *testing.T) {
	tests := []struct {
		name             string
		previousLogEntry Entry
		reqPrevLogIndex  uint64
		reqPrevLogTerm   uint64
		reqLeaderCommit  uint64
		latestCommit     uint64
		logSize          int
		expected         LogStatus
	}{
		{
			name:             "out of sync when log indexes don't match",
			previousLogEntry: Entry{Idx: 3, Term: 2},
			reqPrevLogIndex:  5,
			reqPrevLogTerm:   2,
			reqLeaderCommit:  4,
			latestCommit:     4,
			logSize:          10,
			expected:         LogStatusOutOfSync,
		},
		{
			name:             "out of sync when log terms don't match",
			previousLogEntry: Entry{Idx: 3, Term: 2},
			reqPrevLogIndex:  3,
			reqPrevLogTerm:   5,
			reqLeaderCommit:  4,
			latestCommit:     4,
			logSize:          10,
			expected:         LogStatusOutOfSync,
		},
		{
			name:             "match when logs and commits are in sync",
			previousLogEntry: Entry{Idx: 3, Term: 2},
			reqPrevLogIndex:  3,
			reqPrevLogTerm:   2,
			reqLeaderCommit:  4,
			latestCommit:     4,
			logSize:          10,
			expected:         LogStatusMatch,
		},
		{
			name:             "update commit when leader is ahead and we have the logs",
			previousLogEntry: Entry{Idx: 3, Term: 2},
			reqPrevLogIndex:  3,
			reqPrevLogTerm:   2,
			reqLeaderCommit:  7,
			latestCommit:     4,
			logSize:          10,
			expected:         LogStatusUpdateCommit,
		},
		{
			name:             "out of sync when leader commit is beyond our stored logs",
			previousLogEntry: Entry{Idx: 3, Term: 2},
			reqPrevLogIndex:  3,
			reqPrevLogTerm:   2,
			reqLeaderCommit:  15,
			latestCommit:     4,
			logSize:          10,
			expected:         LogStatusOutOfSync,
		},
		{
			name:             "out of sync when follower has committed more than leader",
			previousLogEntry: Entry{Idx: 3, Term: 2},
			reqPrevLogIndex:  3,
			reqPrevLogTerm:   2,
			reqLeaderCommit:  2,
			latestCommit:     4,
			logSize:          10,
			expected:         LogStatusOutOfSync,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &AppendEntryRequest{
				PreviousLogIndex: tt.reqPrevLogIndex,
				PreviousLogTerm:  tt.reqPrevLogTerm,
				LeaderCommit:     tt.reqLeaderCommit,
			}
			result := inspectLogs(req, tt.previousLogEntry, tt.latestCommit, tt.logSize, testLogger(t))
			assert.Equal(t, tt.expected, result)
		})
	}
}
