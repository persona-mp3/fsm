package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

const SEND_TIMEOUT = 120 * time.Millisecond

// as a leader, when we recv an appendEntry RPC we simply want to know if we need to drop
// we drop on the following conditions:
// 1. it came from a node with a Higher Term
// QUESTION: If a node who is a leader for the current term recvs an AppendEntry from
// someone with the same term, but with more logs than it? This can happen in these:
// 1. Split-brain|network partition? -> well if this happened, at least someone would have a higher
// term, so providing this impl is correct, this case should not really happen, (under normal assumptions)
// 2. Follower who is recving appendEntries? this would then mean the impl is not correct
// NOTE that the current follower  impl already handles the case it's commited entries is ahead of
// the leaders'. It simply sends a RaftResultLogsOutOfSync and then the leader has to sync up. While
// it's not still clear how this could happend which could be an error, in the sense that while it
// it was in a previous term, it had the most updated logs and commited entries from the leader
// but then didn't witness the election but someone else [somehow] won the election for the new term
// [But that does mean data WILL get lost] Let's check the impl spec again
//
// 5.4.1 Election Restriction
//
//    Raft uses the voting process to prevent a candidate from
//    winning an election unless its log contains all committed
//    entries. A candidate must contact a majority of the cluster
//    in order to be elected, which means that every committed
//    entry must be present in at least one of those servers. If the
//    candidate’s log is at least as up-to-date as any other log
//    in that majority (where “up-to-date” is defined precisely
//    below), then it will hold all the committed entries. The
//    RequestVote RPC implements this restriction: the RPC
//    includes information about the candidate’s log, and the
//    voter denies its vote if its own log is more up-to-date than
//    that of the candidate.

// Also to check if the persons logs are up-to-date ie has-most-applied-entries, we check
// the req.LeaderCommit against the nodes latestCommit. Do we have to check prevLogIndex?
// Since we're impl this for a leader, I say we don't because ours is the 'source of thruth'
// and if theirs is greater, we simply transition to Follower, and let the Follower
// handle it

// TODO|PROBLEM|QUESTION: So this part of an issue on Voting problem

type leaderHandler struct {
	latestCommit uint64
}

func NewLeaderHandler(latestCommit uint64) leaderHandler {
	return leaderHandler{latestCommit: latestCommit}
}

// docs: the leader will only ack if it steps down from leader position
// verifyAppendEntry returns [`RaftResuktAcked`] if the request came from a
// node with a higher term. If the request came from a lower term it returns a
// [`RaftResultLowerTerm`]. In the case that the terms are the same, a panic occurs
func (lh leaderHandler) verifyAppendEntry(
	req *AppendEntryRequest,
	previousLogEntry Entry,
	currentTerm uint64,
	logger *slog.Logger,
) RaftResult {
	if req.Term < currentTerm {
		return RaftResultLowerTerm
	}

	if req.Term > currentTerm {
		return RaftResultAcked
	}

	previousLogIndex := previousLogEntry.Idx
	previousLogTerm := previousLogEntry.Term
	panicMsg := fmt.Sprintf(
		`follower node sent an append entry request while current leader for current term. sending RaftResultUnhandled
    currentTerm: %d,
    prevLogIndex: %d,
    prevLogTerm: %d,
    request: 
      %v,

    `, currentTerm, previousLogIndex, previousLogTerm, req)
	panic(panicMsg)
}
