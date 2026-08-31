package main

import (
	"fmt"
	db "fsm/database"
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
	workers     []*Worker
	clusterSize int
	db          db.Database
}

func NewLeaderHandler(workers []*Worker, clusterSize int, database db.Database) leaderHandler {
	return leaderHandler{
		workers:     workers,
		clusterSize: clusterSize,
		db:          database,
	}
}

func (lh leaderHandler) AppendEntryRequest(
	id string,
	req *AppendEntryRequest,
	previousLogEntry *Entry,
	currentTerm uint64,
	latestCommit uint64,
	logger *slog.Logger,

) (RPCReply, RaftState, error) {
	reply := AppendEntryReply{}
	raftResult := lh.verifyAppendEntry(req, *previousLogEntry, currentTerm)

	if raftResult == RaftResultAcked {
		reply = AppendEntryReply{
			Id:               id,
			Result:           raftResult,
			Term:             req.Term,
			Message:          "stepping down from leader",
			PreviousLogIndex: uint64(previousLogEntry.Idx),
			LastCommited:     latestCommit,
		}

		rpcReply := RPCReply{kind: AppendEntry, payload: &reply}
		logger.Info("stepping down from leader to follower")
		return rpcReply, Follower, nil
	}

	rpcReply := RPCReply{kind: AppendEntry, payload: &AppendEntryReply{
		Id:               id,
		Result:           raftResult,
		Term:             req.Term,
		Message:          "ignoring append entry from node",
		PreviousLogIndex: uint64(previousLogEntry.Idx),
		LastCommited:     latestCommit,
	}}

	logger.Info("ignoring append entry payload while a leader from a node",
		slog.Uint64("currentTerm", currentTerm),
		slog.Any("payload", req),
	)
	return rpcReply, Remain, nil

}

func (lh leaderHandler) VoteRequest(
	id string,
	req *VoteRequest,
	previousLogEntry *Entry,
	currentTerm uint64,
	latestCommit uint64,
	logger *slog.Logger,
) (RPCReply, RaftState, error) {
	var rpcReply RPCReply
	if req.Term > currentTerm {
		rpcReply = RPCReply{
			kind: Vote,
			payload: &VoteReply{
				Id:       id,
				Term:     req.Term,
				Result:   RaftResultAcked,
				VotedFor: true,
				Message:  "falling back to leader",
			},
		}

		logger.Info("leader dropping down to follower succesfully updated term due to higher term",
			slog.Uint64("currentTerm", currentTerm),
			slog.Any("voteRPC", req),
		)

		return rpcReply, Follower, nil
	}

	rpcReply = RPCReply{
		kind: Vote,
		payload: &VoteReply{
			Id:       id,
			Term:     req.Term,
			Result:   RaftResultRejectedLeader,
			VotedFor: false,
			Message:  "Coportate espionage is punishable just so you know",
		},
	}
	logger.Info(
		"rejecting voteRPC from a node without a higher term",
		slog.Uint64("currentTerm", currentTerm),
		slog.Any("voteRPC", req),
	)
	return rpcReply, Remain, nil
}

func (lh leaderHandler) SnapshotRequest(
	id string,
	req *SnapshotRequest,
	previousLogEntry *Entry,
	currentTerm uint64,
	latestCommit uint64,
	logger *slog.Logger,
) (RPCReply, RaftState, error) {
	panicMsg := fmt.Sprintf("recvd snapshot request instead of the workers\n%+v\n", req)
	panic(panicMsg)
}

func (lh leaderHandler) CommandRequest(
	id string,
	req *CommandRequest,
	logEntries *Logs,
	currentTerm uint64,
	latestCommit uint64,
	logger *slog.Logger,
) (RPCReply, RaftState, error) {
	var rpcReply RPCReply
	entry, exists := checkLogs(req, currentTerm, logEntries)
	if exists {
		rpcReply = RPCReply{
			kind: ClientCommand,
			payload: &CommandReply{
				From:   id,
				Result: fmt.Sprintf("cached::%s", entry.Value),
			},
		}
		return rpcReply, Remain, nil
	}
	logger.Info("leader-handler begining replication")

	safeForReplication := replicateEntry(entry, lh.workers, lh.clusterSize, logger)
	commandReply := CommandReply{
		From:   "fsm-leader",
		Result: "quorum could not be reached please try again later",
	}

	if !safeForReplication {
		logger.Warn("[debug] entry is not safe for replication")
		rpcReply = RPCReply{kind: ClientCommand, payload: &commandReply}
		return rpcReply, Remain, nil
	}

	logger.Info("commiting entry to logs, safe for replication")
	res, err := lh.Apply(entry, logEntries)
	if err != nil {
		logger.Error("could not apply entry to database", slog.String("err", err.Error()))
		commandReply.Result = err.Error()
	} else {
		commandReply.Result = res.Message
	}

	rpcReply = RPCReply{kind: ClientCommand, payload: &commandReply}
	return rpcReply, Remain, nil
}

// HandleCommandRPC checks if the request already exists in this nodes logs. If it exists in it's logs
// it returns the log and true, otherwise it appends it to the node's logs and returns false
func checkLogs(req *CommandRequest, currentTerm uint64, logs *Logs) (Entry, bool) {
	entry := Entry{
		Operation: req.Operation,
		Term:      currentTerm,
		Key:       req.Key,
		Value:     req.Value,
	}

	if logs.HasEntry(&entry) {
		return entry, true
	}

	logs.Append(&entry)
	return entry, false
}

func (lh leaderHandler) UnknownRequest(
	req any,
	currentTerm uint64,
	latestCommit uint64,
	logger *slog.Logger,

) (RPCReply, RaftState, error) {
	panicMsg := fmt.Sprintf(`
	recvd unknown request::
	%+v,
	`, req)
	panic(panicMsg)
}

// docs: the leader will only ack if it steps down from leader position
// verifyAppendEntry returns [`RaftResuktAcked`] if the request came from a
// node with a higher term. If the request came from a lower term it returns a
// [`RaftResultLowerTerm`]. In the case that the terms are the same, a panic occurs
func (lh leaderHandler) verifyAppendEntry(
	req *AppendEntryRequest,
	previousLogEntry Entry,
	currentTerm uint64,
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
