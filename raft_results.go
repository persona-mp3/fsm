package main

type RaftResult int

const (
	// RaftResultAcked s sent when the Follower successfully acknowledges the Leader
	RaftResultAcked RaftResult = iota

	// RaftResultStaleLeader is sent when the Follower refuses to acknowledege the RPC request from
	// a leader of a previous term.
	RaftResultStaleLeader

	// RaftResultLowerTerm means the Sender was rejected because they had a lower term
	RaftResultLowerTerm

	// RaftResultRejectedLeader means that the Sender was not acknowledged by the Follower as
	// the leader of a current term. This can happen if there was a Split brain or network partition
	RaftResultRejectedLeader

	// RaftResultLogsOutOfSync is sent when the previousLogIndex of the Follower and Leader do not match
	RaftResultLogsOutOfSync

	// RaftResultUnknownUnhandled accounts for situations that are unexpected or unhandled
	RaftResultUnknownUnhandled
)
