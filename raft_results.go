package main

import "fmt"

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

type LogStatus int

const (
	// LogStatusOutOfSync signifies that our local logs don't match with the leaders, which could
	// be by log index or log term
	LogStatusOutOfSync LogStatus = iota

	// LogStatusMatch signifies that our local logs match  with the leaders including the leader commits
	LogStatusMatch

	// LogStatusUpdateCommit signifies that we need to update our commited logs to match with the leaders'
	LogStatusUpdateCommit
)

func (rr RaftResult) String() string {
	switch rr {
	case RaftResultAcked:
		return "RaftResultAcked"
	case RaftResultLogsOutOfSync:
		return "RaftResultLogsOutOfSync"
	case RaftResultLowerTerm:
		return "RaftResultLowerTerm"
	case RaftResultRejectedLeader:
		return "RaftResultRejectedLeader"
	case RaftResultStaleLeader:
		return "RaftResultStaleLeader"
	case RaftResultUnknownUnhandled:
		return "RaftResultUnknownUnhandled"
	default:
		panic(fmt.Sprintf("unexpected main.RaftResult: %#v", rr))
	}
}

func (ll LogStatus) String() string {
	switch ll {
	case LogStatusMatch:
		return "LogStatusMatch"
	case LogStatusOutOfSync:
		return "LogStatusOutOfSync"
	case LogStatusUpdateCommit:
		return "LogStatusUpdateCommit"
	default:
		panic(fmt.Sprintf("unexpected main.LogStatus: %#v", ll))
	}
}
