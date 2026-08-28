package main

import (
	db "fsm/database"
)

// RPCKind singifies that kind of payload the RPCRequest is and the expected Reply
type RPCKind int

const (
	AppendEntry RPCKind = iota
	Vote
	ClientCommand
	Snapshot
)

type RPC struct {
	kind    RPCKind
	payload any
	reply   chan RPCReply
}

type RPCReply struct {
	kind    RPCKind
	payload any
}

type AppendEntryRequest struct {
	// Id is the id of the current node in the cluster and is recognized by other nodes in the cluster
	Id string

	// Term denotes the current term of the sender.
	Term uint64

	// serves for debugging
	Message string

	// Entry is a new log entry the leader has received from clients
	Entry *Entry

	// PreviousLogIndex is sent by the leader to help the Follower check if they're in sync
	PreviousLogIndex uint64
	// PreviousLogTerm is sent by the leader to help the Follower check if they're in sync
	PreviousLogTerm uint64

	// LeaderCommit is the most recent log index that has been applied to the database of the leader
	// and is now safe for logs up to this point to be applied for the followers
	LeaderCommit uint64
}

type AppendEntryReply struct {
	Id               string
	Term             uint64
	Result           RaftResult
	Message          string
	PreviousLogIndex uint64
	PreviousLogTerm  uint64
	LogSize          int
	LastCommited     uint64
}

type VoteRequest struct {
	Id      string
	Term    uint64
	Message string
}

type VoteReply struct {
	Id       string
	Term     uint64
	VotedFor bool
	Message  string
}

type Operation string

const (
	Set    Operation = "set"
	Get    Operation = "get"
	Remove Operation = "rm"
)

type CommandRequest struct {
	From      string
	Operation db.Operation
	Key       string
	Value     string
	Result    string
}

type CommandReply struct {
	From   string
	Result string
}

type SnapshotRequest struct {
	Id           string
	Term         uint64
	Result       RaftResult
	Message      string
	Snapshot     []Entry
	LastCommited uint64
}

type SnapshotReply struct {
	Id               string
	Term             uint64
	Result           RaftResult
	Message          string
	PreviousLogIndex uint64
	PreviousLogTerm  uint64
	LastCommited     uint64
}
