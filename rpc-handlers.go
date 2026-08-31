package main

import "log/slog"

type RPCHandler interface {
	AppendEntryRequest(
		id string,
		req *AppendEntryRequest,
		previousLogEntry *Entry,
		currentTerm uint64,
		latestCommit uint64,
		logger *slog.Logger,
	) (RPCReply, RaftState, error)

	VoteRequest(
		id string,
		req *VoteRequest,
		previousLogEntry *Entry,
		currentTerm uint64,
		latestCommit uint64,
		logger *slog.Logger,
	) (RPCReply, RaftState, error)

	CommandRequest(
		id string,
		req *CommandRequest,
		logEntries *Logs,
		currentTerm uint64,
		latestCommit uint64,
		logger *slog.Logger,
	) (RPCReply, RaftState, error)

	SnapshotRequest(
		id string,
		req *SnapshotRequest,
		previousLogEntry *Entry,
		currentTerm uint64,
		latestCommit uint64,
		logger *slog.Logger,
	) (RPCReply, RaftState, error)

	// currently written for leader to panic
	UnknownRequest(
		req any,
		currentTerm uint64,
		latestCommit uint64,
		logger *slog.Logger,
	) (RPCReply, RaftState, error)
}
