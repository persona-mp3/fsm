package main

type RPCServiceName string

const (
	ServiceNameAppendEntry     RPCServiceName = "Server.AppendEntryRPC"
	ServiceNameAppendEntryVote RPCServiceName = "Server.VoteRequestRPC"
	ServiceNameSnapshot        RPCServiceName = "Server.SnapshotRPC"
)
