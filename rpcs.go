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
	Id      string
	Term    uint64
	Message string
	Entry   *Entry
	// temp
	LastCommitIndex int
	LogSize         int
}

type AppendEntryReply struct {
	Id      string
	Term    uint64
	Acked   bool
	Message string
	// temp
	LastCommited int
	LogSize      int
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

type CommandReq struct {
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

func (s *Server) AppendEntryRPC(req AppendEntryRequest, res *AppendEntryReply) error {
	s.log.Println("forwarding appendRPC to node")
	reply := make(chan RPCReply, 1)
	s.incoming <- RPC{kind: AppendEntry, payload: req, reply: reply}

	response := <-reply
	payload, ok := response.payload.(*AppendEntryReply)
	if !ok {
		res = &AppendEntryReply{
			Id:      s.id,
			Message: "this node is down, an internal error occured",
		}

		s.log.Panic(`received unenxpected reply from for AppendEntryRPC. Got: %+v`, payload)
	}

	*res = *payload
	return nil
}

func (s *Server) VoteRequestRPC(req VoteRequest, res *VoteReply) error {
	s.log.Println("forwarding voteRPC to node")
	reply := make(chan RPCReply, 1)
	s.incoming <- RPC{kind: Vote, payload: req, reply: reply}

	response := <-reply
	payload, ok := response.payload.(*VoteReply)
	if !ok {
		res = &VoteReply{
			Id:      s.id,
			Message: "this node is down, an internal error occured",
		}

		s.log.Panic(`received unexpected reply from for VoteRequestRPC. Expected Vote kind. Got: %+v`, payload)
	}

	*res = *payload
	return nil
}

func (s *Server) CommandRPC(req CommandReq, res *CommandReply) error {
	s.log.Println("forwarding commandRPC to node")
	reply := make(chan RPCReply)
	s.incoming <- RPC{kind: ClientCommand, payload: req, reply: reply}
	response := <-reply
	s.log.Println("response from node-state::", response)
	payload, ok := response.payload.(*CommandReply)
	if !ok {
		s.log.Panic("Expected CommandReply, recvd:", payload)
	}

	*res = *payload
	return nil
}
