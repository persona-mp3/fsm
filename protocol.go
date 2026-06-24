package main

import (
	"fmt"
	"log"
)

// AppendEntryReq is sent by a Node, typically a [Leader] to replicate
// a log it received to a [Follower] node. With an empty body, it simply serves
// as a HeartBeat to a [Follower], siginifying that the [Leader] is still alive.
// If the a [Follower] node receives an [AppendEntry] request from a node whose
// [AppendEntry.Term] is lower, it drops it. If higher, updates it's term with
// the rpc's [AppendEntry.Term].
type AppendEntryReq struct {
	Id   string
	Term uint64
	Data string
}

// AppendEntryRes is typically sent by a [Follower] node after an [AppendEntryReq]
// request was received. If the [AppendEntryReq.Term] is lower than it's own [Raft.term]
// the node, disregards the rpc and responds back with it's own [Raft.Term]. If they're
// on they're equal, the node can assume the [AppendEntryReq] came from a legitimate [Leader]
type AppendEntryRes struct {
	Id           string
	Term         uint64
	Data         string
	Acknowledged bool
	err          error
}

func (s *Server) AppendEntryRPC(req AppendEntryReq, res *AppendEntryRes) error {
	log.Printf("(server) recvd appendEntryRPC %+v\n", req)
	replyCh := make(chan RPCReply)
	s.incoming <- RPC{kind: AppendEntry, payload: req, reply: replyCh}
	log.Println("(server) was able to send rpc to node")
	reply := <-replyCh
	switch reply.kind {
	case AppendEntry:
		payload, ok := reply.payload.(*AppendEntryRes)
		if !ok {
			res.err = fmt.Errorf("internal error occured")
			panic(fmt.Sprintf(`
			(server) expected rpcReply from node to match response, *AppendEntryRes
			recvd reply: %+v
			`, reply))
		}

		*res = *payload
	default:
		res.err = fmt.Errorf("internal error occured")
		panic(fmt.Sprintf(`
			(server) expected rpcReply from node to match reply.kind, AppendEntry
			recvd reply: %+v
			`, reply))
	}

	return nil
}

//	func (s *Server) RequestVoteRPC(req RequestVoteReq, res *RequestVoteRes) error {
//		stub := randomTimeout(time.Millisecond).Seconds()
//		if int(stub)%2 == 0 {
//			res.Acked = false
//			res.Reason = "RequestVoteRPC isn't fully implmented"
//		} else {
//			res.Id = fmt.Sprintf("%d-someguy", stub)
//			res.Acked = true
//			res.Reason = "RequestVoteRPC isn't fully implmented, you're lucky"
//		}
//
//		return nil
//	}

type RequestVoteReq struct {
	Id     string
	Term   uint64
	Reason string
}

type RequestVoteRes struct {
	Id     string
	Term   uint64
	Acked  bool
	Reason string
	err    error
}


func (s *Server) RequestVoteRPC(req RequestVoteReq, res *RequestVoteRes) error {
	s.log.Println("revd requestVoteRPC: %+v\n", req)
	replyCh := make(chan RPCReply)
	s.incoming <- RPC{kind: RequestVote, payload: req, reply: replyCh}
	reply := <-replyCh
	payload, ok := reply.payload.(*RequestVoteRes)
	if !ok {
		res.err = fmt.Errorf("an internal error occured")
		s.log.Panicf(`
		expected rpcReply from node to be *RequestVoteRes
		Got: %+v\n
		`, reply,
		)
	}

	*res = *payload
	s.log.Println("sent requestVoteRPCResponse to client")
	return nil
}
