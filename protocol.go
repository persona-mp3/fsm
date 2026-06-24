package main

import (
	"fmt"
	"log"
	"time"
)

type AppendEntryReq struct {
	Id   string
	Term uint64
	Data string
}

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

func (s *Server) RequestVoteRPC(req RequestVoteReq, res *RequestVoteRes) error {
	stub := randomTimeout(time.Millisecond).Seconds()
	if int(stub)%2 == 0 {
		res.Acked = false
		res.Reason = "RequestVoteRPC isn't fully implmented"
	} else {
		res.Id = fmt.Sprintf("%d-someguy", stub)
		res.Acked = true
		res.Reason = "RequestVoteRPC isn't fully implmented, you're lucky"
	}

	return nil
}
