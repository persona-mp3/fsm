package main

import (
	"fmt"
	"log"
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
	log.Println()
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
