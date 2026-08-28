package main

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

func (s *Server) CommandRPC(req CommandRequest, res *CommandReply) error {
	reply := make(chan RPCReply)
	s.incoming <- RPC{kind: ClientCommand, payload: req, reply: reply}
	response := <-reply
	payload, ok := response.payload.(*CommandReply)
	if !ok {
		s.log.Panic("Expected CommandReply, recvd:", payload)
	}

	*res = *payload
	return nil
}

func (s *Server) SnapshotRPC(req SnapshotRequest, res *SnapshotReply) error {
	reply := make(chan RPCReply)
	s.incoming <- RPC{kind: Snapshot, payload: req, reply: reply}
	response := <-reply
	payload, ok := response.payload.(*SnapshotReply)
	if !ok {
		s.log.Panic("Expected SnapshotReply, recvd:", payload)
	}
	*res = *payload
	return nil
}
