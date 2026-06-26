package main

// RPCKind singifies that kind of payload the RPCRequest is and the expected Reply
type RPCKind int

const (
	AppendEntry RPCKind = iota
	Vote
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
}

type AppendEntryReply struct {
	Id      string
	Term    uint64
	Acked   bool
	Message string
}

type VoteRequest struct {
	Id      string
	Term    uint64
	Message string
}

type VoteReply struct {
	Id       string
	Term     string
	votedFor bool
	Message  string
}

func (s *Server) AppendEntryRPC(req AppendEntryRequest, res *AppendEntryReply) error {
	s.log.Println("forwarding appendRPC to node")
	reply := make(chan RPCReply)
	s.incoming <- RPC{kind: AppendEntry, payload: req, reply: reply}

	response := <-reply
	payload, ok := response.payload.(*AppendEntryReply)
	if !ok {
		res = &AppendEntryReply{
			Id:      s.id,
			Message: "this node is down, an internal error occured",
		}

		s.log.Panicf(`received unenxpected reply from for AppendEntryRPC. Got: %+v`, payload)
	}

	*res = *payload
	return nil
}

func (s *Server) VoteRequestRPC(req VoteRequest, res *VoteReply) error {
	s.log.Println("forwarding voteRPC to node")
	reply := make(chan RPCReply)
	s.incoming <- RPC{kind: Vote, payload: req, reply: reply}

	response := <-reply
	payload, ok := response.payload.(*VoteReply)
	if !ok {
		res = &VoteReply{
			Id:      s.id,
			Message: "this node is down, an internal error occured",
		}

		s.log.Panicf(`received unenxpected reply from for AppendEntryRPC. Got: %+v`, payload)
	}

	*res = *payload
	return nil
}

