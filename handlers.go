package main

import "log/slog"

type Handler interface {
	HandleAppendEntry(
		req AppendEntryRequest,
		term uint64,
		leaader string,
		ch chan RPCReply,
	) Action

	HandleVoteRPC() Action
}

type FollowerHandler struct {
	Id     string
	logger *slog.Logger
}

func NewFollowerHandler(id string, logger *slog.Logger) Handler {
	return FollowerHandler{
		Id:     id,
		logger: logger,
	}
}

func (f FollowerHandler) HandleAppendEntry (
	req AppendEntryRequest,
	term uint64,
	leader string,
	ch chan RPCReply,
) Action {

	reply := &AppendEntryReply{
		Id:   f.Id,
		Term: term,
	}
	action := Action{}

	if req.Term > term {
		reply.Acked = true
		reply.Message = "Acknowledged as new leader for new term"
		reply.Term = req.Term

		action.action = true
		action.newTerm = req.Term
		action.newLeader = req.Id
		f.logger.Info("received append entry from a new node due to higher rpc",
			slog.Uint64("currentTerm", term),
			slog.Uint64("newTerm", req.Term),
			slog.Any("payload", req),
		)

	}

	if req.Term < term {
		reply.Acked = false
		reply.Message = "Obsolete leader, you are no longer recognized as leader"

		action.action = false
		action.newTerm = term
		action.newLeader = leader

		f.logger.Info("received append entry from a lower term",
			slog.Uint64("currentTerm", term),
			slog.Any("payload", req),
		)
	}

	// if a node while in follower mode gave a vote to someone else
	// votedFor=newCandidate, leader=oldLeader
	// currentTerm=candidateTerm
	if req.Term == term && req.Id == leader {
		reply.Acked = true
		reply.Message = "Acknowledged as leader for current term"

		action.action = true
		action.newTerm = term
		action.newLeader = leader
		f.logger.Info("received append entry from valid leader",
			slog.Uint64("currentTerm", term),
			slog.Any("payload", req),
		)
	} else if req.Term == term && req.Id != leader {
		reply.Acked = false
		reply.Message = "Unacknowledged as a leader of current term. We can ban you, you know that?"

		action.action = false
		action.newTerm = term
		action.newLeader = leader
		f.logger.Info("received append entry from an illegitimate leader for current term",
			slog.Uint64("currentTerm", term),
			slog.Any("payload", req),
		)
	}

	ch <- RPCReply{
		kind:    AppendEntry,
		payload: reply,
	}

	return action
}

func (f FollowerHandler) HandleVoteRPC() Action {
	return Action{}
}
