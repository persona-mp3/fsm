package main

import "log/slog"

type Handler interface {
	HandleAppendEntry(
		req AppendEntryRequest,
		term uint64,
		lastCommitIndex int,
		logSize int,
		leader string,
		ch chan RPCReply,
	) Action

	HandleVoteRPC(
		req VoteRequest,
		raft *Raft,
		ch chan<- RPCReply,
	) VoteAction
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

func (f FollowerHandler) HandleAppendEntry(
	req AppendEntryRequest,
	term uint64,
	lastCommitIndex int,
	logSize int,
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

		action.newLeader = req.Id

	} else if req.Term < term {
		reply.Acked = false
		reply.Message = "Obsolete leader, you are no longer recognized as leader"

		action.action = false
		action.newTerm = term
		action.newLeader = leader

		f.logger.Info("received append entry from a lower term",
			slog.Uint64("currentTerm", term),
			slog.Any("payload", req),
		)
	} else if req.Term == term && req.LastCommited >= lastCommitIndex && req.LogSize >= logSize {
		// TODO: Need to check if this is actually our leader instead of assuming anyone w the same term 
		// and log is our leader.
		reply.Acked = true
		reply.Message = "Acknowledged as leader for current term"

		action.action = true
		action.newTerm = term
		action.newLeader = leader
		f.logger.Info("received append entry from valid leader. Logs match and are up to date",
			slog.Uint64("currentTerm", term),
			slog.Any("payload", req),
		)
		action.newLeader = req.Id

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

type VoteAction struct {
	grantedVote bool
	termVoted   uint64
	votedFor    string
}

func (f FollowerHandler) HandleVoteRPC(
	req VoteRequest,
	raft *Raft,
	ch chan<- RPCReply,
) VoteAction {

	votedFor := raft.VotedFor()
	currentTerm := raft.Term()

	action := VoteAction{
		grantedVote: false,
		termVoted:   currentTerm,
		votedFor:    votedFor,
	}

	reply := &VoteReply{
		Id:   f.Id,
		Term: currentTerm,
	}

	if req.Term <= currentTerm {
		reply.VotedFor = false
		reply.Message = "already voted for current term"

		ch <- RPCReply{
			kind:    Vote,
			payload: reply,
		}

		f.logger.Info("received voteRPC request for current term, rejecting request and not granting vote",
			slog.Uint64("currentTerm", currentTerm),
			slog.Any("payload", req),
		)

		return action
	}

	// we can only vote for them not acknowledge them as leader
	if req.Term > currentTerm {
		reply.VotedFor = true
		reply.Message = "I have grant ye my vote"
		reply.Term = req.Term

		currentTerm = req.Term

		raft.GiveVote(req.Term, req.Id)

		action.termVoted = currentTerm
		action.votedFor = req.Id
		action.grantedVote = true

	}

	ch <- RPCReply{
		kind:    Vote,
		payload: reply,
	}

	f.logger.Info("sent reply to candidate. Granting vote to candidate because term is higher",
		slog.Uint64("currentTerm", currentTerm),
		slog.Any("voteRPC", req),
		slog.Any("reply", reply),
	)
	return action
}
