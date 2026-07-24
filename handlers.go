package main

import (
	"fmt"
	"log/slog"
)

type VoteAction struct {
	termVoted   uint64
	votedFor    string
	grantedVote bool
}

type Handler interface {
	HandleAppendEntry(
		req AppendEntryRequest, currentTerm uint64, leader string, lastCommitIndex, logSize int, ch chan RPCReply,
	) Action

	HandleVoteRPC(
		req VoteRequest, votedFor string, currentTerm uint64, ch chan<- RPCReply,
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

func (f FollowerHandler) HandleVoteRPC(
	req VoteRequest,
	votedFor string,
	currentTerm uint64,
	ch chan<- RPCReply,
) VoteAction {

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

// HandleAppendEntry accepts an appendEntryRPC based on the following conditions
//   - Comes from a node who has a higher term and logs are up to date
//   - Comes from a node who has the same term, and the node has not identified its leader
//   - Comes from a node who has the same term and the node has identified it as its leader
//   - Comes from a node
func (f FollowerHandler) HandleAppendEntry(
	req AppendEntryRequest, currentTerm uint64, leader string, lastCommitIndex, logSize int, ch chan RPCReply,
) Action {
	reply := AppendEntryReply{}
	action := Action{}

	switch {
	case req.Term < currentTerm:
		action, reply = f.rejectAppendEntry(&req, currentTerm, lastCommitIndex, logSize)
	case req.Term > currentTerm:
		action, reply = f.acceptNewTerm(&req, lastCommitIndex, logSize)
	case req.Term == currentTerm:
		action, reply = f.proceessAppendEntry(&req, currentTerm, leader, lastCommitIndex, logSize)
	}

	ch <- RPCReply{kind: AppendEntry, payload: &reply}
	return action
}

func (f FollowerHandler) rejectAppendEntry(
	req *AppendEntryRequest, currentTerm uint64, lastCommitIndex, logSize int,
) (Action, AppendEntryReply) {
	reply := AppendEntryReply{
		Id:           f.Id,
		Acked:        false,
		Term:         currentTerm,
		Message:      "Rejected due to lower term",
		LastCommited: lastCommitIndex,
		LogSize:      logSize,
	}

	action := Action{
		action: false,
	}

	f.logger.Info("rejecting appendEntry due to lower term",
		slog.Uint64("currentTerm", currentTerm),
		slog.Int("lastCommitIndex", lastCommitIndex),
		slog.Int("logSize", logSize),
		slog.Any("appendEntryRPC", req),
	)
	return action, reply
}

func (f FollowerHandler) acceptNewTerm(
	req *AppendEntryRequest, lastCommitIndex, logSize int,
) (Action, AppendEntryReply) {
	// TODO: fail fast
	// logsMatch := req.LogSize >= logSize && req.LastCommitIndex >= lastCommitIndex
	action := Action{}
	reply := AppendEntryReply{}

	action.action = true
	action.newLeader = req.Id
	action.newTerm = req.Term

	reply.Id = f.Id
	reply.Acked = true
	reply.Message = "Acknowledged as leader"
	reply.Term = req.Term
  reply.LastCommited = lastCommitIndex
  reply.LogSize = logSize

	return action, reply
}

// currentLeader, logsMatch
func (f FollowerHandler) proceessAppendEntry(
	req *AppendEntryRequest, currentTerm uint64, currentLeader string, lastCommitIndex, logSize int,
) (Action, AppendEntryReply) {
	action := Action{}

	reply := AppendEntryReply{}
	reply.Id = f.Id
	reply.Term = currentTerm
	reply.LastCommited = lastCommitIndex
	reply.LogSize = logSize

	action.newLeader = currentLeader
	action.newTerm = currentTerm

	// TODO: we can just fail fast here if the logs don't match
	logsMatch := req.LastCommitIndex >= lastCommitIndex && req.LogSize >= logSize

	switch {

	case currentLeader == "" && logsMatch:
		reply.Acked = true
		reply.Message = "Acknowledged as new leader for new term"
		reply.Term = req.Term

		action.action = true
		action.newLeader = req.Id
		action.newTerm = req.Term

		f.logger.Info(
			fmt.Sprintf("due to absent leader, recognizing peer %s as leader", req.Id),
			slog.Any("appendEntryRPC", req),
		)

	case currentLeader == req.Id:
		reply.Acked = true
		reply.Message = "Recognized as original leader for current term"
		reply.Term = req.Term

		action.action = true
		action.newLeader = req.Id
		action.newTerm = req.Term

		f.logger.Info("appendEntry came from a recognized leader",
			slog.String("currentLeader", currentLeader),
			slog.Any("appendEntryRPC", req))

	case currentLeader == "" && !logsMatch:
		reply.Acked = false
		reply.Message = "Unacknowledged as a leader of current term. We can ban you, you know that?"

		action.action = false
		f.logger.Info("appendEntry came from an node claiming to be leader with mismatched logs",
			slog.Uint64("currentTerm", currentTerm),
			slog.String("currentLeader", currentLeader),
			slog.Int("lastCommitIndex", lastCommitIndex),
			slog.Int("logSize", logSize),
			slog.Any("appendEntryRPC", req),
		)
	case currentLeader != "" && req.Id != currentLeader:
		reply.Acked = false
		reply.Message = "Unacknowledged as a leader of current term. We can ban you, you know that?"
		action.action = false
		f.logger.Info(
			"appendEntry came from a node claiming to be a leader while i have a leader",
			slog.Uint64("currentTerm", currentTerm),
			slog.String("currentLeader", currentLeader),
			slog.Any("appendEntryRPC", req),
		)

	default:
		f.logger.Info("unforseen circumstance, printing dump before panic",
			slog.Uint64("currentTerm", currentTerm),
			slog.String("currentLeader", currentLeader),
			slog.Int("lastCommitIndex", lastCommitIndex),
			slog.Int("logSize", logSize),
			slog.Any("appendEntryRPC", req),
		)

		panic("up above there^^^")
	}

	return action, reply
}
