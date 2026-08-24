package main

import (
	// "fmt"
	"log/slog"
)

type VoteAction struct {
	termVoted   uint64
	votedFor    string
	grantedVote bool
}

type Handler interface {
	HandleAppendEntry(
		req AppendEntryRequest,
		currentTerm uint64,
		previousLogIndex uint64,
		leader string,
		lastCommitIndex uint64,
		logSize int,
		ch chan RPCReply,
	) Action

	HandleVoteRPC(
		req VoteRequest,
		votedFor string,
		currentTerm uint64,
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

	if req.Term < currentTerm {
		reply.VotedFor = false
		reply.Message = "vote not granted due to lower term"

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

	if req.Term == currentTerm && votedFor != "" {
		reply.VotedFor = false
		reply.Message = "vote not granted. already voted for current term"

		ch <- RPCReply{
			kind:    Vote,
			payload: reply,
		}

		f.logger.Info("received voteRPC request for current term, rejecting request since vote has already been given out",
			slog.Uint64("currentTerm", currentTerm),
			slog.Any("payload", req),
		)

		return action
	}

	// we can only vote for them not acknowledge them as leader
	if req.Term > currentTerm {
		reply.VotedFor = true
		reply.Message = "Vote Granted"
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
	req AppendEntryRequest,
	currentTerm uint64,
	previousLogIndex uint64,
	leader string,
	lastCommitIndex uint64,
	logSize int,
	ch chan RPCReply,
) Action {
	reply := AppendEntryReply{}
	action := Action{}

	switch {
	case req.Term < currentTerm:
		action, reply = f.rejectAppendEntry(&req, currentTerm, lastCommitIndex, logSize)
	case req.Term > currentTerm:
		action, reply = f.acceptNewTerm(&req, lastCommitIndex, logSize)
	case req.Term == currentTerm:
		// action, reply := f.process()
		action, reply = f.refactor(&req, currentTerm, previousLogIndex, leader)

	}

	ch <- RPCReply{kind: AppendEntry, payload: &reply}
	return action
}

func (f FollowerHandler) rejectAppendEntry(
	req *AppendEntryRequest,
	currentTerm uint64,
	lastCommitIndex uint64,
	logSize int,
) (Action, AppendEntryReply) {
	reply := AppendEntryReply{
		Id:           f.Id,
		Result:       RaftResultLowerTerm,
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
		slog.Uint64("lastCommitIndex", lastCommitIndex),
		slog.Int("logSize", logSize),
		slog.Any("appendEntryRPC", req),
	)

	return action, reply
}

func (f FollowerHandler) acceptNewTerm(
	req *AppendEntryRequest, lastCommitIndex uint64, logSize int,
) (Action, AppendEntryReply) {
	// TODO: fail fast
	// logsMatch := req.LogSize >= logSize && req.LastCommitIndex >= lastCommitIndex
	action := Action{}
	reply := AppendEntryReply{}

	action.action = true
	action.newLeader = req.Id
	action.newTerm = req.Term

	reply.Id = f.Id
	reply.Result = RaftResultAcked
	reply.Message = "Acknowledged as leader"
	reply.Term = req.Term
	reply.LastCommited = lastCommitIndex
	reply.LogSize = logSize

	return action, reply
}

// currentLeader, logsMatch
// func (f FollowerHandler) proceessAppendEntry(
// req *AppendEntryRequest,
// currentTerm uint64,
// previousLogIndex uint64,
// currentLeader string,
// lastCommitIndex uint64,
// logSize int,
// ) (Action, AppendEntryReply) {
// 	action := Action{}
//
// 	reply := AppendEntryReply{}
// 	reply.Id = f.Id
// 	reply.Term = currentTerm
// 	reply.LastCommited = lastCommitIndex
// 	reply.LogSize = logSize
//
// 	action.newLeader = currentLeader
// 	action.newTerm = currentTerm
//
// 	// TODO: we can just fail fast here if the logs don't match
// 	logsMatch := req.PreviousLogIndex == previousLogIndex
//
// 	switch {
//
// 	// if this node has no leader but the logs match this is a new leader
// 	// from an election we didn't directly witness
// 	case currentLeader == "" && logsMatch:
// 		reply.Result = RaftResultAcked
// 		reply.Message = "Acknowledged as new leader for new term"
// 		reply.Term = req.Term
//
// 		action.action = true
// 		action.newLeader = req.Id
// 		action.newTerm = req.Term
//
// 		if !logsMatch {
// 			f.logger.Info(
// 				fmt.Sprintf("due to absent leader, recognizing peer %s as leader", req.Id),
// 				slog.Any("appendEntryRPC", req),
// 			)
// 		}
//
// 	case currentLeader == req.Id && !logsMatch:
// 		reply.Result = RaftResultLogsOutOfSync
// 		reply.Message = "Recognized as original leader for current term, but logs don't match"
// 		reply.Term = req.Term
//
// 		action.action = true
// 		action.newLeader = req.Id
// 		action.newTerm = req.Term
//
// 		f.logger.Info("appendEntry came from a recognized leader",
// 			slog.String("currentLeader", currentLeader),
// 			slog.Any("appendEntryRPC", req),
// 		)
//
// 		f.logger.Warn("LEADER LOGS AND FOLLOWER LOGS DONT MATCH YET. STILL IN IMPL", slog.Any("appendRPC", req))
//
// 	case currentLeader == "" && !logsMatch:
// 		reply.Result = RaftResultRejectedLeader
// 		reply.Message = "Unacknowledged as a leader of current term. We can ban you, you know that?"
//
// 		action.action = false
// 		f.logger.Info("appendEntry came from an node claiming to be leader with mismatched logs",
// 			slog.Uint64("currentTerm", currentTerm),
// 			slog.String("currentLeader", currentLeader),
// 			slog.Uint64("lastCommitIndex", lastCommitIndex),
// 			slog.Int("logSize", logSize),
// 			slog.Any("appendEntryRPC", req),
// 		)
// 	case currentLeader != "" && req.Id != currentLeader:
// 		reply.Result = RaftResultRejectedLeader
// 		reply.Message = "Unacknowledged as a leader of current term. We can ban you, you know that?"
// 		action.action = false
//
// 		f.logger.Info(
// 			"appendEntry came from a node claiming to be a leader while i have a leader",
// 			slog.Uint64("currentTerm", currentTerm),
// 			slog.String("currentLeader", currentLeader),
// 			slog.Any("appendEntryRPC", req),
// 		)
//
// 	default:
// 		f.logger.Info("unforseen circumstance, printing dump before panic",
// 			slog.Uint64("currentTerm", currentTerm),
// 			slog.String("currentLeader", currentLeader),
// 			slog.Uint64("lastCommitIndex", lastCommitIndex),
// 			slog.Int("logSize", logSize),
// 			slog.Any("appendEntryRPC", req),
// 		)
//
// 		panic("up above there^^^")
// 	}
//
// 	return action, reply
// }

func (f FollowerHandler) refactor(
	req *AppendEntryRequest,
	currentTerm uint64,
	previousLogIndex uint64,
	currentLeader string,
) (Action, AppendEntryReply) {

	action := Action{}
	logsMatch := req.PreviousLogIndex == previousLogIndex
	logsGreater := previousLogIndex > req.PreviousLogIndex

	reply := AppendEntryReply{}
	reply.Id = f.Id
	if logsMatch {
		// check if we they're are our leader
		switch {
		case req.Id == currentLeader:
			reply.Result = RaftResultAcked
			reply.Term = currentTerm
			reply.PreviousLogIndex = previousLogIndex
			reply.Message = "Recognized as original leader for current term, and logs match"
			f.logger.Info("request came from acknowleged leader with matching logs")

			action.action = true
			action.newLeader = req.Id
			action.newTerm = req.Term
		// if this node does not have a leader, but logs match it's safe to assume that this is the leader
		case currentLeader == "":
			reply.Result = RaftResultAcked
			reply.Message = "Acknowledged as new leader for new term"
			reply.PreviousLogIndex = previousLogIndex
			reply.Term = req.Term
			f.logger.Info("accepting new leader due to empty leader and logs match",
				slog.String("newLeader", req.Id),
			)

			action.action = true
			action.newLeader = req.Id
			action.newTerm = req.Term
			// follower trying to pose as leader
		case req.Id != currentLeader:
			reply.Result = RaftResultRejectedLeader
			reply.Term = currentTerm
			reply.PreviousLogIndex = previousLogIndex
			reply.Message = "Unacknowledged as a leader of current term. We can ban you, you know that?"
			f.logger.Info("request came from illegitimate leader, possibly from a follower since logs match",
				slog.String("from", req.Id),
				slog.Uint64("currentTerm", currentTerm),
				slog.Uint64("requestTerm", req.Term),
				slog.String("currentLeader", currentLeader),
			)
			action.action = false
		default:
			f.logger.Info("logsMatch but we feel into dispair, as this case was not forseen",
				slog.Uint64("currentTerm", currentTerm),
				slog.String("currentLeader", currentLeader),
				slog.Any("payload", req),
			)
			panic("unhandled edgecase check when logsMatch log file")
		}
	} else {
		if !logsGreater {
			switch req.Id {
			case currentLeader:
				reply.Result = RaftResultLogsOutOfSync
				reply.Term = currentTerm
				reply.PreviousLogIndex = previousLogIndex
				reply.Message = "Recognized as original leader for current term, but logs don't match"
				f.logger.Info("request came from acknowleged leader with matching logs")
				action.action = true
				action.newLeader = req.Id
				action.newTerm = req.Term
			}
		} else {
			reply.Result = RaftResultRejectedLeader
			reply.Term = currentTerm
			reply.PreviousLogIndex = previousLogIndex
			reply.Message = "Unacknowledged as a leader of current term. We can ban you, you know that?"
			action.action = false
			f.logger.Info("logs dont match and they're not our leader",
				slog.Uint64("currentTerm", currentTerm),
				slog.String("currentLeader", currentLeader),
				slog.Any("payload", req),
			)
		}

	}
	return action, reply
}
