package main

import (
	"fmt"
	"log/slog"
	"time"
)

// runFollower runs if the node is in a [Follower] state. If it receives
// an [AppendEntryReq] with a term that is higher or similar, it simply
// resets it's electionTimeout or updates it's [Raft.votedFor] and [Raft.term] if the
// AppendEntryReq has a higher term. A [Follower] cannot grant a vote more than once
// in the same term. For example a Node who sends an [RequestVoteRPC] to this node within
// the same term will be ignored. If a Node also sends an [AppendEntryRPC] with a higher
// term, but the follower did not vote of it, the request is also ignored
func (n *Node) runFollower(logger *slog.Logger) {
	var panicMsg string

	ticker := time.NewTicker(n.raft.electionTimeout)
	defer func() {
		ticker.Stop()
		logger.Info(
			"follower mode exited successfully ", slog.Any("diagnostics", n.Diagnostics()))
	}()

	handler := NewFollowerHandler(n.id, logger)
	for {
		select {
		case <-n.stateCtx.Done():
			return
		case <-ticker.C:
			logger.Info("did not recv heartbeat in time from leader transitioning to Candidate",
				slog.Any("timeout", n.raft.electionTimeout),
			)
			n.transition <- Candidate
			return
		case req := <-n.incoming:
			switch req.kind {
			case AppendEntry:
				request, ok := req.payload.(AppendEntryRequest)
				if !ok {
					panicMsg = fmt.Sprintf(
						`received wrong rpcRequet payload. Expected AppendEntryRequest 
						diagnostics: %+v, 
						request:%s`,
						request, n.Diagnostics())
					panic(panicMsg)
				}

				currentLeader := n.raft.CurrentLeader()
				currentTerm := n.raft.Term()
				previousLogIndex := n.logs.PreviousLogIndex.Load()
				latestCommit := n.logs.LastCommited()

				raftLeaderVerification := verifyLeader(&request, currentLeader, currentTerm, logger)

				if raftLeaderVerification != RaftResultAcked {
					req.reply <- RPCReply{kind: AppendEntry,
						payload: &AppendEntryReply{
							Id:               n.id,
							Term:             currentTerm,
							Result:           raftLeaderVerification,
							Message:          "Not accepted as leader",
							PreviousLogIndex: previousLogIndex,
							LastCommited:     latestCommit,
						}}
					logger.Info("rejected append entry request",
						slog.String("from", request.Id),
						slog.String("reason", raftLeaderVerification.String()),
					)
					continue
				}

				logger.Info("append entry came from verified leader, inspecting commits and logs and updating info")

				// check if leader has sent a new entry to be appended before inspecting logs
				if request.Entry != nil {
					n.logs.Append(request.Entry)
					logger.Info("new payload received from leader, updated prevLogEntry")
				} else {
					logger.Info("empty payload just a hearbeat")
				}

				// update term and leader
				n.raft.UpdateTerm(request.Term, request.Id)

				previousLogEntry, logSize := n.logs.GetPreviousLogEntry()

				replyPayload := AppendEntryReply{}
				replyPayload.Id = n.id
				replyPayload.Term = request.Term
				replyPayload.LastCommited = latestCommit
				replyPayload.PreviousLogIndex = uint64(previousLogEntry.Idx)
				replyPayload.PreviousLogTerm = previousLogEntry.Term

				logStatus := inspectLogs(&request, previousLogEntry, latestCommit, logSize, logger)

				switch logStatus {
				case LogStatusOutOfSync:
					replyPayload.Result = RaftResultLogsOutOfSync
					replyPayload.Message = "Logs out of sync"
				case LogStatusMatch:
					replyPayload.Result = RaftResultAcked
					replyPayload.Message = "Logs match"
				case LogStatusUpdateCommit:
					n.logs.FlushTill(request.LeaderCommit, n.database)
					replyPayload.Result = RaftResultAcked
					replyPayload.LastCommited = request.LeaderCommit
				default:
					msg :=
						fmt.Sprintf(`
					unhandled case of enum type LogStatus when after inspectLogs()
					LogStatusRecvd: %s, request: %+v`, logStatus.String(), request)

					panic(msg)
				}

				ticker.Reset(n.raft.electionTimeout)

				logger.Info("election timer reset, heartbeat arrived and sending response to server",
					slog.String("diagnostics", n.Diagnostics()),
				)
				// TODO|REVIEW: Replace with [backgroundSendCh]
				req.reply <- RPCReply{kind: AppendEntry, payload: &replyPayload}
				logger.Info("heartbeat response sent to server")

			case Vote:
				request, ok := req.payload.(VoteRequest)
				if !ok {
					panicMsg = fmt.Sprintf(`
					Unexpected  RPC payload. Expected VoteRequest, got
					request: %+v
					---
					diagnostics: %+v
					`, req, n.Diagnostics())
					panic(panicMsg)
				}

				votedFor := n.raft.VotedFor()
				currentTerm := n.raft.Term()
				voteAction := handler.HandleVoteRPC(request, votedFor, currentTerm, req.reply)
				if voteAction.grantedVote {
					n.raft.GiveVote(voteAction.termVoted, voteAction.votedFor)
				}

			case ClientCommand:
				req.reply <- RPCReply{
					kind: ClientCommand,
					payload: &CommandReply{
						From:   n.id,
						Result: "[follower] Forward request to leader, currently follower",
					},
				}

			case Snapshot:
				fmt.Printf("[FOLLOWER] recvd snapshot request: %+v\n", req)
				panic("[MAGNENTS]")
			default:
				panicMsg = fmt.Sprintf(
					`Unhandled RPC Not yet implemented:
					request: %+v
					---
					diagnostics: %+v
					`, req, n.Diagnostics())
				panic(panicMsg)
			}
		}
	}

}

// verifyLeader checks that the AppendEntry can be acknowledged by checking it's payload
// against it's stored leader and current term. When verifyLeader returns [RaftResultAcked] they
// should update the status of the node to the payloads ID
func verifyLeader(
	req *AppendEntryRequest,
	currentLeader string,
	currentTerm uint64, logger *slog.Logger,
) RaftResult {
	termsMatch := req.Term == currentTerm
	fromHigherTerm := req.Term > currentTerm
	fromLowerTerm := req.Term < currentTerm

	if fromLowerTerm {
		return RaftResultLowerTerm
	}

	// check if req.Id and leaders match
	if termsMatch {
		// we don't have a leader currently, but our terms match. This can happen when a cluster has
		// just be spun and this node is still waiting
		switch currentLeader {
		case req.Id, "":
			return RaftResultAcked

		// at this point, the request came from a fellow follower claiming to be a leader
		default:
			logger.Info(
				"recvd AppendEntry from a fellow follower claiming to be leader",
				slog.String("currentLeader", currentLeader),
				slog.String("from", req.Id), slog.Uint64("currentTerms", currentTerm),
			)
			return RaftResultRejectedLeader
		}
	}

	if fromHigherTerm {
		logger.Info(
			"recvd AppendEntry from a higher term,  assigning them as leader",
			slog.String("currentLeader", currentLeader),
			slog.String("from", req.Id),
			slog.Uint64("higherTerm", req.Term),
			slog.Uint64("currentTerm", currentTerm),
		)
		return RaftResultAcked
	}

	errMsg := fmt.Sprintf(`
	Unhandled case when verifying leader in verifyLeader.
	currentLeader: %s, currentTerm: %d
	requestPayload: %+v
	`, currentLeader, currentTerm, req)

	panic(errMsg)
}

// inspectLogs check's its log state against the leaders request. Callers SHOULD updated the logs
// first before calling inspectLogs
func inspectLogs(
	req *AppendEntryRequest,
	previousLogEntry Entry,
	latestCommit uint64,
	logSize int,
	logger *slog.Logger,
) LogStatus {

	if previousLogEntry.Idx != int(req.PreviousLogIndex) || previousLogEntry.Term != req.PreviousLogTerm {
		logger.Info("previous log indexes don't match with leader",
			slog.Uint64("previousLogIndex", uint64(previousLogEntry.Idx)),
			slog.Uint64("previousLogTerm", uint64(previousLogEntry.Term)),
			slog.Uint64("leaderPrevLogIndex", req.PreviousLogIndex),
			slog.Uint64("leaderPrevLogIndexTerm", req.PreviousLogTerm),
		)
		return LogStatusOutOfSync
	}

	if req.LeaderCommit == latestCommit {
		return LogStatusMatch
	}

	if req.LeaderCommit > uint64(logSize) {
		logger.Info("leader commit is greater than the amount of logs stored. sending LogStatusOutOfSync",
			slog.Uint64("LeaderCommit", req.LeaderCommit),
			slog.Int("logSize", logSize),
			slog.Uint64("lastCommited", latestCommit),
		)
		return LogStatusOutOfSync
	}

	// QUESTION: Should the leader commit ever be smaller than ours? Would that mean that the impl
	// is not correct? After all, while leaderCommit shows all the logs that can be safely applied
	// Well, if the leader says it's whatever, or less than ours, then we'll need -> LogStatusOutOfSync
	// when it's less than ours. If it's greater, we simply update our commit
	if req.LeaderCommit > latestCommit {
		return LogStatusUpdateCommit
	} else if req.LeaderCommit < latestCommit {
		logger.Warn("follower's commit is ahead of leaderCommit",
			slog.Uint64("latestCommit", uint64(latestCommit)),
			slog.Uint64("leaderCommit", req.LeaderCommit),
		)
		return LogStatusOutOfSync
	}

	msg := fmt.Sprintf(`fol
	unhandled case in checking logs status of request
	logSize: %d, 
	latestCommit: %d,
	previousLogEntry: %+v
	payload: %+v
	`, logSize, latestCommit, previousLogEntry, req)

	panic(msg)
}
