package main

import (
	rlog "fsm/raftlogger"
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
func (n *Node) runFollower() {
	term := n.raft.Term()
	logger := rlog.NewHumaneLogger(n.id, "follower", term, n.log.Out())

	ticker := time.NewTicker(n.raft.electionTimeout)
	defer func() {
		ticker.Stop()
		logger.Println("follower mode exited successfully", n.Diagnostics())
	}()

	slogger := slog.New(slog.NewJSONHandler(n.log.Out(), nil))
	handler := NewFollowerHandler(n.id, slogger)
	for {
		select {
		case <-n.stateCtx.Done():
			return
		case <-ticker.C:
			logger.Println("did not recv heartbeat from leader")
			n.transition <- Candidate
			return
		case req := <-n.incoming:
			switch req.kind {
			case AppendEntry:
				request, ok := req.payload.(AppendEntryRequest)
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				action := handler.HandleAppendEntry(
					request,
					n.raft.Term(),
					n.logs.LastCommited(),
					n.logs.Size(),
					n.raft.CurrentLeader(),
					req.reply,
				)

				if !action.action {
					continue
				}

				if request.Entry != nil {
					if !n.logs.Contains(request.Entry) {
						slogger.Info(
							"IN_PROGRESS: received new entry from leader",
							slog.Group("details",
								slog.Any("entry", request.Entry),
								slog.Bool("available", n.logs.Contains(request.Entry)),
							),
						)
						n.logs.Append(request.Entry)
						slog.Info("append new log to entry", slog.Bool("appended", n.logs.Contains(request.Entry)))
					} else {
						slogger.Info(
							"entry in request already exists",
							slog.Any("entry", request.Entry),
							slog.String("currentLogs", n.logs.String()),
						)
					}

				}

				n.raft.UpdateTerm(action.newTerm, action.newLeader)
				ticker.Reset(n.raft.electionTimeout)
				slogger.Info(
					"updated term and reset timeout",
					slog.String("diagnostics", n.Diagnostics()),
				)

			case Vote:
				request, ok := req.payload.(VoteRequest)
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				handler.HandleVoteRPC(request, n.raft, req.reply)

			case ClientCommand:
				slogger.Info("in follower state, need to forward request to leader")
				req.reply <- RPCReply{
					kind: ClientCommand,
					payload: &CommandReply{
						From:   n.id,
						Result: "FOLLOWER_STUB: Forward request to leader, currently follower",
					},
				}

			default:
				logger.Panic("Unhandled RPC Not yet implemented:", req.payload, n.Diagnostics())
			}
		}
	}

}
