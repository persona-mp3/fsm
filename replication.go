package main

import (
	"context"
	"fsm/raftlogger"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

func (n *Node) StartLeader(logger *slog.Logger) {
	logger.Info("leader state transitioned successfully",
		slog.Any("diagnostics", n.Diagnostics()),
	)

	ctx, cancel := context.WithCancel(n.stateCtx)
	defer cancel()

	// TODO: not sure if we want to redial here, but the connections
	// with the peers should still be kept alive from the election
	connectedPeers := n.getRPCPeers()
	if len(connectedPeers) == 0 {
		logger.Warn(
			"no peers have been connected, possibly dropped or closed from election",
			slog.Any("connectedPeers", connectedPeers),
		)
		n.transition <- Follower
		return
	}

	currentTerm := n.raft.Term()
	// to track number of workers still active

	wg := sync.WaitGroup{}
	allWorkers := []*Worker{}
	for _, peer := range connectedPeers {
		if peer == nil {
			continue
		}

		worker := NewWorker(peer.id, n.logs.LastCommited(), logger.With())
		allWorkers = append(allWorkers, worker)

		wg.Go(func() {
			worker.Run(ctx, n.id, peer, currentTerm, heartbeatInterval)
		})
	}

	n.workers = allWorkers

	go func() {
		wg.Wait()
		logger.Info("all workers have returned")
	}()

	for {
		select {
		case <-n.stateCtx.Done():
			return
		case req := <-n.incoming:
			switch req.kind {
			case AppendEntry:
				request, ok := req.payload.(AppendEntryRequest)
				if !ok {
					logger.Warn(
						"received wrong rpcRequet payload. Expected AppendEntry",
						slog.Any("payload", req.payload),
						slog.String("diagnostics", n.Diagnostics()))
					panic("recvd wrong payload ^^")
				}

				action := n.handleAppendEntry(
					request,
					req.reply,
					raftlogger.NewHumaneLogger(n.id, "AE", n.raft.Term(), nil),
				)

				if !action.action {
					continue
				}

				n.raft.UpdateTerm(action.newTerm, action.newLeader)
				logger.Info("leader dropping down to follower succesfully updated term", "diagnostics", n.Diagnostics())
				n.transition <- Follower
				return

			case Vote:
				request, ok := req.payload.(VoteRequest)
				if !ok {
					logger.Warn("received wrong rpcRequet payload. Expected VoteRPC",
						slog.Any("payload", req.payload),
						slog.String("diagnostics", n.Diagnostics()),
					)
					panic("recvd wrong payload ^^")
				}

				if request.Term > currentTerm {
					req.reply <- RPCReply{
						kind: Vote,
						payload: &VoteReply{
							Id:       n.id,
							Term:     request.Term,
							VotedFor: true,
							Message:  "Ye was a leader, now sunderring",
						},
					}
					logger.Info("leader dropping down to follower succesfully updated term due to higher term",
						slog.Any("voteRPC", request),
						slog.Any("diagnostics", n.Diagnostics()),
					)
					n.raft.GiveVote(request.Term, request.Id)
					n.transition <- Follower
					return
				}

				// TODO(persona) will need to do a check here in the event that two nodes might
				// think they're a leader. We then compare against their logs
				req.reply <- RPCReply{
					kind: Vote,
					payload: &VoteReply{
						Id:       n.id,
						Term:     request.Term,
						VotedFor: false,
						Message:  "Coportate espionage is punishable just so you know",
					},
				}

			case ClientCommand:
				request, ok := req.payload.(CommandRequest)
				if !ok {
					logger.Warn("received wrong rpcRequet payload. Expected CommandRequest",
						slog.Any("payload", req.payload),
						slog.String("diagnostics", n.Diagnostics()),
					)
					panic("recvd wrong payload ^^")
				}

				entry, exists := HandleCommandRPC(&request, currentTerm, &n.logs)
				if exists {
					value, _ := n.logs.Get(entry.Operation, entry.Key)
					req.reply <- RPCReply{
						kind: ClientCommand,
						payload: &CommandReply{
							From:   n.id,
							Result: "LEADER_STUB: I gave you this before, where did you keep it?",
						},
					}
					logger.Info("entry already existed, not repeating duplicate logs",
						slog.Any("entry", entry),
						slog.Bool("exists", exists), slog.String("value", value),
						slog.String("diagnostics", n.Diagnostics()),
					)
					continue
				}
				// replicate entry accross workers
				go func(entry Entry, replyCh chan RPCReply, workers []*Worker) {
					safe := replicateEntry(entry, workers, len(n.peers), logger.With())
					reply := CommandReply{
						From:   "fsm-leader",
						Result: "quorum not reached please try again later",
					}
					if !safe {
						select {
						case replyCh <- RPCReply{kind: ClientCommand, payload: &reply}:
						default:
							return
						}
						return
					}

					reply.Result = "mock: not applied commitment yet as mid refactor"
					select {
					case replyCh <- RPCReply{kind: ClientCommand, payload: &reply}:
					default:
						return
					}
				}(entry, req.reply, allWorkers)
			}
		}
	}
}

func HandleCommandRPC(req *CommandRequest, currentTerm uint64, logs *Logs) (Entry, bool) {
	entry := Entry{
		Operation: req.Operation,
		Term:      currentTerm,
		Key:       req.Key,
		Value:     req.Value,
	}

	if logs.HasEntry(&entry) {
		return entry, true
	}

	logs.Append(&entry)
	return entry, false
}

// todo: not sure of this yet
type Worker struct {
	id           int
	replicateCh  chan replicate
	leaderCommit uint64
	// for new changes to recent commits to keep in sync
	leaderCommitCh <-chan uint64
	logger         *slog.Logger
}

const replicaTimeout = time.Millisecond * 180

// replicateEntry tries to send the new entry to all the workers. If a majority of the workers
// are able to send out their RPC with the new entry, repliacteEntry returns true signalling that
// the leader can apply and commit this entry to the database.
func replicateEntry(
	entry Entry, workers []*Worker, clusterSize int, logger *slog.Logger,
) bool {
	replicasMade := atomic.Uint32{}
	replicasMade.Add(1)

	// done is used to signal when a majority of the workers have responded
	// when a majority has been reached, then the quorum will be calculated
	// this is also placed against a hardSet timeout at the moment of 180ms
	// to make sure slow workers don't delay and we don't hang forever
	done := make(chan bool, len(workers))

	replica := replicate{
		entry:   entry,
		success: &replicasMade,
		done:    done,
	}

	for _, worker := range workers {
		select {
		case worker.replicateCh <- replica:
		default:
			done <- false
			logger.Warn("dropped replica packet because worker is blocked")
		}
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), replicaTimeout)
	defer cancel()

	quorumTarget := (clusterSize / 2) + 1
	for i := range workers {
		_ = i
		if replicasMade.Load() >= uint32(quorumTarget) {
			logger.Info("quorum for replication was reached", slog.Uint64("total", uint64(replicasMade.Load())))
			return true
		}
		select {
		case <-timeoutCtx.Done():
			logger.Info("failed to reach quorum. Could not replicate entries to all workers within deadline")
			return false
		case <-done:
		}
	}

	// incase all the workers send and a quorum for replication still hasn't been reached
	if replicasMade.Load() >= uint32(quorumTarget) {
		logger.Info("quorum for replication was reached", slog.Uint64("total", uint64(replicasMade.Load())))
		return true
	}

	logger.Info("failed to reach quorum. Could not replicate entries to all workers",
		slog.Uint64("total", uint64(replicasMade.Load())),
	)

	return false
}
