package main

import (
	"context"
	"fmt"
	"fsm/raftlogger"
	"log/slog"
	"sync"
	"sync/atomic"
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
			worker.Run(ctx, n.id, peer, currentTerm, HeartBeatInterval)
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

				switch {
				case request.Term > currentTerm:
					req.reply <- RPCReply{
						kind: Vote,
						payload: &VoteReply{
							Id:       n.id,
							Term:     request.Term,
							VotedFor: true,
							Message:  "retreating back to leader",
						},
					}

					n.raft.GiveVote(request.Term, request.Id)
					logger.Info("leader dropping down to follower succesfully updated term due to higher term",
						slog.Any("voteRPC", request),
						slog.Any("diagnostics", n.Diagnostics()),
					)
					n.transition <- Follower
					return

				// TODO(persona) will need to do a check here in the event that two nodes might
				// think they're a leader. We then compare against their logs
				default:
					req.reply <- RPCReply{
						kind: Vote,
						payload: &VoteReply{
							Id:       n.id,
							Term:     request.Term,
							VotedFor: false,
							Message:  "Coportate espionage is punishable just so you know",
						},
					}
					logger.Info(
						"rejecting voteRPC from a node without a higher term",
						slog.Uint64("currentTerm", n.raft.Term()),
						slog.Any("voteRPC", req),
					)
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
							Result: fmt.Sprintf("cached::%s", value),
						},
					}
					continue
				}

				// replicate entry accross workers
				go func(entry Entry, replyCh chan RPCReply, workers []*Worker) {
					safeForReplication := replicateEntry(entry, workers, len(n.peers), logger.With())
					reply := CommandReply{
						From:   "fsm-leader",
						Result: "quorum not reached please try again later",
					}

					if !safeForReplication {
						select {
						case replyCh <- RPCReply{kind: ClientCommand, payload: &reply}:
						default:
							return
						}
						return
					}

					// TODO: continue
					// At this point, we'll need to send the database the command that came from the client
					// The order in which the requests came in, will be the order in which they will 
					// enter the log, be replicated among the cluster and applied to the database
					// But since the network channel only has one person recieving on it, there's an
					// assistance for serializablity and ordered operations. 
					// A client that makes concurrent requests will still land as unique requests so
					// we don't need to worry about that. 
					// 
					// 
					// 
					reply.Result = "mock: not applied commit yet as mid refactor"
					select {
					case replyCh <- RPCReply{kind: ClientCommand, payload: &reply}:
					default:
						return
					}
				}(entry, req.reply, allWorkers)

				logger.Info("leader inspection", slog.Any("diagnostics", n.Diagnostics()))
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

	timeoutCtx, cancel := context.WithTimeout(context.Background(), WORKER_SEND_TIMEOUT)
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
