```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

type leader struct {
	quitCh     chan struct{}
	workerErrs chan error
}

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

	// 1. Initialise all the workers
	// _workers := initalizeNewWorkers(n.peers, n.logs.getAtomicCommit(), n.logs.PreviousLogIndex, &n.logs)

	// 2. Start all the workers in parallel
	/*
	   wg := sync.WaitGroup{}
	   for _,worker := range _workers {
	     wg.Go(func() {
	       if err := worker.Run(ctx, peerAddr, currentTerm); err != nil {
	         // log error or propagate via struct or channel to notify the leader incase we'd want to
	         // respawn the worker
	       }
	     })
	   }

	   go func() {
	     wg.Wait()
	     tellLeaderToQuitCh <- struct{}{} // close(tellLeaderToQuitCh)
	   }
	*/

	// to track number of workers still active
	wg := sync.WaitGroup{}
	allWorkers := []*Worker{}
	for _, peer := range connectedPeers {
		if peer == nil {
			continue
		}

		worker := NewWorker(
			peer.id,
			n.logs.getAtomicCommit(),
			n.logs.PreviousLogIndex,
			&n.logs,
			logger.With(),
		)
		allWorkers = append(allWorkers, worker)

		wg.Go(func() {
			worker.Run(ctx, n.id, peer, currentTerm)
		})
	}

	// QUESTION: Why do we need this? if the leader has exited, there's no need for these
	n.workers = allWorkers

	go func() {
		wg.Wait()
		logger.Info("all workers have returned")
	}()
	var panicMsg string
	lh := NewLeaderHandler(n.logs.LastCommited())

	for {
		select {
		case <-n.stateCtx.Done():
			return
		case rpcRequest := <-n.incoming:
			switch rpcRequest.kind {
			case AppendEntry:
				request, ok := rpcRequest.payload.(AppendEntryRequest)
				if !ok {
					panicMsg = fmt.Sprintf(
						`recvd unexpected payload. Expected AppendEntry
             payload: %+v,
             ---
             diagnostics:
             %+v
            `, request, n.Diagnostics())
					panic(panicMsg)
				}
				previousLogEntry, _ := n.logs.GetPreviousLogEntry()
				raftResult := lh.verifyAppendEntry(&request, previousLogEntry, currentTerm, logger)

				if raftResult == RaftResultAcked {
					reply := AppendEntryReply{
						Id:               n.id,
						Result:           raftResult,
						Term:             request.Term,
						Message:          "stepping down from leader",
						PreviousLogIndex: uint64(previousLogEntry.Idx),
						LastCommited:     lh.latestCommit,
					}

					rpcReply := RPCReply{kind: AppendEntry, payload: &reply}
					backgroundSendCh(ctx, rpcRequest.reply, rpcReply)
					n.raft.UpdateTerm(request.Term, request.Id)
					n.transition <- Follower
					logger.Info("stepping down from leader to follower")
					return
				}

				rpcReply := RPCReply{kind: AppendEntry, payload: &AppendEntryReply{
					Id:               n.id,
					Result:           raftResult,
					Term:             request.Term,
					Message:          "ignoring append entry from node",
					PreviousLogIndex: uint64(previousLogEntry.Idx),
					LastCommited:     n.logs.LastCommited(),
				}}

				backgroundSendCh(ctx, rpcRequest.reply, rpcReply)
				logger.Info("ignoring append entry payload while a leader from a node",
					slog.Uint64("currentTerm", currentTerm),
					slog.Any("payload", request),
				)
			case Vote:
			}
		}
	}
}

func (n *Node) handleIncomingPayload(req RPC, currentTerm uint64, logger *slog.Logger) {
	switch req.kind {
	// CURRENTLY: Refactoring, moved AppendEntry up to main loop

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

	case Snapshot:
		panic("recvd snapshot request instead of the workers")
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
			return
		}

		// replicate entry accross workers
		// go func(entry Entry, replyCh chan RPCReply, workers []*Worker) {
		safeForReplication := replicateEntry(entry, n.workers, len(n.peers), logger.With())
		reply := CommandReply{
			From:   "fsm-leader",
			Result: "quorum not reached please try again later",
		}

		if !safeForReplication {
			select {
			case req.reply <- RPCReply{kind: ClientCommand, payload: &reply}:
			default:
				return
			}
			return
		}

		// NOTE:
		// we should typically not log or replicate 'GET' commands
		// as jkvs itself does not either. It just causes noise and extra
		// stuff when debugging
		dbResponse, err := n.Apply(entry)
		if err != nil {
			reply.Result = err.Error()
		} else {
			reply.Result = dbResponse.Message
		}
		select {
		case req.reply <- RPCReply{kind: ClientCommand, payload: &reply}:
		default:
			return
		}

		logger.Info("leader inspection", slog.Any("diagnostics", n.Diagnostics()))

	}
}

// HandleCommandRPC checks if the request already exists in this nodes logs. If it exists in it's logs
// it returns the log and true, otherwise it appends it to the node's logs and returns false
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
```
