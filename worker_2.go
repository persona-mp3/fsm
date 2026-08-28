package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

func (w *Worker) run(ctx context.Context, leaderId string, currentTerm uint64, peer *Peer) {
	w.logger.Info("[w2] running")
	ticker := time.NewTicker(2100 * time.Millisecond)
	defer func() {
		ticker.Stop()
		w.cleanUp(peer)
	}()

	// 1. listen on replicaCh
	// 2. listen on heartbeat timer
	var request AppendEntryRequest
	request.Id = leaderId
	request.Term = currentTerm

	for {
		select {
		case replicate := <-w.replicateCh:
			fmt.Println("recvd replica to send to peer::", replicate.entry)
			request.Entry = &replicate.entry
			request.LeaderCommit = w.leaderCommit.Load()
			request.PreviousLogIndex = w.previousLogIndex.Load()
			request.Message = "append new entry to your logs"
			reply := AppendEntryReply{}
			err := attemptRequest(ServiceNameAppendEntry, request, &reply, peer, w.logger)

			if err != nil {
				w.logger.Error("could not send replication request.", "reason", err)
				// TODO: We might not even need the replicate.done ch
				replicate.done <- false
				return
			}

			success := w.handleReplicateCommand(replicate, leaderId, &reply, currentTerm, peer)
			if !success {
				w.logger.Info("[w2] replica was a failure")
				return
			}

			w.logger.Info("[w2] replica was a success")
			ticker.Reset(HeartBeatInterval)

		default:
			select {
			case <-ctx.Done():
				w.logger.Error("worker returning, context cancelled",
					slog.Any("workerId", w.id), slog.Any("reason:", ctx.Err().Error()))
				return
			case <-ticker.C:
				request.Entry = nil
				request.LeaderCommit = w.leaderCommit.Load()
				request.PreviousLogIndex = w.previousLogIndex.Load()
				request.Message = "this is a heartbeat"
				reply := AppendEntryReply{}
				err := attemptRequest(ServiceNameAppendEntry, request, &reply, peer, w.logger)
				if err != nil {
					return
				}
				w.logger.Info("heartbeat recognized by follower",
					slog.Any("currentTerm", request.Term),
					slog.Any("reply", reply),
				)
				ticker.Reset(HeartBeatInterval)

			case replicate := <-w.replicateCh:
				fmt.Println("recvd replica to send to peer::", replicate.entry)
				request.Entry = &replicate.entry
				request.LeaderCommit = w.leaderCommit.Load()
				request.PreviousLogIndex = w.previousLogIndex.Load()
				request.Message = "append new entry to your logs"
				reply := AppendEntryReply{}
				err := attemptRequest(ServiceNameAppendEntry, request, &reply, peer, w.logger)

				if err != nil {
					w.logger.Error("could not send replication request.", "reason", err)
					// TODO: We might not even need the replicate.done ch
					replicate.done <- false
					return
				}

				success := w.handleReplicateCommand(replicate, leaderId, &reply, currentTerm, peer)
				if !success {
					fmt.Println(`[debug] replication failed`)
					replicate.done <- false
					return
				}

				replicate.done <- true
				replicate.success.Add(1)
				fmt.Println(`[debug] replication successfull`)
				ticker.Reset(HeartBeatInterval)
			}
		}
		// here
	}
}

func (w *Worker) cleanUp(peer *Peer) {
	if peer.rpcConn != nil {
		if err := peer.rpcConn.Close(); err != nil {
			w.logger.Error("worker: could not close rpcConn",
				slog.String("reason", err.Error()))
		}
	}

	w.logger.Info("worker resources cleaned up")
}

func attemptRequest[Request any, Reply any](
	service RPCServiceName,
	req Request,
	reply *Reply,
	peer *Peer,
	logger *slog.Logger,
) error {
	var failedDials int
	var rpcErr error
	delay := randomTimeout(time.Millisecond)

	// reply := AppendEntryReply{}
	for failedDials < MAX_RPC_CALL_RETRIALS {
		if rpcErr = peer.rpcConn.Call(string(service), req, reply); rpcErr != nil {
			logger.Error(
				"failed to dial peer, retrying again after",
				slog.Any("failedDials", failedDials),
				slog.Any("peerAddr", peer.addr), slog.Any("reason", rpcErr),
			)
			failedDials++
			time.Sleep(delay)
		}
	}

	if failedDials == MAX_RPC_CALL_RETRIALS {
		return fmt.Errorf("failed to dial contact client after retrials. %w", rpcErr)
	}

	return nil
}

func (w *Worker) handleLogsOutOfSync(reply *AppendEntryReply) []Entry {
	// what we actually want to do is get the snapshot, and just send it over to SnapshotRPC
	snapshot, err := w.logEntries.SnapshotFrom(reply.PreviousLogIndex, reply.PreviousLogTerm)
	if err != nil {
		snapshot = w.logEntries.Snapshot()
	}
	return snapshot
}
