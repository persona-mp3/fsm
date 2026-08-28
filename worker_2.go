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

			success := w.handleReplicateCommand(&reply)
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

				switch reply.Result {
				case RaftResultAcked:
					w.logger.Info("heartbeat recognized by follower")
					// CURRENTLY: When we send the follower a snapshot request
					// Do we do it here?
				case RaftResultLogsOutOfSync:
					fmt.Printf(`[debug] getting snapshot for: prevIndex:%d, prevTerm:%d\n`,
						reply.PreviousLogIndex, reply.PreviousLogTerm)

					snapshot := w.getSnapshot(reply.PreviousLogIndex, reply.PreviousLogTerm)
					ssReq := SnapshotRequest{}
					ssReq.Id = leaderId
					ssReq.Term = currentTerm
					ssReq.LastCommited = w.leaderCommit.Load()
					ssReq.Snapshot = snapshot
					ssReq.Message = "snapshot message"
					ssReq.Result = RaftResultAcked

					fmt.Printf(`[debug] sending snapshot to follower: %+v`, snapshot)

					go func() {
						ssReply := SnapshotReply{}
						err := attemptRequest(ServiceNameSnapshot, ssReq, &ssReply, peer, w.logger)
						if err != nil {
							fmt.Printf("[debug] failed to send SSRequest. Reason %+v\n", err)
							return
						}
						fmt.Printf("[debug] successfully sent snapshotReq %+v\n", reply)
					}()

				default:
					fmt.Printf("[debug] got rejected by follower or demoted or paniced")
					fmt.Printf("payload:%+v", reply)
					return
				}
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

				success := w.handleReplicateCommand(&reply)
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

func (w *Worker) getSnapshot(previousLogIndex, previousLogTerm uint64) []Entry {
	snapshot, err := w.logEntries.SnapshotFrom(previousLogIndex, previousLogTerm)
	if err != nil {
		snapshot = w.logEntries.Snapshot()
	}
	return snapshot
}
