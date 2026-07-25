package main

import (
	"context"
	"log/slog"
	"time"
)

const (
	MAX_RPC_CALL_RETRIALS = 5
	WORKER_CHAN_BUFFER     = 100
)

func NewWorker(id int, initialCommit uint64, logger *slog.Logger) *Worker {
	return &Worker{
		id:             id,
		replicateCh:    make(chan replicate, WORKER_CHAN_BUFFER),
		leaderCommit:   initialCommit,
		leaderCommitCh: make(chan uint64, WORKER_CHAN_BUFFER),
		logger:         logger,
	}
}

func (w *Worker) Run(
	ctx context.Context, leaderId string, peer *Peer, currentTerm uint64, heartbeat time.Duration,
) {
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()

	var leaderCommit uint64 = w.leaderCommit
	var failedCalls int

	for {
		if failedCalls == MAX_RPC_CALL_RETRIALS {
			w.logger.Info("max rpc call retrials reached, worker exiting",
				slog.Int("failedCalls", failedCalls),
			)
			return
		}
		select {
		case newCommit := <-w.leaderCommitCh:
			leaderCommit = newCommit
			w.logger.Info("new leader commit recvd", slog.Uint64("leaderCommit", leaderCommit))

		case replica := <-w.replicateCh:
			req := AppendEntryRequest{}
			reply := AppendEntryReply{}

			req.Id = leaderId
			req.Term = currentTerm
			req.Entry = &replica.entry
			req.LeaderCommit = leaderCommit
			req.Message = "Replicate appendEntryRPC"

			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				w.logger.Info("failed to Call Server.AppendEntryRPC for heartbeats.",
					slog.String("error", err.Error()), slog.Int("peerId", peer.id),
				)
				failedCalls++
				continue
			}

			failedCalls = 0
			if !reply.Acked {
				replica.done <- false
				w.logger.Info("follower did not ack log replication", slog.Any("appendEntryReply", reply))
				continue
			}
			replica.done <- true
			replica.success.Add(1)
			w.logger.Info("sent replication acked back to producer")
			ticker.Reset(heartbeatInterval)
		default:
		}
		select {
		case <-ctx.Done():
			return

		case newCommit := <-w.leaderCommitCh:
			leaderCommit = newCommit
			w.logger.Info("new leader commit recvd", slog.Uint64("leaderCommit", leaderCommit))

		case replica := <-w.replicateCh:
			req := AppendEntryRequest{}
			reply := AppendEntryReply{}

			req.Id = leaderId
			req.Term = currentTerm
			req.Entry = &replica.entry
			req.LeaderCommit = leaderCommit
			req.Message = "Replicate appendEntryRPC"

			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				w.logger.Info("failed to Call Server.AppendEntryRPC for heartbeats.",
					slog.String("error", err.Error()), slog.Int("peerId", peer.id),
				)
				failedCalls++
				continue
			}

			failedCalls = 0
			if !reply.Acked {
				replica.done <- false
				w.logger.Info("follower did not ack log replication", slog.Any("appendEntryReply", reply))
				continue
			}
			replica.done <- true
			replica.success.Add(1)
			w.logger.Info("sent replication acked back to producer")
			ticker.Reset(heartbeatInterval)

		case <-ticker.C:
			req := AppendEntryRequest{}
			req.Id = leaderId
			req.LeaderCommit = leaderCommit
			req.Term = currentTerm

			reply := AppendEntryReply{}
			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				w.logger.Info("failed to Call Server.AppendEntryRPC for heartbeats.",
					slog.String("error", err.Error()), slog.Int("peerId", peer.id),
				)
				failedCalls++
				continue
			}
			failedCalls = 0
			if !reply.Acked {
				w.logger.Info("reply from heartbeatRPC was not recognized by follower exiting", slog.Int("workerId", w.id), slog.Any("heartbeatRPC", reply))
				return
			}

			w.logger.Info("reply from heartbeatRPC was recognized by follower", slog.Int("workerId", w.id), slog.Any("heartbeatRPC", reply))
			ticker.Reset(heartbeat)
		}
	}
}
