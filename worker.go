package main

import (
	"context"
	"crypto/rand"
	"log/slog"
	"math/big"
	"time"
)

const (
	MAX_RPC_CALL_RETRIALS = 5
	WORKER_CHAN_BUFFER    = 100
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
	defer func() {
		ticker.Stop()
		if peer.rpcConn != nil {
			peer.rpcConn.Close()
		}
		w.logger.Info("worker exiting, closed connection")

	}()

	leaderCommit := w.leaderCommit

	for {
		select {
		case newCommit := <-w.leaderCommitCh:
			leaderCommit = newCommit
			w.logger.Info("new leader commit recvd", slog.Uint64("leaderCommit", leaderCommit))

		case replica := <-w.replicateCh:
			req := AppendEntryRequest{}
			req.Id = leaderId
			req.Term = currentTerm
			req.LeaderCommit = leaderCommit

			if !attemptSend(req, peer, replica, w.logger.With()) {
				return
			}
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
			req.Id = leaderId
			req.Term = currentTerm
			req.LeaderCommit = leaderCommit

			if !attemptSend(req, peer, replica, w.logger.With()) {
				return
			}
			ticker.Reset(heartbeatInterval)

		case <-ticker.C:
			var failedCalls int
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

func attemptSend(
	req AppendEntryRequest, peer *Peer, replica replicate, logger *slog.Logger,
) bool {
	var failedCalls int
	reply := AppendEntryReply{}
	req.Entry = &replica.entry
	req.Message = "Replicate appendEntryRPC"

	for failedCalls < MAX_RPC_CALL_RETRIALS {
		if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
			logger.Info("failed to Call Server.AppendEntryRPC for heartbeats.",
				slog.String("error", err.Error()), slog.Int("peerId", peer.id),
			)
			failedCalls++
			continue
		}
		break
	}

	if failedCalls == MAX_RPC_CALL_RETRIALS {
		logger.Warn("max fail calls reached", slog.Int("totalFailed", failedCalls))
		return false
	}

	if !reply.Acked {
		replica.done <- false
		logger.Info("follower did not ack log replication", slog.Any("appendEntryReply", reply))
		return false
	}
	replica.done <- true
	replica.success.Add(1)
	logger.Info("sent replication acked back to producer")
	return true
}

func randomTimeout(d time.Duration) time.Duration {
	// crypto/rand requires a *big.Int for limits
	limit := big.NewInt(int64(maxInterval - minInterval + 1))
	n, _ := rand.Int(rand.Reader, limit)

	actualInterval := n.Int64() + int64(minInterval)
	return d * time.Duration(actualInterval)
}
