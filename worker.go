package main

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

const (
	// MAX_RPC_CALL_RETRIALS is the maxium amout of time a [Worker] can dial a peer for sending rpcs
	// before exiting
	MAX_RPC_CALL_RETRIALS = 5

	// WORKER_CHAN_BUFFER is the maximum amount of packets a worker can receive before being blocked
	// This is tuned to the same level as [NETWORK_CHAN_BUFFER]
	WORKER_CHAN_BUFFER = 100

	// WORKER_SEND_TIMEOUT is the maximum waiting time for a worker to receive a packet via
	// it's [Worker] via it's channels.
	WORKER_SEND_TIMEOUT = time.Millisecond * 180
)

// Worker is used by the leader to send heatbeats to the followers. If the leader
// decides to send out new [AppendEntries] for the cluster to replicate it uses
// the [Worker.replicateCh] to do this
type Worker struct {
	id           int
	replicateCh  chan replicate
	leaderCommit *atomic.Uint64
	// for new changes to recent commits to keep in sync
	logger *slog.Logger
}

func NewWorker(id int, leaderCommit *atomic.Uint64, logger *slog.Logger) *Worker {
	return &Worker{
		id:           id,
		replicateCh:  make(chan replicate, WORKER_CHAN_BUFFER),
		leaderCommit: leaderCommit,
		logger:       logger,
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

	var failedCalls int
	for {
		if failedCalls == MAX_RPC_CALL_RETRIALS {
			w.logger.Warn("max retrials reached for rpcClient. worker exiting")
			return
		}
		select {
		// case newCommit := <-w.leaderCommitCh:
		// 	leaderCommit = newCommit
		// 	w.logger.Info("new leader commit recvd", slog.Uint64("leaderCommit", leaderCommit))

		case replica := <-w.replicateCh:
			req := AppendEntryRequest{}
			req.Id = leaderId
			req.Term = currentTerm
			req.LeaderCommit = w.leaderCommit.Load()

			if !attemptSend(req, peer, replica, w.logger.With()) {
				return
			}
			ticker.Reset(HeartBeatInterval)
		default:
			select {
			case <-ctx.Done():
				return

			// case newCommit := <-w.leaderCommitCh:
			// 	leaderCommit = newCommit
			// 	w.logger.Info("new leader commit recvd", slog.Uint64("leaderCommit", leaderCommit))

			case replica := <-w.replicateCh:
				req := AppendEntryRequest{}
				req.Id = leaderId
				req.Term = currentTerm
				req.LeaderCommit = w.leaderCommit.Load()

				if !attemptSend(req, peer, replica, w.logger.With()) {
					return
				}
				ticker.Reset(HeartBeatInterval)

			case <-ticker.C:
				req := AppendEntryRequest{}
				req.Id = leaderId
				req.LeaderCommit = w.leaderCommit.Load()
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

			delay := randomTimeout(time.Millisecond)
			logger.Warn("failed to attemptSend appendEntry to client",
				slog.Any("timeout before calling again", delay),
				slog.Int("failedCalls", failedCalls),
			)
			time.Sleep(delay)
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
