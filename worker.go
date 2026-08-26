package main

import (
	"context"
	"fmt"
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
	id               int
	replicateCh      chan replicate
	leaderCommit     *atomic.Uint64
	previousLogIndex *atomic.Uint64
	logEntries       *Logs
	logger           *slog.Logger
}

func NewWorker(
	id int,
	leaderCommit *atomic.Uint64,
	previousLogIndex *atomic.Uint64,
	logEntries *Logs,
	logger *slog.Logger,
) *Worker {
	logger.Info("starting woker with following config:",
		slog.Int("id", id),
		slog.Uint64("leaderCommit: ", leaderCommit.Load()),
	)
	return &Worker{
		id:               id,
		replicateCh:      make(chan replicate, WORKER_CHAN_BUFFER),
		leaderCommit:     leaderCommit,
		previousLogIndex: previousLogIndex,
		logger:           logger,
		logEntries:       logEntries,
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
		case replica := <-w.replicateCh:
			req := AppendEntryRequest{}
			req.Id = leaderId
			req.Term = currentTerm
			req.LeaderCommit = w.leaderCommit.Load()
			req.PreviousLogIndex = w.previousLogIndex.Load()

			if !w.attemptSend(req, peer, replica, w.logger.With()) {
				return
			}
			ticker.Reset(HeartBeatInterval)
		default:
			select {
			case <-ctx.Done():
				return
			case replica := <-w.replicateCh:
				req := AppendEntryRequest{}
				req.Id = leaderId
				req.Term = currentTerm
				req.LeaderCommit = w.leaderCommit.Load()
				req.PreviousLogIndex = w.previousLogIndex.Load()

				if !w.attemptSend(req, peer, replica, w.logger.With()) {
					return
				}
				ticker.Reset(HeartBeatInterval)

			case <-ticker.C:
				req := AppendEntryRequest{}
				req.Id = leaderId
				req.Term = currentTerm
				req.LeaderCommit = w.leaderCommit.Load()
				req.PreviousLogIndex = w.previousLogIndex.Load()

				reply := AppendEntryReply{}
				if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
					w.logger.Info("failed to Call Server.AppendEntryRPC for heartbeats.",
						slog.String("error", err.Error()), slog.Int("peerId", peer.id),
					)
					failedCalls++
					continue
				}
				failedCalls = 0

				if !handleReply(w.logger, currentTerm, w.logEntries, reply) {
					w.logger.Info(
						"reply from heartbeatRPC was not recognized by follower exiting",
						slog.Int("workerId", w.id),
						slog.Any("heartbeatRPC", reply),
					)
					return
				}

				w.logger.Info(
					"reply from heartbeatRPC was recognized by follower",
					slog.Int("workerId", w.id),
					slog.Any("heartbeatRPC", reply),
				)
				ticker.Reset(heartbeat)
			}
		}
	}
}

func (w *Worker) attemptSend(
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

	// if !reply.Acked {
	// 	replica.done <- false
	// 	logger.Info("follower did not ack log replication", slog.Any("appendEntryReply", reply))
	// 	return false
	// }
	if !handleReply(logger, req.Term, w.logEntries, reply) {
		replica.done <- false
		return false
	}
	replica.done <- true
	replica.success.Add(1)
	logger.Info("sent replication acked back to producer")
	return true
}

// TASK|CURRENTLY: Right now, we are only logging the actions but not actually doing anything, 
// esp when we need to send a snapshot to the Follower. Refactor the function or the whole thing
// if you have to(probably might). We've introduce RaftResult and LogStatus types now, so some things should be 
// easier. See follower.go
func handleReply(
	logger *slog.Logger,
	currentTerm uint64,
	logEntries *Logs,
	reply AppendEntryReply,
) bool {
	result := true
	switch reply.Result {
	case RaftResultAcked:
		logger.Info(
			"reply from heartbeatRPC was recognized by follower",
			slog.Any("heartbeatRPC", reply),
		)

	case RaftResultStaleLeader:
		logger.Info(
			"was tagged as stale leader",
			slog.String("from", reply.Id),
			slog.Uint64("replyTerm", reply.Term),
			slog.Uint64("currentTerm", currentTerm),
		)
		result = false

	case RaftResultLowerTerm:
		logger.Info(
			"recvd lower term reply from node",
			slog.String("from", reply.Id),
			slog.Uint64("replyTerm", reply.Term),
			slog.Uint64("currentTerm", currentTerm),
		)
		result = false

	case RaftResultRejectedLeader:
		logger.Info(
			"rejected as leader for current term",
			slog.String("from", reply.Id),
			slog.Uint64("replyTerm", reply.Term),
			slog.Uint64("currentTerm", currentTerm),
		)
		result = false

	case RaftResultLogsOutOfSync:
		logger.Info(
			"follower's log out of sync, preparing for snapshots",
			slog.String("from", reply.Id),
			slog.Uint64("replyTerm", reply.Term),
			slog.Uint64("currentTerm", currentTerm),
			slog.Uint64("followerPrevLogIndex", reply.PreviousLogIndex),
		)
		snapshot, err := logEntries.SnapshotFrom(reply.PreviousLogIndex)
		if err != nil {
			panic(fmt.Sprintf("could not get snapshot of logs. Reason: %d\n", err))
		}

		// Again, would we want to actually make a new RPC from here?
		fmt.Printf("%s\n", fmt.Sprintf("this is good panic;; we got snapshot;; %+v\n", snapshot))

	case RaftResultUnknownUnhandled:
		logger.Warn(
			"this node will panic due to an unhandled case",
			slog.String("from", reply.Id),
			slog.Uint64("replyTerm", reply.Term),
			slog.Uint64("currentTerm", currentTerm),
			slog.Uint64("followerPrevLogIndex", reply.PreviousLogIndex),
			slog.String("message: ", reply.Message),
		)
		result = false

	default:
		msg := fmt.Sprintf(
			"recvd unrecognized RaftResult: %d from node-%s\nPayload: %+v",
			reply.Result, reply.Id, reply)
		panic(msg)
	}

	return result
}
