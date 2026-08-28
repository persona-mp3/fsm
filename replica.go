package main

import (
	"fmt"
	"log/slog"
)

func (w *Worker) handleReplicateCommand(
	replicate replicate,
	leaderId string,
	reply *AppendEntryReply,
	currentTerm uint64,
	peer *Peer,
) bool {
	var panicMsg string
	switch reply.Result {
	case RaftResultLowerTerm | RaftResultRejectedLeader | RaftResultStaleLeader:
		w.logger.Warn("got demoted from leader by peer", slog.Any("payload", reply))
		return false
	case RaftResultUnknownUnhandled:
		// we don't know if the server is still up, so we just exit instead
		w.logger.Warn("got an UnknownUnhandled from peer", slog.Any("payload", reply))
		return false
	case RaftResultAcked:
		w.logger.Info("successfully replicated by follower")
		return true
	case RaftResultLogsOutOfSync:
		snapshot := w.handleLogsOutOfSync(reply)
		req := SnapshotRequest{}
		req.Id = leaderId
		req.Term = currentTerm
		req.Result = RaftResultSnapshot
		req.Snapshot = snapshot

		reply := SnapshotReply{}
		err := attemptRequest(ServiceNameSnapshot, req, &reply, peer, w.logger)
		if err != nil {
			w.logger.Error("could not send snapshot request", "reason", err)
			return false
		}
		fmt.Println(`[debug] snapshot reply`, reply)
		return true
	default:
		panicMsg = fmt.Sprintf(
			`unhandled case in handleReplicateCommand against reply.Result
				result: %+v, 
				reply Payload: 
				----
				%v
			`, reply.Result, reply,
		)
		panic(panicMsg)
	}
}
