package main

import (
	"fmt"
	"log/slog"
)

func (w *Worker) handleReplicateCommand(
	reply *AppendEntryReply,
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
		fmt.Printf("skipping outOfSyncLogs while replication: %+v\n", reply)
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
