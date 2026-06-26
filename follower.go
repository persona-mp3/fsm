package main

import (
	rlog "fsm/raftlogger"
	"time"
)

// runFollower runs if the node is in a [Follower] state. If it receives
// an [AppendEntryReq] with a term that is higher or similar, it simply
// resets it's electionTimeout or updates it's [Raft.votedFor] and [Raft.term] if the
// AppendEntryReq has a higher term. A [Follower] cannot grant a vote more than once
// in the same term. For example a Node who sends an [RequestVoteRPC] to this node within
// the same term will be ignored. If a Node also sends an [AppendEntryRPC] with a higher
// term, but the follower did not vote of it, the request is also ignored
func (n *Node) runFollower() {
	term := n.raft.getTerm()
	logger := rlog.NewHumaneLogger(n.id, "follower", term, n.log.Out())

	ticker := time.NewTicker(n.raft.electionTimeout)
	defer func() {
		ticker.Stop()
		logger.Println("follower mode exited successfully", n.Diagnostics())
	}()

	for {
		select {
		case <-n.stateCtx.Done():
			return
		case <-ticker.C:
			logger.Println("did not recv heartbeat from leader")
			n.transition <- Candidate
			return
		case req := <-n.incoming:
			switch req.kind {
			case AppendEntry:
				request, ok := req.payload.(AppendEntryRequest)
				// no point in relaying respose backup to the server because the server will still
				// invalidate it and panic
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				action := n.handleAppendEntry(request, req.reply, logger.Inherit("handleAE"))
				if !action.action {
					continue
				}

				n.raft.updateTerm(action.newTerm, action.newLeader)
				logger.Println("succesfully updated term, timeout reset", n.Diagnostics())
				ticker.Reset(n.raft.electionTimeout)

			default:
				logger.Panic("Unhandled RPC Not yet implemented:", req.payload, n.Diagnostics())
			}
		}
	}

}
