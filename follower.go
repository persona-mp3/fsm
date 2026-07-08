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

				// TODO: The leader can send the same entry as long as it wants? But we'd need to distinguish if
				// we already have this entry the leader has sent, by simply checking against the [Entry.Idx],and [Entry.Term]
				if request.Entry != nil {
					if !n.logs.Contains(request.Entry.Idx, request.Entry.Term) {
						logger.Println("CONSTRUCTION:FOLLOWER_ received a new entry from leader", request.Entry, n.logs.Contains(request.Entry.Idx, request.Entry.Term))
						n.logs.Append(request.Entry)
					} else {
						logger.Println("entry already exists", request.Entry, n.logs.Contains(request.Entry.Idx, request.Entry.Term))
					}

				}

				n.raft.updateTerm(action.newTerm, action.newLeader)
				logger.UpdateTerm(action.newTerm)
				logger.Println("succesfully updated term, timeout reset", n.Diagnostics())
				ticker.Reset(n.raft.electionTimeout)

			case Vote:
				request, ok := req.payload.(VoteRequest)
				// no point in relaying respose backup to the server because the server will still
				// invalidate it and panic
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				action := n.handleVoteRequest(request, req.reply, logger.Inherit("handleVoteRequest"))
				if !action.action {
					continue
				}

				n.raft.updateTerm(action.newTerm, action.newLeader)
				logger.Println("succesfully updated term, timeout reset", n.Diagnostics())
				ticker.Reset(n.raft.electionTimeout)

			case ClientCommand:
				logger.Println("in follower state, need to forward request to leader")
				req.reply <- RPCReply{
					kind: ClientCommand,
					payload: &CommandReply{
						From:   n.id,
						Result: "FOLLOWER_STUB: Forward request to leader, currently follower",
					},
				}

			default:
				logger.Panic("Unhandled RPC Not yet implemented:", req.payload, n.Diagnostics())
			}
		}
	}

}
