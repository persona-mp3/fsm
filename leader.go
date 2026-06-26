package main

import "fsm/raftlogger"

func (n *Node) runLeader(logger raftlogger.RLogger) {
	logger.Println("leader state transitioned successfully", n.Diagnostics())
	defer func() {
		logger.Println("leader state terminated succesfully")
	}()

	for {
		select {
		case <-n.stateCtx.Done():
			return
		case req := <-n.incoming:
			switch req.kind {
			case AppendEntry:
				request, ok := req.payload.(AppendEntryRequest)
				// no point in relaying respose backup to the server because the server will still invalidate it and panic
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				action := n.handleAppendEntry(request, req.reply, logger.Inherit("handleAE"))
				if !action.action {
					continue
				}

				n.raft.updateTerm(action.newTerm, action.newLeader)
				logger.Println("leader dropping down to follower succesfully updated term, timeout reset", n.Diagnostics())
				n.transition <- Follower

			default:
				logger.Panic("Unhandled RPC Not yet implemented:", req.payload, n.Diagnostics())
			}
		}
	}
}
