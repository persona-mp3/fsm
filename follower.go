package main

import (
	rlog "fsm/raftlogger"
	"time"
)

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
			logger.Println("recvd rpc, reseting timer %+v\n", req)
			ticker.Reset(n.raft.electionTimeout)
		}
	}

}

// for when the custom logger has been impl
