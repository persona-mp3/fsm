package main

import (
	rlog "fsm/raftlogger"
	"time"
)

func (n *Node) runCandidate(logger rlog.RLogger) {
	n.raft.incrementTerm()
	logger.UpdateTerm(n.raft.getTerm())

	logger.Println("candidate state succesfully initiated", n.Diagnostics())

	timeout := randomTimeout(time.Millisecond)
	n.raft.electionTimeout = timeout
	timer := time.NewTimer(timeout)

	defer func() {
		if !timer.Stop() {
			<-timer.C
		}

		logger.Println("candidate mode terminated succesfully")
	}()

	select {
	case <-timer.C:
		logger.Println("election timer fired dropping back to Follower")
		if int(timeout.Seconds())%2 == 0 {
			logger.Println("stub: won the election")
			n.transition <- Leader
			return
		}
		n.transition <- Follower
		return
	case <-n.stateCtx.Done():
		return
	}

}
