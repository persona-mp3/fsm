package main

import (
	"fmt"
	"time"
)

func (n *Node) runCandidate() {
	fmt.Println("candidate state succesfully initiated")
	timeout := randomTimeout(time.Millisecond)
	n.raft.electionTimeout = timeout
	timer := time.NewTimer(timeout)

	defer func() {
		if !timer.Stop() {
			<-timer.C
		}

		fmt.Println("candidate mode terminated succesfully ")
	}()

	select {
	case <-timer.C:
		fmt.Println("election timer fired dropping back to Follower")
		if int(timeout.Seconds())%2 == 0 {
			fmt.Println("stub: won the election")
			n.transition <- Leader
			return
		}
		n.transition <- Follower
		return
	case <-n.stateCtx.Done():
		return
	}

}
