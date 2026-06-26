package main

import "fsm/raftlogger"

func (n *Node) runLeader(logger raftlogger.RLogger) {
	logger.Println("leader state transitioned successfully", n.Diagnostics())
	defer func() {
		logger.Println("leader state terminated succesfully")
	}()

	<-n.stateCtx.Done()
}
