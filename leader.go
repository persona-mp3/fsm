package main

import (
	"fmt"
)

func (n *Node) runLeader() {
	fmt.Println("\n\nleader state transitioned successfully")
	defer func() {
		fmt.Println("leader state terminated succesfully")
	}()

	<-n.stateCtx.Done()
}

