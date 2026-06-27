package main

import (
	"fmt"
	"io"
	"net/rpc"
	"testing"
	// "time"

	"github.com/stretchr/testify/assert"
)

func TestNodeStepsDown(t *testing.T) {
	addr := "localhost:4000"
	peers := []string{}
	node, err := NewNode(t.Name(), addr, peers, io.Discard)
	if err != nil {
		t.Error("Could not create NewNode ", err)
	}

	go func() {
		if err := node.Run(t.Context()); err != nil {
			t.Error("Could not RunNode ", err)
		}
	}()

	// force it to become a leader
	if node.raft.getState() != Leader {
		node.transition <- Leader
		// node.raft.updateTerm(1, t.Name())
	}

	if node.raft.getState() == Leader {
		// interrupt with a higer vote
		d, err := rpc.Dial("tcp", addr)
		if err != nil {
			t.Error("Could not dial the node that was spawn via rpc: ", err)
			t.FailNow()

		}

		currentTerm := node.raft.getTerm()
		imposedTerm := currentTerm * 10
		req := AppendEntryRequest{Id: "test-package", Term: imposedTerm, Message: ""}
		res := &AppendEntryReply{}

		if err := d.Call("Server.AppendEntryRPC", req, res); err != nil {
			t.Error("Failed to call Server.AppendEntryRPC ", err)
		}

		go func() {
			// we can ignore error here
			reply := make(chan RPCReply)
			node.incoming <- RPC{AppendEntry, AppendEntryRequest{}, reply}
			select {
			case <-reply:
				return
			case <-t.Context().Done():
				return
			}
		}()

		// the node's internal term should match ours
		if node.raft.getTerm() != imposedTerm {
			t.Errorf("Node's term does not match term imposed. Node: %d, imposed: %d\n", node.raft.getTerm(), imposedTerm)
		}

		assert.Equal(t,
			node.raft.getState(), Follower,
			fmt.Sprintf("Expected node's state to be Follower after imposed term: %d. Got: %s",
				imposedTerm, node.raft.getState()),
		)

		if node.raft.getState() != Follower {
			t.Errorf("Expected node's state to be Follower after imposed term: %d. Got: %s", imposedTerm, node.raft.getState())
		}

	}
}

// func TestNodeRejectsLowerTermAppendEntry(t *testing.T) {
// 	id := fmt.Sprintf("%s", t.Name())
// 	addr := "localhost:4001"
// 	peers := []string{}
// 	node, err := NewNode(id, addr, peers, io.Discard)
// 	if err != nil {
// 		t.Error("Could not create NewNode ", err)
// 	}
//
// 	go func() {
// 		if err := node.Run(t.Context()); err != nil {
// 			t.Error("Could not RunNode ", err)
// 		}
// 	}()
//
// 	<-time.After(600 * time.Millisecond)
// 	if node.raft.getTerm() == 1 {
// 		node.raft.incrementTerm()
// 	}
// 	d, err := rpc.Dial("tcp", addr)
// 	if err != nil {
// 		t.Error("Could not dial the node that was spawn via rpc: ", err)
// 	}
//
// 	req := AppendEntryRequest{Id: "test-package", Term: 0, Message: "Should reject my appendEntry because of term"}
// 	res := &AppendEntryReply{}
//
// 	expectedRes := &AppendEntryReply{
// 		Id:      t.Name(),
// 		Term:    node.raft.getTerm(),
// 		Acked:   false,
// 		Message: "you have an outdated term",
// 	}
//
// 	if err := d.Call("Server.AppendEntryRPC", req, res); err != nil {
// 		t.Error("Failed to call Server.AppendEntryRPC ", err)
// 	}
//
// 	assert.False(t, res.Acked)
// 	assert.EqualExportedValues(t, res, expectedRes,
// 		fmt.Sprintf("Node did not send expected response: %+v, got: %+v\n", expectedRes, res),
// 	)
//
// }
