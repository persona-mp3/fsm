package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFollowerHandlerRejectsLowerTerm(t *testing.T) {
	handler := NewFollowerHandler("1", slog.New(slog.NewJSONHandler(t.Output(), nil)))
	nodeTerm := uint64(4)
	req := AppendEntryRequest{
		Id:    t.Name(),
		Term:  0,
		Entry: nil,
	}
	lastCommitIdx := 4
	logSize := 10
	reply := make(chan RPCReply, 1)

	expectedReply := &AppendEntryReply{
		Id:           "1",
		Term:         nodeTerm,
		Acked:        false,
		Message:      "Rejected due to lower term",
		LastCommited: lastCommitIdx,
		LogSize:      logSize,
	}

	handler.HandleAppendEntry(req, nodeTerm, "", lastCommitIdx, logSize, reply)

	actualReply := <-reply
	assert.Equal(t, actualReply.payload, expectedReply, "expected appendEntryHandler rejects lower term")
}

func TestFollowerHandlerAcceptsHigherTerm(t *testing.T) {
	handler := NewFollowerHandler("1", slog.New(slog.NewJSONHandler(t.Output(), nil)))

	higherTerm := uint64(40)
	req := AppendEntryRequest{
		Id:      t.Name(),
		Term:    higherTerm,
		Message: "this is a hearbeat message",
		Entry:   nil,
	}

	reply := make(chan RPCReply, 1)
	// since the node currently has no leader, it will accept this appendEntry as the new leader for
	// it's term. Even though the logs don't match, it should still reply with it's own details to
	// indicate it's logs are out of sync with this new leader
	action := handler.HandleAppendEntry(req, uint64(1), "", 100, 101, reply)

	expectedReply := &AppendEntryReply{
		Id:           "1",
		Term:         higherTerm,
		Acked:        true,
		Message:      "Acknowledged as leader",
		LastCommited: 100,
		LogSize:      101,
	}

	actualReply := <-reply
	assert.Equal(t, actualReply.payload, expectedReply, "expected appendEntryHandler accepts higher term")

	assert.True(t, action.action, "expected action.action returned from handler to be true for higher term")
	assert.Equal(t, action.newLeader, req.Id, "expected action.newLeader returned from handler to match leader from new term")
	assert.Equal(t, action.newTerm, higherTerm, "expected action.newTerm returned from hadler to match higher term")
}

func TestFollowerHandlerRejectsUnrecognizedLeader(t *testing.T) {
	nodeId := "1"
	handler := NewFollowerHandler(nodeId, slog.New(slog.NewJSONHandler(t.Output(), nil)))

	currentLeaderId := "7"
	currentTerm := uint64(40)

	req := AppendEntryRequest{
		Id:      t.Name(),
		Term:    currentTerm,
		Message: "this is a hearbeat message",
		Entry:   nil,
	}

	reply := make(chan RPCReply, 1)
	action := handler.HandleAppendEntry(req, currentTerm, currentLeaderId, 100, 101, reply)

	expectedReply := &AppendEntryReply{
		Id:           nodeId,
		Term:         currentTerm,
		Acked:        false,
		Message:      "Unacknowledged as a leader of current term. We can ban you, you know that?",
		LastCommited: 100,
		LogSize:      101,
	}

	actualReply := <-reply
	assert.Equal(t, actualReply.payload, expectedReply, "expected appendEntryHandler accepts higher term")

	assert.False(t, action.action, "expected action.action returned from handler to be true for higher term")
	assert.Equal(t, action.newLeader, currentLeaderId, "expected action.newLeader returned from handler to match leader from new term")
	assert.Equal(t, action.newTerm, currentTerm, "expected action.newTerm returned from hadler to match higher term")
}
