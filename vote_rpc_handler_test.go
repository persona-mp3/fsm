package main

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestHandleVoteRPC(t *testing.T) {
	testNodeName := fmt.Sprintf("%s-1", t.Name())
	testLogger := slog.New(slog.NewJSONHandler(t.Output(), nil))
	replyCh := make(chan RPCReply, 1)
	testVoteTerm := uint64(9)
	testLowerTerm := uint64(1)
	oldLeader := "oldLeader"
	testCandidate := fmt.Sprintf("test-candidate-%s", t.Name())

	fh := NewFollowerHandler(testNodeName, testLogger)
	testReq := VoteRequest{
		Id:      testCandidate,
		Term:    testVoteTerm,
		Message: "testing that a node in a lower term grants us a vote for provided they haven't voted",
	}

	action := fh.HandleVoteRPC(testReq, oldLeader, testLowerTerm, replyCh)
	reply := <-replyCh

	expectedReply := &VoteReply{
		Id:       testNodeName,
		Term:     testVoteTerm,
		VotedFor: true,
		Message:  "Vote Granted",
	}

	expectedAction := VoteAction{
		grantedVote: true,
		votedFor:    testCandidate,
		termVoted:   testVoteTerm,
	}

	assert.Equal(t, expectedReply, reply.payload)
	assert.Equal(t, expectedAction, action)
}

func TestVoteHandlerRejectsLowerTerm(t *testing.T) {
	nodeId, handler := newTestFollowerHandler(t)
	lowerTerm := uint64(12)
	higherTerm := uint64(42)
	votedFor := ""

	testReq := VoteRequest{
		Id:      "testId",
		Term:    lowerTerm,
		Message: "testing that you reject vote since i have a lower term",
	}

	replyCh := make(chan RPCReply, 1)
	action := handler.HandleVoteRPC(testReq, votedFor, higherTerm, replyCh)
	reply := <-replyCh
	expectedReply := &VoteReply{
		Id:       nodeId,
		Term:     higherTerm,
		VotedFor: false,
		Message:  "vote not granted due to lower term",
	}

	expectedAction := VoteAction{
		grantedVote: false,
		votedFor:    votedFor,
		termVoted:   higherTerm,
	}

	assert.Equal(t, expectedReply, reply.payload)
	assert.Equal(t, expectedAction, action)
}

func newTestFollowerHandler(t *testing.T) (string, Handler) {
	slogger := slog.New(slog.NewJSONHandler(t.Output(), nil))
	id := fmt.Sprintf("%s-%s", t.Name(), uuid.New().String())
	fh := NewFollowerHandler(id, slogger)
	return id, fh
}
