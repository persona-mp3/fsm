package main

import (
	"context"
	"log"
	"time"
)

// runFollower starts the Node in a [Follower] state.
// It listens for updates on the [Raft.heartbeat] channel and also an internal
// ticker, that fires based of [Raft.electionTimeout]. If the electionTimeout
// fires, this function returns, and updates the state of this node to
// a [Candidate], otherwise it keeps running until it's ctx is cancelled
func (r *Raft) runFollower(ctx context.Context) {
	log.Println("becoming a follower")
	ticker := time.NewTicker(r.electionTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.updateRaftState(Candidate)
			return
		case <-r.heartbeat:
			log.Println("heartbeat met")
		}
	}

}

func (r *Raft) runCandidate(ctx context.Context) {
	log.Println("running for candidate")
	newTimeout := generateRandomTimeout(time.Millisecond)
	r.electionTimeout = newTimeout

	newTerm := r.incrementTerm()
	log.Println("going all out for a newTerm: ", newTerm)
	// STUB to transition into leader
	time.Sleep(5 * time.Second)
	if seed := generateRandomTimeout(time.Millisecond).Milliseconds(); seed%2 == 0 {
		r.updateRaftState(Leader)
	} else {
		r.updateRaftState(Follower)
	}

}

func (r *Raft) runLeader(ctx context.Context) {
	log.Println("running as leader")
	time.Sleep(3 * time.Second)
}

// Updates the current state of this node
func (r *Raft) updateRaftState(state RaftState) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.state = state
}

// getCurrentState returns the current state of the Node
func (r *Raft) getCurrentState() RaftState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	currentState := r.state
	return currentState
}

func (r *Raft) incrementTerm() int {
	return int(r.term.Add(1))
}
