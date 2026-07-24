package main

import (
	"fmt"
	rlog "fsm/raftlogger"
	"os"
	"sync"
	"time"
)

// RaftState is the current state of the Node
type RaftState int

const (
	Leader RaftState = iota
	Follower
	Candidate
)

// Raft holds the RaftState and information about this node
type Raft struct {
	id string
	// the mutex should be used when reading or updating values that
	// cannot be read atomically
	mu sync.RWMutex

	// state represents the current [RaftState] of this node
	state RaftState

	// term is the internal clock for the node
	// term atomic.Uint64
	//
	// leaderLock sync.Mutex
	// votedFor is the [Leader] this node voted for, for this [Raft.term]

	electionTimeout time.Duration

	termInfo *TermInfo

	log rlog.RLogger
}

type TermInfo struct {
	term     uint64
	votedFor string
	leader   string
	hasVoted bool
}

func NewRaft(id string, initialTimeout time.Duration) *Raft {
	raftLogger := rlog.NewHumaneLogger(id, "raft", 0, os.Stdout)

	termInfo := TermInfo{
		term:     0,
		votedFor: "",
		leader:   "",
		hasVoted: false,
	}

	return &Raft{
		id:              id,
		mu:              sync.RWMutex{},
		state:           Follower,
		termInfo:        &termInfo,
		electionTimeout: initialTimeout,
		log:             raftLogger,
	}
}

// incrementTerm atomically updates the currentTerm of this Node by 1
// This is usually called when the Node transists into a [Candidate] state.
func (r *Raft) IncrementTerm() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.termInfo.term++
}

func (r *Raft) GiveVote(term uint64, votedFor string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.termInfo.term = term
	r.termInfo.votedFor = votedFor
	r.termInfo.hasVoted = true
}

// UpdateTerm updates the current raftTerm and who the new [Leader] of the
// for this term is
func (r *Raft) UpdateTerm(term uint64, leader string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.termInfo.term = term
	r.termInfo.leader = leader
}

// Term returns the current [Raft.term] of this Node
func (r *Raft) Term() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.termInfo.term
}

func (r *Raft) HasVoted() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.termInfo.hasVoted
}

// updateState updates the [Raft.state] to the state provided
func (r *Raft) UpdateState(to RaftState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = to

}

func (r *Raft) State() RaftState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *Raft) ResetElectionTimeout() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	dur := randomTimeout(time.Millisecond)
	r.electionTimeout = dur
	return dur
}

func (r *Raft) CurrentLeader() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.termInfo.leader
}

func (r *Raft) ClearLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.termInfo.leader = ""
}

func (r *Raft) VotedFor() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.termInfo.votedFor
}

func (rs RaftState) String() string {
	switch rs {
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	default:
		panic(fmt.Sprintf("unexpected main.RaftState: %#v", rs))
	}
}
