package main

import (
	"fmt"
	rlog "fsm/raftlogger"
	"os"
	"sync"
	"sync/atomic"
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
	term atomic.Uint64

	leaderLock sync.Mutex
	// votedFor is the [Leader] this node voted for, for this [Raft.term]
	votedFor string

	electionTimeout time.Duration

	log rlog.RLogger
}

func NewRaft(id string) *Raft {
	initialTimeout := randomTimeout(time.Millisecond)
	// prefix := fmt.Sprintf("(%s:raft) ", id)

	// raftLogger := log.New(os.Stdout, prefix, log.Ldate|log.Lmicroseconds|log.Lmsgprefix)
	raftLogger := rlog.NewHumaneLogger(id, "raft", 0, os.Stdout)

	return &Raft{
		id:              id,
		mu:              sync.RWMutex{},
		state:           Follower,
		term:            atomic.Uint64{},
		leaderLock:      sync.Mutex{},
		votedFor:        "",
		electionTimeout: initialTimeout,
		log:             raftLogger,
	}
}

// incrementTerm atomically updates the currentTerm of this Node by 1
// This is usually called when the Node transists into a [Candidate] state.
func (r *Raft) incrementTerm() {
	r.term.Add(1)
}

// getTerm returns the current [Raft.term] of this Node
func (r *Raft) getTerm() uint64 {
	return r.term.Load()
}

// updateTerm updates the current raftTerm and who the new [Leader] of the
// for this term is
func (r *Raft) updateTerm(term uint64, votedFor string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.term.Store(term)
	r.votedFor = votedFor
}

// updateState updates the [Raft.state] to the state provided
func (r *Raft) updateState(to RaftState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = to

}

func (r *Raft) getState() RaftState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *Raft) resetElectionTimeout(dur time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.electionTimeout = dur
}

func (r *Raft) getCurrentLeader() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.votedFor
}

func (r *Raft) clearLeader() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.votedFor = ""
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
