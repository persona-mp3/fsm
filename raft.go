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
	// None means no transition should occur
	Remain
)

// Raft holds the RaftState and information about this node
type Raft struct {
	id string
	// the rwtex should be used when reading or updating values that
	// cannot be read atomically
	rw sync.RWMutex

	// state represents the current [RaftState] of this node
	state RaftState

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
		rw:              sync.RWMutex{},
		state:           Follower,
		termInfo:        &termInfo,
		electionTimeout: initialTimeout,
		log:             raftLogger,
	}
}

// IncrementTerm atomically updates the currentTerm of this Node by 1
// This is usually called when the Node transists into a [Candidate] state.
func (r *Raft) IncrementTerm() {
	r.rw.Lock()
	defer r.rw.Unlock()
	r.termInfo.term++
}

func (r *Raft) GiveVote(term uint64, votedFor string) {
	r.rw.Lock()
	defer r.rw.Unlock()

	r.termInfo.term = term
	r.termInfo.votedFor = votedFor
	r.termInfo.hasVoted = true
}

// UpdateTerm updates the current raftTerm and who the new [Leader] of the
// for this term is
func (r *Raft) UpdateTerm(term uint64, leader string) {
	r.rw.Lock()
	defer r.rw.Unlock()

	r.termInfo.term = term
	r.termInfo.leader = leader
}

// Term returns the current [Raft.term] of this Node
func (r *Raft) Term() uint64 {
	r.rw.RLock()
	defer r.rw.RUnlock()
	return r.termInfo.term
}

func (r *Raft) HasVoted() bool {
	r.rw.RLock()
	defer r.rw.RUnlock()
	return r.termInfo.hasVoted
}

// UpdateState updates the [Raft.state] to the state provided
func (r *Raft) UpdateState(to RaftState) {
	r.rw.Lock()
	defer r.rw.Unlock()
	r.state = to

}

func (r *Raft) State() RaftState {
	r.rw.RLock()
	defer r.rw.RUnlock()
	return r.state
}

func (r *Raft) ResetElectionTimeout() time.Duration {
	r.rw.Lock()
	defer r.rw.Unlock()

	dur := randomTimeout(time.Millisecond)
	r.electionTimeout = dur
	return dur
}

func (r *Raft) CurrentLeader() string {
	r.rw.RLock()
	defer r.rw.RUnlock()
	return r.termInfo.leader
}

func (r *Raft) ClearLeader() {
	r.rw.Lock()
	defer r.rw.Unlock()
	r.termInfo.leader = ""
}

func (r *Raft) VotedFor() string {
	r.rw.RLock()
	defer r.rw.RUnlock()
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
		panic(fmt.Sprintf("unexpected main.RaftState: %+v", rs))
	}
}
