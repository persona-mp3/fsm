package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

type RaftState int

const (
	Leader RaftState = iota
	Follower
	Candidate
)

type RPC struct {
	payload string
	reply   chan string
}

type Raft struct {
	mu              sync.RWMutex
	state           RaftState
	server          *Server
	term            *atomic.Uint64
	electionTimeout time.Duration
	incoming        chan RPC
	outgoing        chan any
	address         string
	peers           []string
	heartbeat       chan struct{}
}

func NewRaft(address string, peers []string, electionTimeout time.Duration) *Raft {
	incoming := make(chan RPC)
	outgoing := make(chan any)
	heartbeat := make(chan struct{})

	server := NewServer(incoming, outgoing)
	return &Raft{
		mu:              sync.RWMutex{},
		state:           Follower,
		server:          server,
		address:         address,
		peers:           peers,
		term:            &atomic.Uint64{},
		electionTimeout: electionTimeout,
		outgoing:        outgoing,
		heartbeat:       heartbeat,
		incoming:        incoming,
	}
}

func (r *Raft) Run(parentCtx context.Context) {
	errCh := make(chan error)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	go func() {
		defer close(errCh)
		if err := r.server.Listen(ctx, r.address); err != nil {
			errCh <- err
		}
	}()

	log.Println("raft started")
	for {
		select {
		case <-parentCtx.Done():
			return
		case reqRPC := <-r.incoming:
			fmt.Printf("node recvd rpc: %+v\n", reqRPC.payload)
			resp := "this is control tower, who is you?"
			reqRPC.reply <- resp

		case err := <-errCh:
			log.Println(err)
			return
		default:
		}

		currentState := r.getCurrentState()
		r.printDiagnostics()
		switch currentState {
		case Leader:
			r.runLeader(ctx)
		case Follower:
			r.runFollower(ctx)
		case Candidate:
			r.runCandidate(ctx)
		}
	}

}

func generateRandomTimeout(d time.Duration) time.Duration {
	minInterval, maxInterval := 100, 500
	n := rand.IntN(maxInterval-minInterval) + minInterval

	return d * time.Duration(n)
}

func (r *Raft) printDiagnostics() {
	diagnostics := fmt.Sprintf("\n\nraft_diagnostics: { address: %s, state: %s, term: %d, electionTimeout: %s }\n\n",
		r.address, r.state.String(), r.term.Load(), r.electionTimeout)
	log.Println(diagnostics)
}

func (s RaftState) String() string {
	switch s {
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("unexpected main.RaftState: %#v", s)
	}
}
