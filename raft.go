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

type RPCKind int

const (
	AppendEntry RPCKind = iota
)

type RPC struct {
	kind    RPCKind
	payload any
	reply   chan RPCReply
}

type RPCReply struct {
	kind    RPCKind
	payload any
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
	recentChange    atomic.Bool
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
		recentChange:    atomic.Bool{},
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
		ctx, cancel := context.WithCancel(parentCtx)
		select {
		case <-parentCtx.Done():
			cancel()
			return
		case reqRPC := <-r.incoming:
			log.Printf("(node) recvd incoming reqRPC: %+v\n", reqRPC)
			switch reqRPC.kind {
			case AppendEntry:
				go r.handleAppendEntry(&reqRPC)
			}

		case err := <-errCh:
			log.Println(err)
			cancel()
			return
		default:
		}

		currentState := r.getCurrentState()
		r.printDiagnostics()
		if r.recentChange.Load() {
			r.recentChange.Store(false)
			switch currentState {
			case Leader:
				cancel)_
				log.Println("(node) leader mode not yet implementd, stub")
				r.runLeader(ctx)
			case Follower:
				r.runFollower(ctx)
			case Candidate:
				r.runCandidate(ctx)
			}
		} else {
			r.runFollower(ctx)
		}
	}

}

// handleAppendEntry recevies RPC requests that are of the [AppendEntryReq] kind
// It returns the response to the server via the [RPCReply.reply] channel, and
// if the node is currently a Follower, it sends a heartbeat.
//
// Although not yet implemented, the node can disregard the AppendEntry by returning a
// false reply in [AppendEntryRes.Acknowledged] field when it's currentTerm is higher
// than that of the RPC request. In this case the heartbeat will not be sent. It also
// updates the [Raft.term] and [Raft.state] of this node if the request's RPC is higher.
func (r *Raft) handleAppendEntry(req *RPC) {
	log.Println("(node) handling appendEntryRPC")
	go func() {
		timer := time.NewTimer(300 * time.Millisecond)
		select {
		case req.reply <- RPCReply{
			kind: AppendEntry,
			payload: &AppendEntryRes{
				Id:           "this_node",
				Term:         r.term.Load(),
				Data:         "testing AppendEntry RPC",
				Acknowledged: true,
				err:          nil,
			},
		}:
		case <-timer.C:
			if !timer.Stop() {
				<-timer.C
			}
			log.Println("(sub_routine) could not send appendRPCRes within 300ms, slow server")
			return

		}
	}()

	if r.getCurrentState() == Follower {
		log.Println("(node) handled appendEntryRPC, sending heartbeat signal")
		r.heartbeat <- struct{}{}
	} else if r.getCurrentState() == Leader {
		log.Println("(node) dropping node from Leader to Follower")
		r.updateRaftState(Follower)
	} else if r.getCurrentState() == Candidate {
		log.Println("(node) dropping node from Candidate to Follower")
		r.updateRaftState(Follower)
	}
	log.Println("(node) handled appendEntryRPC")
}

func (r *Raft) handleRequestVote(req *RPC) {}

func generateRandomTimeout(d time.Duration) time.Duration {
	minInterval, maxInterval := 100, 500
	n := rand.IntN(maxInterval-minInterval) + minInterval

	return d * time.Duration(n)
}

func (r *Raft) printDiagnostics() {
	diagnostics := fmt.Sprintf("\nraft_diagnostics: { address: %s, state: %s, term: %d, electionTimeout: %s }\n\n",
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

func (r *Raft) Start(parentCtx context.Context) {
	errCh := make(chan error)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	go func() {
		defer close(errCh)
		if err := r.server.Listen(ctx, r.address); err != nil {
			errCh <- err
			return
		}
	}()

	log.Println("(raft) running raft machine")
	for {
		select {
		case <-parentCtx.Done():
			log.Println("(raft) parentCtx cancelled")
			return
		case err := <-errCh:
			log.Println("(raft) error from server: ", err)
			return
		default:
		}

		raftState := r.getCurrentState()
		switch raftState {
		case Leader:
			r.runLeader(ctx)
		case Follower:
			r.runFollower(ctx)
		case Candidate:
			r.runCandidate(ctx)
		}
	}
}
