package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// According to the Raft Paper, it's recommended for timeouts(election) to range from 100-500ms
	minInterval = 100
	maxInterval = 500
)

type RaftState int

const (
	Leader RaftState = iota
	Follower
	Candidate
)

const (
	heartbeatInterval = time.Millisecond * 50
)

// RPCKind tells what kind of payload we received and helps to determine what
// kind of reply to send back
type RPCKind int

const (
	AppendEntry RPCKind = iota
	// For some reason an unexpected RPCRequest got through
	Confused
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
	id    string
	mu    sync.RWMutex
	state RaftState

	serverAddr string
	server     *Server

	// IPAddr of other nodes in a cluster
	peers []string

	// currentTerm this node is perceived to be in
	term atomic.Uint64

	// max amount of time before a node in the [Follower] state can go
	// before transitioning into a [Candidate]
	electionTimeout time.Duration

	// rpcRequests are forwarded from the server for the node to process
	incoming chan RPC

	// used by RaftStates to communicate what [RaftState] node should go into
	// after their exit or preconditions are met
	transition chan RaftState

	// each raft state __could__ be cancelled via this context. But isn't strictly
	// necessary as of the moment. It primarily serves as a way for signal handling or
	// ensuring no resource leaks
	stateCtx       context.Context
	stateCtxCancel context.CancelFunc

	log *log.Logger

	// holds all the connections to the nodes in the cluster as provided by [Raft.peer]
	rpcPeers []*rpc.Client
}

const (
	bufferChanSize = 1
)

func NewRaft(
	id string,
	addr string,
	peers []string,
	electionTimeout time.Duration,
	output io.Writer,
) *Raft {
	incoming := make(chan RPC, bufferChanSize)
	// we only want one state transition to happen at a time
	transition := make(chan RaftState)
	server := NewServer(incoming, nil)

	var l *log.Logger
	if output == nil {
		l = log.New(os.Stdout, "(raft) ", log.Default().Flags()|log.Lmicroseconds)
	} else {
		l = log.New(output, "(raft) ", log.Default().Flags()|log.Lmicroseconds)
	}

	// rpcPeers, failed := dialPeers("tcp", peers)
	// if failed > 0 {
	// 	l.Printf("[STARTUP] WARN: could not dial %d nodes\n", failed)
	// } else if failed == len(peers) {
	// 	return nil, fmt.Errorf("Could not dial any peer, cannot continue Raft. Failed Dials: %d", failed)
	// }

	return &Raft{
		id:              id,
		mu:              sync.RWMutex{},
		state:           Follower,
		serverAddr:      addr,
		server:          server,
		peers:           peers,
		term:            atomic.Uint64{},
		electionTimeout: electionTimeout,
		incoming:        incoming,
		transition:      transition,
		rpcPeers:        []*rpc.Client{},
		log:             l,
	}
}

func (r *Raft) Run(parentCtx context.Context) error {
	errCh := make(chan error)
	// rpcPeers, failed := dialPeers("tcp", r.peers)
	// if failed > 0 {
	// 	r.log.Printf("[STARTUP] WARN: could not dial %d nodes\n", failed)
	// } else if failed == len(r.peers) {
	// 	return fmt.Errorf("Could not dial any peer, because no peers were present cannot continue Raft. Failed Dials: %d, peers: %+v", failed, r.peers)
	// }

	// r.rpcPeers = rpcPeers
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	go func() {
		if err := r.server.Listen(ctx, r.serverAddr); err != nil {
			r.log.Println("error occured while starting the sever")
			errCh <- err
		}
	}()

	stateCtx, stateCtxCancel := context.WithCancel(parentCtx)
	r.stateCtx = stateCtx
	r.stateCtxCancel = stateCtxCancel
	defer stateCtxCancel()

	r.log.Println("starting raft node: ", r.Diagnostics())

	go r.runFollower(nil)

	for {
		select {
		case <-parentCtx.Done():
			r.log.Println("exiting full raftState")
			return nil
		case err := <-errCh:
			r.log.Println("error from server: ", err)
			return err

		case raftState := <-r.transition:
			r.stateCtxCancel()
			r.newStateContext(parentCtx)

			switch raftState {
			case Leader:
				r.log.Println("received transition request to Leader from: ", r.getCurrentState())
				if r.getCurrentState() == Leader {
					r.log.Panicf("currently a Leader and recvd Leader transition currState: %s to: %s\n",
						raftState.String(), r.getCurrentState().String())
				}
				r.electionTimeout = randomTimeout(time.Millisecond)
				r.updateRaftState(raftState)
				r.log.Println("updated state to Leader: ", r.Diagnostics())

				// go r.startLeader()
				go r.runLeader(nil)
			case Follower:
				r.log.Println("received transition request to Follower from: ", r.getCurrentState())
				if r.getCurrentState() == Follower {
					r.log.Panicf("currently a Follower and recvd Follower transition currState: %s to: %s\n",
						raftState.String(), r.getCurrentState().String())
				}

				r.electionTimeout = randomTimeout(time.Millisecond)
				r.updateRaftState(raftState)
				go r.runFollower(nil)

			case Candidate:
				r.log.Println("received transition request to Candidate from: ", r.getCurrentState().String())
				if r.getCurrentState() == Candidate {
					r.log.Panicf("currently a Candidate and recvd Candidate transition currState: %s to: %s\n",
						raftState.String(), r.getCurrentState().String())
				}
				r.updateRaftState(raftState)
				// go r.runCandidate(nil)
				go r.Candidate(nil)
			}
		}
	}
}

// Updates the current state of this node
func (r *Raft) updateRaftState(state RaftState) {
	r.mu.Lock()
	defer r.mu.Unlock()
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

// randomTimeout generates a number between the [minInterval] and [maxInterval]
// and returns the duration [d]
func randomTimeout(d time.Duration) time.Duration {
	n := rand.IntN(maxInterval-minInterval) + minInterval

	return d * time.Duration(n)
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
		return fmt.Sprintf("unexpected main.RaftState: %#v", rs)
	}
}

func dialRaftPeers(network string, addrs []string) (rpcPeers []*rpc.Client, failedDials int) {
	peers := []*rpc.Client{}
	failed := 0

	for _, addr := range addrs {
		client, err := rpc.Dial(network, addr)
		if err != nil {
			fmt.Printf("[DIALER] failed to dial: %s. Reason: %s\n", addr, err)
			failed += 1
			continue
		}

		peers = append(peers, client)
	}
	return peers, failed
}

// TODO: Not sure if an unknown RPC could ever happend because 
// validation is done at the protocol level
func (r *Raft) handleUnknownRPCKind(rpcReq RPC, opts *Opts) {
	var o *Opts
	if opts == nil {
		o = defaultOpts()
	} else {
		o = opts
	}

	o.log.SetPrefix(fmt.Sprintf("(%s:unknownRPCHandler) ", r.id))
	o.log.Printf("handling unknownRequestRPC: %+v\n", rpcReq.payload)

	rpcReq.reply <- RPCReply{
		kind: Confused,
		payload: &AppendEntryRes{
			Id:           r.id,
			Term:         r.term.Load(),
			Data:         "I don't understand this protocol you speak of",
			Acknowledged: false,
		},
	}
	o.log.Println("sent AppendEntryRes because node is confused")
}
