package main

import (
	"context"
	"fmt"
	db "fsm/database"
	rlog "fsm/raftlogger"
	"io"
	"net/rpc"
	"os"
	"slices"
	"sync"
	"time"
)

const (
	// heartbeatInterval is the rate at which the node when in a [Leader] state sends
	// out heartbeats to follower in a cluster. At the moment, this is set to be 200 which
	// is roughly half the minimum election timeout interval
	heartbeatInterval = time.Millisecond * 200

	// According to the Raft Paper, it's recommended for timeouts(election) to range from 100-500ms, but
	// we're increasing it because that's too aggressive
	minInterval = 500
	maxInterval = 1200
)

// Peer has the underlying rpc connection to a raft peer alongside
// a dedicated channel for sending new logEntries
type Peer struct {
	id      int
	addr    string
	rpcConn *rpc.Client
	dropCh  chan Entry
}

type Node struct {
	mu sync.Mutex

	id string
	// address is where the Node's server will listen for incoming RPC's
	address string

	// raft holds Raft state
	raft *Raft

	// incoming relays RPC's from the  server to the active [RaftState] of this Node.
	// Reponses to the server are relayed back through the [RPC.reply] chan.
	// The server is always gauranteed to wait for this response
	incoming chan RPC

	// transition changes the [RaftState] of this Node to what value was sent in. This
	// channel will remain unbuffered to gaurantee that only one state transition can
	// happen at a time. The current running state sends requests through here after
	// then do they exit
	transition chan RaftState

	// server recvs incoming RPC's from the newtork and relays them to [Node.incoming]
	// for the Node to process
	server *Server

	// peers contains the ip addresses of other nodes in the cluster, excluding this Node
	peers []string

	// connectedPeers are connections that have been made when the [Node] was either
	// a [Leader] or [Candidate]. This shoudl be accessed safely
	rpcPeers []*Peer

	// stateCtx cancels the active [Raft.State] listening when an the [Node] needs to
	// shutdown. To cancel, call [Raft.stateCtxCancel]. After every cancel, a new ctx
	// needs to be created for the state to be ran
	stateCtx context.Context

	// stateCtxCancel cancels [Raft.stateCtx]
	stateCtxCancel context.CancelFunc

	database db.Database

	log rlog.RLogger

	// logs are all the logs that this node has had through out this term
	// and previous terms. It receives these logs from clients when a leader
	// or from the leader for the currentTerm via the AppendEntryRPCs
	logs Logs
}

const (
	// defaultChanBuffer is used for the Node's incoming chan
	defaultChanBuffer = 1
)

func NewNode(id string, address string, peers []string, out io.Writer) (*Node, error) {
	raft := NewRaft(id)
	incoming := make(chan RPC, defaultChanBuffer)

	// purposely left unbuffered to enforce one state transition at a time
	transition := make(chan RaftState)

	if out == nil {
		out = os.Stdout
	}
	logger := rlog.NewHumaneLogger(id, "node", 0, out)
	sl := rlog.NewHumaneLogger(id, "server", 0, out)
	server := NewServer(id, address, incoming, sl)

	jkvsDatabase := db.NewJKVSDatabase("tcp", "localhost:9090")

	return &Node{
		mu:         sync.Mutex{},
		id:         id,
		address:    address,
		raft:       raft,
		incoming:   incoming,
		transition: transition,
		server:     server,
		peers:      peers,
		rpcPeers:   []*Peer{},
		database:   jkvsDatabase,
		log:        logger,
	}, nil
}

func (n *Node) Run(parentCtx context.Context) error {
	n.log.Println("initialising node")
	n.log.Println("connecting to database...")

	if err := n.database.Connect(); err != nil {
		return err
	}

	n.log.Println("successfully connected to database")
	n.database.Commit(db.Command{Operation: db.GetOps, Key: "raft-node-id", Value: "Testing raft-db connection"})
	errCh := make(chan error)

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	go func() {
		if err := n.server.Listen(ctx, "tcp", n.address); err != nil {
			n.log.Println("server could not start, sent an error message")
			errCh <- err
		}
	}()

	stateCtx, stateCancel := context.WithCancel(ctx)
	n.stateCtx = stateCtx
	n.stateCtxCancel = stateCancel

	defer stateCancel()

	go func() {
		n.runFollower()
	}()

	for {
		select {
		case <-parentCtx.Done():
			return nil
		case err := <-errCh:
			n.log.Println(err.Error())
			return nil

		case raftState := <-n.transition:
			switch raftState {
			case Follower:
				if n.raft.getState() == Follower {
					n.log.Panic(`recvd transition into Follower while in Follower state`, n.Diagnostics())
				}

				n.log.Println("recvd transition to Follower")
				n.raft.updateState(raftState)
				// cancel context and make a new one
				n.stateCtxCancel()
				n.newContext(ctx)

				go n.runFollower()
			case Leader:
				if n.raft.getState() == Leader {
					n.log.Panic(`recvd transition into Leader while in Leader state`, n.Diagnostics())
				}

				n.log.Println("recvd transition to Leader")
				n.raft.updateState(raftState)
				// cancel context and make a new one
				n.stateCtxCancel()
				n.newContext(ctx)

				rlog := rlog.NewHumaneLogger(n.id, "leader", n.raft.getTerm(), n.log.Out())
				go n.runLeader(rlog)
			case Candidate:
				if n.raft.getState() == Candidate {
					n.log.Panic(`recvd transition into Candidate while in Candidate state`, n.Diagnostics())
				}

				n.log.Println("recvd transition to Candidate")
				n.raft.updateState(raftState)
				// cancel context and make a new one
				n.stateCtxCancel()
				n.newContext(ctx)

				clog := rlog.NewHumaneLogger(n.id, "candidate", n.raft.getTerm(), n.log.Out())
				go n.runCandidate(clog)
			default:
				n.log.Panic("%s state not yet implemented!\n", raftState)
			}
		}
	}
}

// Action represents what operation or next step action the current state, should take.
type Action struct {
	// represents if the state should ignore the request
	action    bool
	newTerm   uint64
	newLeader string
}

// handleAppendEntryRequest handles the incoming RPC request
//   - If the node is a [Follower] and handleRPCRequest returns and true,
//     the [Follower] updates it's term with the number returned
//
// a term number higher, ,
// it updates it's term with  the number returned, and the returned string with
// votedFor with the
func (n *Node) handleAppendEntry(req AppendEntryRequest, replyCh chan RPCReply, logger rlog.RLogger) Action {
	currentTerm := n.raft.getTerm()
	currentLeader := n.raft.getCurrentLeader()

	action := Action{}
	if req.Term < currentTerm {
		action.action = false
		replyCh <- RPCReply{
			kind: AppendEntry,
			payload: &AppendEntryReply{
				Id:      n.id,
				Acked:   false,
				Term:    currentTerm,
				Message: "you have an outdated term",
			},
		}
		logger.Println("appendEntry was from a lower term:", req, n.Diagnostics())
		return action
	}

	// assume node missed an election and old leader died
	if req.Term > currentTerm {
		replyCh <- RPCReply{
			kind: AppendEntry,
			payload: &AppendEntryReply{
				Id:      n.id,
				Acked:   true,
				Term:    req.Term,
				Message: "yielding to you",
			},
		}
		action.action = true
		action.newLeader = req.Id
		action.newTerm = req.Term
		logger.Println("appendEntry was from a a higher term:", req, n.Diagnostics())
		return action
	}

	// assume rpc is legitimate leader
	if req.Term == currentTerm && req.Id == currentLeader {
		replyCh <- RPCReply{
			kind: AppendEntry,
			payload: &AppendEntryReply{
				Id:      n.id,
				Acked:   true,
				Term:    currentTerm,
				Message: "accepting appendEntry recognised as leader",
			},
		}
		action.action = true
		action.newLeader = req.Id
		action.newTerm = req.Term
		logger.Println("appendEntry was from a valid leader term:", req, n.Diagnostics())
		return action
	}

	//  the request has same term but we haven't voted from them
	replyCh <- RPCReply{
		kind: AppendEntry,
		payload: &AppendEntryReply{
			Id:      n.id,
			Term:    currentTerm,
			Acked:   false,
			Message: "not accepting appendEntry",
		},
	}
	action.action = false
	logger.Println("appendEntry invalidated: ", req, n.Diagnostics())
	return action
}

// newContext creates a new context and cancel func and attaches it to the Node for
// states to actively running states to be canceled
func (n *Node) newContext(parent context.Context) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.stateCtx.Err() == nil {
		n.stateCtxCancel()
		panic("stateCtx not cancelled yet")
	} else {
		ctx, cancel := context.WithCancel(parent)
		n.stateCtx = ctx
		n.stateCtxCancel = cancel
	}
}

func (n *Node) getRPCPeers() []*Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rpcPeers
}

func (n *Node) addRPCPeer(peers ...*Peer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range peers {
		if p != nil && !slices.Contains(n.rpcPeers, p) {
			n.rpcPeers = append(n.rpcPeers, p)
		}
	}
}

func (n *Node) handleVoteRequest(req VoteRequest, replyCh chan RPCReply, logger rlog.RLogger) Action {
	currentTerm := n.raft.getTerm()
	currentLeader := n.raft.getCurrentLeader()

	action := Action{}
	if req.Term > currentTerm {
		replyCh <- RPCReply{
			kind: Vote,
			payload: &VoteReply{
				Id:       n.id,
				VotedFor: true,
				Term:     req.Term,
				Message:  "yielding my vote to you",
			},
		}
		action.action = true
		action.newLeader = req.Id
		action.newTerm = req.Term
		logger.Println("requestRPC was from a a higher term:", req, n.Diagnostics())
		return action
	} else if req.Term < currentTerm {
		action.action = false
		replyCh <- RPCReply{
			kind: Vote,
			payload: &VoteReply{
				Id:       n.id,
				VotedFor: false,
				Term:     currentTerm,
				Message:  "you have an outdated term",
			},
		}
		logger.Println("requestVote was from a lower term:", req, n.Diagnostics())
		return action
	} else if req.Term == currentTerm && currentLeader == "" {
		// possibly in a candidate state and someone requested our vote
		replyCh <- RPCReply{
			kind: Vote,
			payload: &VoteReply{
				Id:       n.id,
				VotedFor: true,
				Term:     req.Term,
				Message:  "yielding my vote to you",
			},
		}
		action.action = true
		action.newLeader = req.Id
		action.newTerm = req.Term
		logger.Println("voteRPC had same term, but reached first and I have no leader:", req, n.Diagnostics())
		return action
	}
	return action
}

// Diagnostics returns all revelevant information for this Node, including who it's
// votedFor, current term, and what state it's in
func (n *Node) Diagnostics() string {
	term := n.raft.getTerm()
	state := n.raft.getState().String()
	votedFor := n.raft.getCurrentLeader()

	diagnostics := fmt.Sprintf("diagnostics: { term: %d, state: %s, votedFor|leader: %s, logs: %+v }",
		term, state, votedFor, n.logs.String())
	return diagnostics
}
