package main

import (
	"context"
	"fmt"
	"log"
	"net/rpc"
	"os"
)

type RPCKind int

type RPC struct {
	kind    RPCKind
	payload any
	reply   chan RPCReply
}

type RPCReply struct {
	kind    RPCKind
	payload any
}

type Node struct {
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
	// If no peers are empty, the Node will refuse to start
	peers []string

	// connectedPeers are connections that have been made when the [Node] was either
	// a [Leader] or candidate
	connectedPeers []*rpc.Client

	// stateCtx cancels the active [Raft.State] listening when an the [Node] needs to
	// shutdown. To cancel, call [Raft.stateCtxCancel]. After every cancel, a new ctx
	// needs to be created for the state to be ran
	stateCtx context.Context

	// stateCtxCancel cancels [Raft.stateCtx]
	stateCtxCancel context.CancelFunc

	log *log.Logger
}

const (
	// defaultChanBuffer is used for the Node's transition chan
	defaultChanBuffer = 1
)

func NewNode(id string, address string, peers []string) (*Node, error) {
	raft := NewRaft(id)
	incoming := make(chan RPC, defaultChanBuffer)
	server := NewServer(id, address, incoming)

	prefix := fmt.Sprintf("(%s:node) ", id)
	logger := log.New(os.Stdout, prefix, log.Ldate|log.Lmicroseconds|log.Lmsgprefix)

	return &Node{
		id:       id,
		address:  address,
		raft:     raft,
		incoming: incoming,
		server:   server,
		peers:    peers,
		log:      logger,
	}, nil
}

func (n *Node) Run(parentCtx context.Context) error {
	n.log.Println("initialising...")
	errCh := make(chan error)

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	go func() {
		if err := n.server.Listen(ctx, "tcp", n.address); err != nil {
			n.log.Println("server could not start, sent an error message")
			errCh <- err
		}
	}()

	for {
		select {
		case <-parentCtx.Done():
			return nil
		case err := <-errCh:
			n.log.Println(err)
			return nil
		}
	}
}
