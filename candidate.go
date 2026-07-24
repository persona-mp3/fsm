package main

import (
	"errors"
	"fmt"
	rlog "fsm/raftlogger"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

func (n *Node) runCandidate(logger rlog.RLogger) {
	n.raft.IncrementTerm()
	logger.UpdateTerm(n.raft.Term())
	n.raft.ClearLeader()
	newTimeout := n.raft.ResetElectionTimeout()

	logger.Println("candidate state succesfully initiated", n.Diagnostics())
	logger.Println("running for election")

	electionTimer := time.NewTimer(newTimeout)

	defer func() {
		electionTimer.Stop()

		logger.Println("candidate mode terminated succesfully")
	}()

	connectedPeers := n.getRPCPeers()

	if len(connectedPeers) == 0 {
		logger.Println("YOO, WE DONT HAVE RECONNECTED PEERS")
		successfulDials, failedCount := dialPeers("tcp", n.peers, logger.Inherit("dialPeers"))
		if failedCount == len(n.peers) || len(successfulDials) == 0 {
			// TODO: Worth adding Shutdown state because of these kind of variants, instead of hard panics
			logger.Println(
				`no dials were succesfull, transitioning back to Follower:TODO: ADD Shutdown state, successDails, failedCount, peers`,
				successfulDials, failedCount, n.peers,
			)

			n.transition <- Follower
			return
		}

		n.addRPCPeer(successfulDials...)

		connectedPeers = successfulDials
	}

	voteCount := atomic.Int64{}
	// raft paper mentions a node can vote itself first for an election
	voteCount.Add(1)

	wg := sync.WaitGroup{}
	for _, peer := range connectedPeers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.collectVote(peer, &voteCount, logger.Inherit("collectVote"))
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	for {
		select {
		case <-n.stateCtx.Done():
			return
		case <-electionTimer.C:
			logger.Println("election timer fired dropping back to Follower")
			n.transition <- Follower
			return
		case req := <-n.incoming:
			switch req.kind {
			case AppendEntry:
				request, ok := req.payload.(AppendEntryRequest)
				// no point in relaying respose backup to the server because the server will still
				// invalidate it and panic
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				action := n.handleAppendEntry(request, req.reply, logger.Inherit("handleAE"))
				if !action.action {
					continue
				}

				n.raft.UpdateTerm(action.newTerm, action.newLeader)
				logger.Println("succesfully updated term, dropping down to Follower", n.Diagnostics())
				n.closeConnections()
				logger.Println("closed connections")
				n.transition <- Follower
				return

			case Vote:
				request, ok := req.payload.(VoteRequest)
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				// Once in Candidate state, VoteRPCs are automatically rejected as this node has used
				// it's vote for itself.
				req.reply <- RPCReply{
					kind: Vote,
					payload: &VoteReply{
						Id:       n.id,
						Term:     n.raft.Term(),
						VotedFor: false,
						Message:  "I am candidate, i cannot give my vote",
					},
				}
				// action := n.handleVoteRequest(request, req.reply, logger.Inherit("handleVoteRequest"))
				// if !action.action {
				// 	continue
				// }
				//
				// n.raft.UpdateTerm(action.newTerm, action.newLeader)
				// logger.Println("succesfully updated term, timeout reset", n.Diagnostics())
				// n.closeConnections()
				// logger.Println("closed connections")
				// n.transition <- Follower
				// return

			case ClientCommand:
				logger.Println("in candidate_state, need to forward request to leader")
				req.reply <- RPCReply{
					kind: ClientCommand,
					payload: &CommandReply{
						From:   n.id,
						Result: "CANDIDATE_STUB: Read spec impl on how to handle requests mid election",
					},
				}

			default:
				log.Panic("Unhandled RPC Not yet implemented:", req.payload, n.Diagnostics())
			}

		case <-done:
			totalVotes := voteCount.Load()
			logger.Println("all vote routines have finshed, totalVotes:", totalVotes)
			if totalVotes > int64((len(connectedPeers)/2)+1) {
				logger.Println("recvd majority, becoming Leader with total votes of", totalVotes)
				n.transition <- Leader
				return
			}

			logger.Println("lost election, going back to Follower. Total votes received:", totalVotes)
			n.closeConnections()
			logger.Println("closed connections")
			n.transition <- Follower
			return

		}
	}
}

func dialPeers(network string, peers []string, logger rlog.RLogger) ([]*Peer, int) {
	clients := []*Peer{}
	failed := 0
	for id, addr := range peers {
		dial, err := rpc.Dial(network, addr)
		if err != nil {
			if errors.Is(err, rpc.ErrShutdown) {
				logger.Println(fmt.Sprintf("connection: %s has been shutdown", addr))
				failed++
				continue
			} else {
				logger.Println("could not dial peer:", err)
				failed++
				continue
			}
		}

		p := &Peer{id: id, addr: addr, rpcConn: dial}

		clients = append(clients, p)
	}
	return clients, failed
}

func (n *Node) collectVote(peer *Peer, voteCount *atomic.Int64, logger rlog.RLogger) {
	req := VoteRequest{
		Id:      n.id,
		Term:    n.raft.Term(),
		Message: "Give me your vote",
	}

	reply := &VoteReply{}
	if err := peer.rpcConn.Call("Server.VoteRequestRPC", req, reply); err != nil {
		logger.Println("could not dial rpc client:", peer.addr, err)
		return
	}

	if reply.VotedFor {
		logger.Println("recvd vote from: ", reply.Id, reply.VotedFor, reply.Message)
		voteCount.Add(1)
	} else {
		logger.Println("did not recv vote from: ", reply.Id, reply.VotedFor, reply.Message)
	}
}

func (n *Node) closeConnections() {
	peers := n.getRPCPeers()
	for _, p := range peers {
		if err := p.Close(); err != nil {
			n.log.Println(err.Error())
		}
	}

	n.addRPCPeer([]*Peer{}...)
}
