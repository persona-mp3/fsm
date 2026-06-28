package main

import (
	rlog "fsm/raftlogger"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

func (n *Node) runCandidate(logger rlog.RLogger) {
	n.raft.incrementTerm()
	logger.UpdateTerm(n.raft.getTerm())
	n.raft.clearLeader()
	newTimeout := n.raft.resetElectionTimeout()

	logger.Println("candidate state succesfully initiated", n.Diagnostics())
	logger.Println("running for election")

	electionTimer := time.NewTimer(newTimeout)

	defer func() {
		electionTimer.Stop()
		logger.Println("candidate mode terminated succesfully")
	}()

	connectedPeers := n.getRPCPeers()

	if len(connectedPeers) == 0 {
		// TODO: Might be worth considering dialing peers in-seperate goroutines since we don't
		// want to block this mean thread because of a slow client or slow dial
		// successfulDials, failedCount := dialPeers("tcp", n.peers, logger.Inherit("dialPeers"))
		successfulDials, failedCount := dialPeers("tcp", n.peers, logger.Inherit("dialPeers"))
		if failedCount == len(n.peers) {
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
	// raft paper metiones a node can vote itself first for an election
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
		done <- struct{}{}
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

				n.raft.updateTerm(action.newTerm, action.newLeader)
				logger.Println("succesfully updated term, dropping down to Follower", n.Diagnostics())
				n.transition <- Follower
				return

			case Vote:
				request, ok := req.payload.(VoteRequest)
				// no point in relaying respose backup to the server because the server will still
				// invalidate it and panic
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				action := n.handleVoteRequest(request, req.reply, logger.Inherit("handleVoteRequest"))
				if !action.action {
					continue
				}

				n.raft.updateTerm(action.newTerm, action.newLeader)
				logger.Println("succesfully updated term, timeout reset", n.Diagnostics())

			default:
				logger.Panic("Unhandled RPC Not yet implemented:", req.payload, n.Diagnostics())
			}

		case <-done:
			totalVotes := voteCount.Load()
			logger.Println("all vote routines have finshed, totalVotes:", totalVotes)
			if int(totalVotes) > len(connectedPeers)/2 {
				logger.Println("recvd majority, becoming Leader")
				n.transition <- Leader
				return
			}

			logger.Println("lost election, going back to Follower")
			n.transition <- Follower

		}
	}
}

func dialPeers(network string, peers []string, logger rlog.RLogger) ([]*Peer, int) {
	clients := []*Peer{}
	failed := 0
	for id, addr := range peers {
		dial, err := rpc.Dial(network, addr)
		if err != nil {
			logger.Println("could not dial: ", addr, err)
			failed++
		}

		p := &Peer{id: id, addr: addr, rpcConn: dial}

		clients = append(clients, p)
	}
	return clients, failed
}

func (n *Node) collectVote(peer *Peer, voteCount *atomic.Int64, logger rlog.RLogger) {
	req := VoteRequest{
		Id:      n.id,
		Term:    n.raft.getTerm(),
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
