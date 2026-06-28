package main

import (
	"context"
	"fmt"
	rlog "fsm/raftlogger"
	"math/rand/v2"
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
	minInterval = 400
	maxInterval = 1500
)

func (n *Node) runLeader(logger rlog.RLogger) {
	logger.Println("leader state transitioned successfully", n.Diagnostics())

	ctx, cancel := context.WithCancel(n.stateCtx)
	defer cancel()

	wg := sync.WaitGroup{}
	rpcPeers := n.getRPCPeers()

	if len(rpcPeers) == 0 {
		// we might want a resting state or Shutdown because it could possbily
		// mean this node is the only one active in the cluster and others have died
		logger.Println("todo::warning:: could not find any connected peer")
		n.transition <- Follower
		return

	}

	currentPeers := n.getRPCPeers()
	for idx, rpcPeer := range currentPeers {
		if rpcPeer != nil {
			wg.Add(1)

			childLogger := n.log.Inherit(fmt.Sprintf("%d-sendHB", idx))

			go func(ctx context.Context, rpcPeer *Peer, logger rlog.RLogger) {
				defer wg.Done()
				n.sendHeartBeat(ctx, rpcPeer, heartbeatInterval, logger)
			}(ctx, rpcPeer, childLogger)
		}
	}

	// wait for all children to return
	done := make(chan struct{})
	go func() {
		wg.Wait()
		done <- struct{}{}
		logger.Println("DEBUG:: leader loop recvdvd")
	}()

	for {
		select {
		case <-n.stateCtx.Done():
			return

		case <-done:
			logger.Println("all child sendhearbeats have returned")
			n.transition <- Follower
			return

		case req := <-n.incoming:
			switch req.kind {
			case AppendEntry:
				request, ok := req.payload.(AppendEntryRequest)
				// no point in relaying respose backup to the server because the server will still invalidate it and panic
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				action := n.handleAppendEntry(request, req.reply, logger.Inherit("handleAE"))
				if !action.action {
					continue
				}

				n.raft.updateTerm(action.newTerm, action.newLeader)
				logger.Println("leader dropping down to follower succesfully updated term, timeout reset", n.Diagnostics())
				n.transition <- Follower
				return

			default:
				logger.Panic("Unhandled RPC Not yet implemented:", req.payload, n.Diagnostics())
			}
		}
	}
}

// TODO: Make a way for them to tell the leader that it's gotten demoted, ie a clietn returns
// a higher term maybe through a channel
func (n *Node) sendHeartBeat(ctx context.Context, peer *Peer, interval time.Duration, logger rlog.RLogger) {
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		logger.Println("returning back to parent")
	}()

	req := AppendEntryRequest{
		Id:      n.id,
		Term:    n.raft.getTerm(),
		Message: "This is a heartbeat message",
	}

	reply := AppendEntryReply{}

	for {
		select {
		case <-ticker.C:
			logger.Println("sending heartbeatRPC")
			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				logger.Println("Failed to send heartbeat", err, peer.id, peer.addr)
				return
			}
			logger.Println("reply: ", reply, peer.addr)
		case <-ctx.Done():
			return
		}
	}
}

func randomTimeout(d time.Duration) time.Duration {
	n := rand.IntN(maxInterval-minInterval) + minInterval

	return d * time.Duration(n)
}
