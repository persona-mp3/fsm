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
	entries := make(chan Entry)
	for idx, rpcPeer := range currentPeers {
		if rpcPeer != nil {
			wg.Add(1)

			childLogger := n.log.Inherit(fmt.Sprintf("%d-sendHB", idx))

			go func(ctx context.Context, rpcPeer *Peer, e chan Entry, logger rlog.RLogger) {
				defer wg.Done()
				n.sendHeartBeat(ctx, rpcPeer, heartbeatInterval, e, logger)
			}(ctx, rpcPeer, entries, childLogger)
		}
	}

	// wait for all children to return
	done := make(chan struct{})
	go func() {
		wg.Wait()
		done <- struct{}{}
		logger.Println("leader workers exited succsessfully")
	}()

	for {
		select {
		case <-n.stateCtx.Done():
			return

		case <-done:
			logger.Println("all child sendheartbeats have returned")
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

			case ClientCommand:
				logger.Println("in leader state, received command request from a client", req.payload)
				req.reply <- RPCReply{
					kind: ClientCommand,
					payload: &CommandReply{
						From:   n.id,
						Result: "LEADER_STUB: Need to send requests to whole cluster to append this to their logs",
					},
				}
				logger.Println("begin sending out appendRPCs")
				payload, ok := req.payload.(CommandReq)
				if !ok {
					logger.Panic("Expected CommandReq, got", payload)
				}
				// TODO: Let's check our logs if we have this first
				entry := Entry{}
				entry.Term = n.raft.getTerm()
				entry.Operation = payload.Operation
				entry.Key = payload.Key
				entry.Value = payload.Value
				if !n.logs.HasEntry(&entry) {
					index := n.logs.Append(&entry)
					entry.Idx = index
          // TODO: Might need to keep this buffered? The thing here is that 
          // there are x-worker routines sending hearbeats to x connectedPeers
          // And all workers recv on this channel. Sending to this channel is a blocking operation
          // so there's no garauntee that each worker receives the newEntry, and one 
          // doesn't take all of them, or there are no listeners, or the worker is busy on another
          // select/case branch. Another way might involve having each worker return a seperate channel 
          // the recv on, and then just looping over those, but, i'm not yet sure if I'd want to go that 
          // route yet
					for i := range len(n.rpcPeers) {
						_ = i
						entries <- entry
					}
					logger.Println("sent new Entry to all peers")
				} else {
					logger.Println("entry already existed, not repeating duplicate logs", n.Diagnostics())
        }
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
				logger.Println("succesfully updated term, dropping back to follower", n.Diagnostics())
        n.transition <- Follower


			default:
				logger.Panic("Unhandled RPC Not yet implemented:", req.payload, n.Diagnostics())
			}
		}
	}
}

// contemplation: I'd want the main state routine to send jobs to these worker routines, but I'd also
// want these workers to be able to communicate back w the leader otherwise. For example, if a new client
// command comes in, I'd want the runLeader to send the command across the cluster for 'replication' via
// the appendRPC's channel. So then this worker can just send the data to the Follower it's attached to.
// At the moment, if a call fails, we simply return
// But what it the follower doesn't acknowledge the RPCS? We'd want this communication btwn the worker and
// leader then. Without having to wrap appendRPCs with a channel abstraction, I think it would be better
// to give the worker some independence accross the Node. So if the follower fails to ack the appendEntry, we
// can simply just read against this Node's logs instead of handing it over to the top-level runLeader
func (n *Node) sendHeartBeat(ctx context.Context, peer *Peer, interval time.Duration, entry chan Entry, logger rlog.RLogger) {
	// TODO: Might also be worth retrying connections with  dropped peers incase it was just
	// a network glitch
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
		case <-ctx.Done():
			return
		case e := <-entry:
			logger.Println("recvd entry::", e)
			req.Entry = &e
			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				logger.Println("Failed to send heartbeat", err, peer.id, peer.addr)
				return
			}
		case <-ticker.C:
			logger.Println("sending heartbeatRPC")
			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				logger.Println("Failed to send heartbeat", err, peer.id, peer.addr)
				return
			}
			logger.Println("reply: ", reply, peer.addr)
		}
	}
}

func randomTimeout(d time.Duration) time.Duration {
	n := rand.IntN(maxInterval-minInterval) + minInterval

	return d * time.Duration(n)
}
