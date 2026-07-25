package main

import (
	"context"
	"crypto/rand"
	"fmt"
	db "fsm/database"
	rlog "fsm/raftlogger"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

const dropChBuff = 10

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
				replicateCh := make(chan replicate, dropChBuff)
				rpcPeer.replicateCh = replicateCh
				n.sendHeartBeat(ctx, rpcPeer, heartbeatInterval, logger)
			}(ctx, rpcPeer, childLogger)
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
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				action := n.handleAppendEntry(request, req.reply, logger.Inherit("handleAE"))
				if !action.action {
					continue
				}

				n.raft.UpdateTerm(action.newTerm, action.newLeader)
				logger.Println("leader dropping down to follower succesfully updated term, timeout reset", n.Diagnostics())
				n.transition <- Follower
				return

			case ClientCommand:
				logger.Println("in leader state, received command request from a client", req.payload)

				payload, ok := req.payload.(CommandRequest)
				if !ok {
					logger.Panic("expected CommandReq as payload got:", payload)
				}

				logger.Println("begin sending out appendRPCs")
				entry := Entry{}
				entry.Term = n.raft.Term()
				entry.Operation = payload.Operation
				entry.Key = payload.Key
				entry.Value = payload.Value
				if !n.logs.HasEntry(&entry) {
					index := n.logs.Append(&entry)
					entry.Idx = index
					logger.Println("sent new Entry to all peers")
					go n.commitFunc(payload, entry, req.reply, logger.Inherit("commitFunc"))
				} else {
					logger.Println("entry already existed, not repeating duplicate logs", n.Diagnostics())
					req.reply <- RPCReply{
						kind: ClientCommand,
						payload: &CommandReply{
							From:   n.id,
							Result: "LEADER_STUB: I gave you this before, where did you keep it?",
						},
					}
				}
			case Vote:
				request, ok := req.payload.(VoteRequest)
				// no point in relaying response to the server because the server will still
				// invalidate it and pani
				if !ok {
					logger.Panic("received wrong rpcRequet payload. Expected AppendEntry:", request, n.Diagnostics())
				}

				if request.Term > n.raft.Term() {
					req.reply <- RPCReply{
						kind: Vote,
						payload: &VoteReply{
							Id:       n.id,
							Term:     request.Term,
							VotedFor: true,
							Message:  "Ye was a leader, now sunderring",
						},
					}
					n.raft.GiveVote(request.Term, request.Id)
					n.transition <- Follower
					return
				}

				// TODO(persona) will need to do a check here in the event that two nodes might
				// think they're a leader. We then compare against their logs
				req.reply <- RPCReply{
					kind: Vote,
					payload: &VoteReply{
						Id:       n.id,
						Term:     request.Term,
						VotedFor: false,
						Message:  "Coportate espionage is punishable just so you know",
					},
				}

			default:
				logger.Panic("Unhandled RPC Not yet implemented:", req.payload, n.Diagnostics())
			}
		}
	}
}

func (n *Node) sendHeartBeat(ctx context.Context, peer *Peer, interval time.Duration, logger rlog.RLogger) {
	// TODO: Might also be worth retrying connections with  dropped peers incase it was just
	// a network glitch
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		logger.Println("returning back to parent")
	}()

	reply := AppendEntryReply{}

	for {
		select {
		case replicate := <-peer.replicateCh:
			logger.Println("recvd entry::", replicate)
			req := AppendEntryRequest{
				Id:              n.id,
				Term:            n.raft.Term(),
				Message:         "This is a new entry",
				LastCommitIndex: n.logs.LastCommited(),
				Entry:           &replicate.entry,
			}
			req.Entry = &replicate.entry
			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				logger.Println("Failed to send appendEntry with data", err, peer.id, peer.addr, req)
				return
			}
			logger.Println("reply: ", reply, peer.addr)
			if reply.Acked {
				replicate.done <- true
				replicate.success.Add(1)
				logger.Println("CONSTRUCTION:: increased replicationCount for commitFunc")
			}
			ticker.Reset(interval)
			continue
		default:
			// break if no logEntries need to be sent
		}

		select {
		case <-ctx.Done():
			return

		case replicate := <-peer.replicateCh:
			req := AppendEntryRequest{
				Id:              n.id,
				Term:            n.raft.Term(),
				Message:         "This is a new entry",
				LastCommitIndex: n.logs.LastCommited(),
				Entry:           &replicate.entry,
			}

			logger.Println("recvd entry::", replicate)
			req.Entry = &replicate.entry
			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				logger.Println("Failed to send appendEntry with data", err, peer.id, peer.addr, req)
				return
			}
			logger.Println("reply: ", reply, peer.addr)
			if reply.Acked {
				replicate.done <- true
				replicate.success.Add(1)
				logger.Println("CONSTRUCTION:: increased replicationCount for commitFunc")
			}
			ticker.Reset(interval)
			continue

		case <-ticker.C:
			req := AppendEntryRequest{
				Id:              n.id,
				Term:            n.raft.Term(),
				Message:         "This is a heartbeat message",
				LastCommitIndex: n.logs.LastCommited(),
			}
			logger.Println("sending heartbeatRPC")
			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				logger.Println("Failed to send heartbeat", err, peer.id, peer.addr)
				return
			}
			if !reply.Acked {
				logger.Println("reply: ", reply, peer.addr)
			}
			logger.Println("log-inspection-diagnostics:", n.Diagnostics())
		}
	}
}

func randomTimeout(d time.Duration) time.Duration {
	// crypto/rand requires a *big.Int for limits
	limit := big.NewInt(int64(maxInterval - minInterval + 1))
	n, _ := rand.Int(rand.Reader, limit)

	actualInterval := n.Int64() + int64(minInterval)
	return d * time.Duration(actualInterval)
	// n := rand.IntN(maxInterval-minInterval) + minInterval
	//
	// return d * time.Duration(n)
}

// TODO: Commit command to database
//
// CONTEMPLATION:: Usually, I'm looking for a way to get the workers to comms
// with the main leader. Using a channel sounds good, but idk why I'm iffy. Because of theb
// scheduler. painful. Sure at somepoint, a worker might be sending to done <- struct{}{}
// but this function might not have started listening on it. Using a waitgroup here would also
// not work, because the goroutines need to terminate before wg.Wait before if can unblock. And
// I don't think Cond is the right thing either. So I want to try a timer. After x-ms, check the
// successReplicas *atomic.Uint32 and if they meet quorum, execute and respond back to client.
// One issue i have w this approach is that already sending to a cluster from a client's perspective
// is already slow. Sending to cluster, getting quorum, executing to database, and getting result from
// db and sending back to client is a long trip. If anything, i think this increases latency painfully
func (n *Node) commitFunc(payload CommandRequest, entry Entry, reply chan RPCReply, logger rlog.RLogger) {
	// send new entries to all workers
	numPeers := len(n.rpcPeers)
	done := make(chan bool, numPeers)

	successCount := atomic.Uint32{}
	successCount.Add(1)

	for _, peer := range n.rpcPeers {
		select {
		case peer.replicateCh <- replicate{
			entry:   entry,
			success: &successCount,
			done:    done,
		}:
		default:
			logger.Println("WARNING:", fmt.Sprintf("peer-worker: %d is blocked, dropping entry", peer.id))
			done <- false
		}
	}

	quorumTarget := (numPeers / 2) + 1
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
	defer cancel()
	for i := range numPeers {
		_ = i
		if successCount.Load() >= uint32(quorumTarget) {
			logger.Println("quorum has been reached for replication", successCount.Load())
			break
		}
		select {
		case <-ctx.Done():
			logger.Println("replication quorum failed under 180ms")
			return
		case <-done:
		}
	}

	if successCount.Load() < uint32(quorumTarget) {
		logger.Println("replication failed to reach quorum", successCount.Load(), quorumTarget)
		return
	}

	response, err := n.database.Commit(db.Command{Operation: payload.Operation, Key: payload.Key, Value: payload.Value})
	if err != nil {
		logger.Println("could not commit database with command: ", payload, err)
		return
	}

	logger.Println(fmt.Sprintf("debug:: after commiting: %s, sending response", payload.Key))
	reply <- RPCReply{kind: ClientCommand, payload: &CommandReply{
		From:   "raft-leader-database",
		Result: response.Message,
	}}
	logger.Println("successfully sent response to client")
}
