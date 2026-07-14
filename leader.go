package main

import (
	"context"
	"fmt"
	db "fsm/database"
	rlog "fsm/raftlogger"
	"math/rand/v2"
	"sync"
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
	// entries := make(chan Entry)
	for idx, rpcPeer := range currentPeers {
		if rpcPeer != nil {
			wg.Add(1)

			childLogger := n.log.Inherit(fmt.Sprintf("%d-sendHB", idx))

			go func(ctx context.Context, rpcPeer *Peer, logger rlog.RLogger) {
				defer wg.Done()
				dropCh := make(chan Entry, dropChBuff)
				rpcPeer.dropCh = dropCh
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
				// no point in relaying response backup to the server because the server will still invalidate it and panic
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

				payload, ok := req.payload.(CommandReq)
				if !ok {
					logger.Panic("expected CommandReq as payload got:", payload)
				}

				go func() {
					response, err := n.database.Commit(db.Command{Operation: payload.Operation, Key: payload.Key, Value: payload.Value})
					if err != nil {
						logger.Println("could not commit database with command: ", req.payload, err)
						return
					}

					logger.Println("database-response after commiting: ", response.Message)
				}()


				req.reply <- RPCReply{
					kind: ClientCommand,
					payload: &CommandReply{
						From:   n.id,
						Result: "LEADER_STUB: Need to send requests to whole cluster to append this to their logs",
					},
				}
				logger.Println("begin sending out appendRPCs")
				entry := Entry{}
				entry.Term = n.raft.getTerm()
				entry.Operation = payload.Operation
				entry.Key = payload.Key
				entry.Value = payload.Value
				if !n.logs.HasEntry(&entry) {
					index := n.logs.Append(&entry)
					entry.Idx = index

					// send new entries to all workers
					for _, peer := range n.rpcPeers {
						select {
						case peer.dropCh <- entry:
						default:
							logger.Println("WARNING:", fmt.Sprintf("peer-worker: %d is blocked, dropping entry", peer.id))
						}
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
				return

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

	req := AppendEntryRequest{
		Id:      n.id,
		Term:    n.raft.getTerm(),
		Message: "This is a heartbeat message",
	}

	reply := AppendEntryReply{}

	for {
		select {
		case e := <-peer.dropCh:
			logger.Println("recvd entry::", e)
			req.Entry = &e
			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				logger.Println("Failed to send appendEntry with data", err, peer.id, peer.addr, req)
				return
			}
			logger.Println("reply: ", reply, peer.addr)
			ticker.Reset(interval)
			continue
		default:
			// break if no logEntries need to be sent
		}

		select {
		case <-n.stateCtx.Done():
			return

		case e := <-peer.dropCh:
			logger.Println("recvd entry::", e)
			req.Entry = &e
			if err := peer.rpcConn.Call("Server.AppendEntryRPC", req, &reply); err != nil {
				logger.Println("Failed to send appendEntry with data", err, peer.id, peer.addr, req)
				return
			}
			logger.Println("reply: ", reply, peer.addr)
			ticker.Reset(interval)
			continue

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
