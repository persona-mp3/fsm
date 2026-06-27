package main

import (
	"fmt"
	"sync"
	"context"
	rlog "fsm/raftlogger"
	"net/rpc"
	"time"
)

func (n *Node) runLeader(logger rlog.RLogger) {
	logger.Println("leader state transitioned successfully", n.Diagnostics())
	ticker := time.NewTicker(heartbeatInterval)
	defer func() {
		ticker.Stop()
		logger.Println("leader state terminated succesfully")
	}()

	ctx, cancel := context.WithCancel(n.stateCtx)
	defer cancel()

	wg := sync.WaitGroup{}

	if len(n.connectedPeers) == 0 {
		// we might want a resting state or Unit because it could possbily 
		// mean this node is the only one active in the cluster and others have died
		logger.Println("todo::warning:: could not find any connected peer")
	}

	for idx,  rpcPeer := range n.connectedPeers  {
		if rpcPeer != nil {
			wg.Add(1)

			childLogger := n.log.Inherit(fmt.Sprintf("%d-sendHB", idx))

			go func(ctx context.Context, d *rpc.Client, logger rlog.RLogger){
				defer wg.Done()
				sendHeartBeat(ctx, d,  heartbeatInterval, logger)
			}(ctx, rpcPeer, childLogger)
		}
	}

	// wait for all children to return
	done := make(chan struct{})
	go func(){
		wg.Wait()
		done <- struct{}{}
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

func sendHeartBeat(ctx context.Context, dial *rpc.Client, interval time.Duration, logger rlog.RLogger) {
	ticker := time.NewTicker(interval)
	defer func(){
		ticker.Stop()
		logger.Println("returning back to parent")
	}()

	for {
		select {
		case <-ticker.C:
			logger.Println("sending heartbeatRPC")
			time.Sleep(2  * time.Second)
			ticker.Reset(interval)

		case <-ctx.Done():
			return
		}
	}
}
