package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// func (r *Raft) Begin(parentCtx context.Context) {
// 	errch := make(chan error)
// 	ctx, cancel := context.WithCancel(parentCtx)
// 	defer cancel()
//
// 	go func() {
// 		if err := r.server.Listen(ctx, r.address); err != nil {
// 			log.Println("(server_subroutine) error occured, sending to node")
// 			errch <- err
// 		}
// 	}()
// 	log.Printf("(raft) node started: %s", r.Diagnostics())
//
// 	stateCtx, stateCancel := context.WithCancel(context.Background())
// 	r.stateCtx = stateCtx
// 	r.stateCancel = stateCancel
//
// 	defer r.stateCancel()
//
// 	go func() {
// 		r.startFollower()
// 	}()
//
// 	for {
// 		select {
// 		case <-parentCtx.Done():
// 			return
// 		case err := <-errch:
// 			log.Println("error from server >> ", err)
// 			panic("")
// 		case raftState := <-r.transition:
// 			r.stateCancel()
// 			r.newStateContext(parentCtx)
//
// 			switch raftState {
// 			case Leader:
// 				log.Println("(node) recvd transition to turn leader, replacing with folower")
// 				r.electionTimeout = generateRandomTimeout(time.Millisecond)
// 				r.updateRaftState(raftState)
// 				go r.startLeader()
// 				// go r.startFollower()
// 			case Follower:
// 				log.Println("(node) recvd transition to turn follower")
// 				if r.getCurrentState() == Follower {
// 					log.Panicf("(node) currently  a follower, recvd Follower transition currState: %s to: %s\n", raftState.String(), r.getCurrentState().String())
// 				}
//
// 				r.electionTimeout = generateRandomTimeout(time.Millisecond)
// 				r.updateRaftState(raftState)
//         go r.startFollower()
//
// 			}
// 		}
// 	}
// }

func (r *Raft) startFollower() {
	timer := time.NewTimer(r.electionTimeout)
	defer func() {
		if !timer.Stop() {
			go func() { <-timer.C }()
		}
		log.Println("(d_follower) exiting")
	}()

	resetTimer := func() {
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(r.electionTimeout)
	}

	for {
		select {
		case <-r.stateCtx.Done():
			return
		case rpc := <-r.incoming:
			if rpc.kind == AppendEntry {
				log.Println("(d_follower) heartbeat recvd, restarting timer")
				resetTimer()
				log.Println("(d_follower) sending rpcReply")
				rpc.reply <- RPCReply{kind: AppendEntry, payload: &AppendEntryRes{
					Term:         r.term.Load(),
					Data:         "I yield to you",
					Acknowledged: true,
					Id:           "(d_follower)-single-node-server",
					err:          nil,
				}}
				log.Println("(d_follower) sent rpcReply")
			} else {
				log.Printf("(d_follower) unexpected rpc: %+v\nsending rpcReply\n", rpc.payload)
				rpc.reply <- RPCReply{kind: AppendEntry, payload: &AppendEntryRes{
					Term:         r.term.Load(),
					Data:         "I don't undertand this rpc",
					Acknowledged: false,
					Id:           "(d_follower)-single-node-server",
					err:          nil,
				}}
				log.Println("(d_follower) sent rpcReply")
			}
		case <-timer.C:
			log.Println("(d_follower) timer fired, going to leader mode")
			r.transition <- Leader
			return
		}
	}
}
// func (r *Raft) startLeader() {
// 	log.Println("(d-leader) started:", r.Diagnostics())
// 	ticker := time.NewTicker(1 * time.Second)
// 	defer ticker.Stop()
// 	for {
// 		select {
// 		case <-ticker.C:
// 			log.Println("(d-leader) sending heartbeats to ye followers")
// 		case <-r.stateCtx.Done():
// 			return
// 		case rpc := <-r.incoming:
// 			rpcKind := rpc.kind
// 			switch rpcKind {
// 			case AppendEntry:
// 				payload, ok := rpc.payload.(AppendEntryReq)
// 				if !ok {
// 					log.Panicf("(d_leader) expected appendEntry: %+v\n", payload)
// 				}
//
// 				if payload.Term > r.term.Load() {
// 					r.term.Store(payload.Term)
// 					rpc.reply <- RPCReply{
// 						kind: AppendEntry,
// 						payload: &AppendEntryRes{
// 							Id:           "(d-leader) [-]",
// 							Data:         "your rpc is bigger,  yielding to you",
// 							Acknowledged: true,
// 						},
// 					}
// 					log.Printf("(d_leader) dropping from leader to follower: %+v\n", payload)
// 					r.transition <- Follower
// 				} else {
// 					rpc.reply <- RPCReply{
// 						kind: AppendEntry,
// 						payload: &AppendEntryRes{
// 							Id:           "(d-leader)",
// 							Data:         "disregarding your opinions",
// 							Acknowledged: false,
// 						},
// 					}
// 					log.Printf("(d_leader) disregarding rpc from lowerterm candidate: %+v\n", payload)
// 				}
//
// 			default:
// 				rpc.reply <- RPCReply{
// 					kind: AppendEntry,
// 					payload: &AppendEntryRes{
// 						Id:           "(d-leader)",
// 						Data:         "I dont understand this rpc call yet",
// 						Acknowledged: false,
// 					},
// 				}
// 				log.Printf("(d_leader) leader does not understand this rpc: %+v\n", rpc)
// 			}
// 		}
// 	}
// }
//

func (r *Raft) Diagnostics() string {
	diagnostics := fmt.Sprintf("diagnostics: { address: %s, state: %s, term: %d, electionTimeout: %s }",
		r.serverAddr, r.state.String(), r.term.Load(), r.electionTimeout)
	return diagnostics
}

func (r *Raft) newStateContext(parentCtx context.Context) {
	ctx, cancel := context.WithCancel(parentCtx)
	r.stateCtx = ctx
	r.stateCtxCancel = cancel
}
