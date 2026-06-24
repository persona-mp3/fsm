package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Opts struct {
	log log.Logger
}

func defaultOpts() *Opts {
	return &Opts{
		log: *log.New(os.Stdout, "", log.Default().Flags()|log.Lmicroseconds),
	}
}

// runLeader is responsible for sending out heartbeats to other
// peers in the cluster. When it receives an [AppendEntryReq] from another node,
// it sends a transition to the [Raft.Run] loop. Otherwise it disregards the rpcRequest
// and responds with it's own term
func (r *Raft) runLeader(opts *Opts) {
	var o *Opts
	if opts == nil {
		o = defaultOpts()
	} else {
		o = opts
	}

	o.log.SetPrefix(fmt.Sprintf("(%s:leader) ", r.id))

	o.log.Println("started: ", r.Diagnostics())
	// TODO: this should be changed later, it's a second for easy debugging
	const dur = 50 * time.Millisecond
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	for {
		select {
		case <-r.stateCtx.Done():
			return
		// TODO: when this is fully implemented, seperate this into it's own routine
		case <-ticker.C:
			o.log.Println("sending heartbeat to peers...")
		case rpcReq := <-r.incoming:
			switch rpcReq.kind {
			case AppendEntry:
				payload, ok := rpcReq.payload.(AppendEntryReq)
				if !ok {
					o.log.Panicf("expected appendEntry from payload, recvd: %+v\n", payload)
				}

				if transit := r.handleAppendEntryRPC(o, payload, rpcReq.reply); transit {
					o.log.Println("sending transition request to manager")
					r.transition <- Follower
					o.log.Println("sent transition request successfully")
					return
				}
			case RequestVote:
				o.log.Println("processing RequestVoteRPC request")
				req, ok := rpcReq.payload.(RequestVoteReq)
				if !ok {
					rpcReq.reply <- RPCReply{kind: RequestVote, payload: &RequestVoteRes{
						err:  fmt.Errorf("%s internal error occured", r.getCurrentState().String()),
						Id:   r.id,
						Term: r.term.Load(),
					}}
					o.log.Panicf(`expected RequestVoteRPC request from got : %+v\n`, rpcReq)
				}

				// rpcReq came from an outdated candidate or leader. ignore them
				if req.Term < r.term.Load() {
					rpcReq.reply <- RPCReply{
						kind: RequestVote,
						payload: &RequestVoteRes{
							Id:     r.id,
							Acked:  false,
							Term:   r.term.Load(),
							Reason: fmt.Sprintf("%s: term higher", r.getCurrentState().String()),
						},
					}
					o.log.Println("requestVoteRPC had a lower term, rejecting it", r.Diagnostics())
					continue
				} else if req.Term == r.term.Load() {
					rpcReq.reply <- RPCReply{
						kind: RequestVote,
						payload: &RequestVoteRes{
							Id:     r.id,
							Term:   r.term.Load(),
							Reason: fmt.Sprintf("%s: an internal error occured", r.getCurrentState()),
						},
					}
					// TODO: PANIC : SPLIT_VOTE 
					o.log.Printf(`
					we have a split brain scenario. This node who is a leader, also 
					recvd an RequestVoteRPC from a node of the same term. 
					RPCRequestTerm: %+v
					Diagnotics: %s
					
					While this hasn't been accounted for yet, we are going to panic
					`, req, r.Diagnostics())
					panic("")
				}

				o.log.Printf("stepping down to Follower, recvd a higher RPCTerm: %d, %s\n", req.Term, r.Diagnostics())
				r.term.Store(req.Term)
				rpcReq.reply <- RPCReply{
					kind: RequestVote,
					payload: &RequestVoteRes{
						Id:     r.id,
						Acked:  true,
						Term:   r.term.Load(),
						Reason: fmt.Sprintf("%s: an internal error occured", r.getCurrentState()),
					},
				}
				o.log.Println("sent replyRPC, transitioning to Follower")
				r.transition <- Follower

			default:
				rpcReq.reply <- RPCReply{
					kind: AppendEntry,
					payload: &AppendEntryRes{
						Id:           r.id,
						Term:         r.term.Load(),
						Data:         "I dont understand this rpc call yet",
						Acknowledged: false,
					},
				}
				log.Printf("rpcRequest not understood: %+v\n", rpcReq)
			}

		}
	}

}

// handleAppendEntryRPC processes an incoming AppendEntry RPC and sends a reply to the caller.
// It returns true to signal that a state transition is required, and false if the RPC
// should be ignored.
//
//   - [Leader] or [Candidate]: if true is returned, this node should transition to [Follower].
//   - [Follower]: if true is returned, reset the heartbeat timer and apply any log entries
//     present in the payload.
//
// Returns true when the incoming term is greater than or equal to this node's current term,
// indicating the RPC is from a legitimate or more up-to-date leader.
func (r *Raft) handleAppendEntryRPC(o *Opts, req AppendEntryReq, reply chan RPCReply) bool {
	// while a  node is a leader or candidate, they will change to a follower state
	// with their term updated. if a follower, the term and logs are just updated
	// heartbeat timers reset
	if req.Term > r.term.Load() {
		o.log.Printf("reqRPC: %d is larger %s\n", req.Term, r.Diagnostics())
		r.term.Store(req.Term)
		reply <- RPCReply{
			kind: AppendEntry,
			payload: &AppendEntryRes{
				Id:           r.id,
				Data:         "recvd larger rpc, yielded",
				Acknowledged: true,
			}}
		o.log.Println("sentReply to rpcClient")
		return true
	} else if req.Term < r.term.Load() {
		o.log.Printf("recvd a lower termRPC: %d,  %s Ignoring rpc\n", req.Term, r.Diagnostics())
		reply <- RPCReply{
			kind: AppendEntry,
			payload: &AppendEntryRes{
				Id:           r.id,
				Term:         r.term.Load(),
				Data:         "sender's rpc outdated",
				Acknowledged: false,
			},
		}

		o.log.Printf("sent rpc to lowerClient")
		return false
	} else {
		// Terms are equal - treat as valid heartbeat from leader
		o.log.Printf("reqRPC term %d match. possibly from current leader %s\n", req.Term, r.Diagnostics())
		reply <- RPCReply{
			kind: AppendEntry,
			payload: &AppendEntryRes{
				Id:           r.id,
				Data:         "updated my logs",
				Acknowledged: true,
			}}
		o.log.Println("sent rpcReply to client-node")
		o.log.Println("sentRPC")
	}

	return true
}
