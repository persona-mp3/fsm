package main

import (
	"fmt"
	"time"
)

// runFollower has an internal timer that goes off on [Raft.electionTimeout]
// When the timer fire, it transists into a [Candidate] state. If an [AppendEntry] rpc
// or `heartbeat` arrives before the timer fires, the node remains in this state
// and resets it's internal timer. If it receives an [AppendEntry] rpcs from another
// nodes whose term is higher it updates this nodes term to the rpc provided in the request
func (r *Raft) runFollower(opts *Opts) {
	var o *Opts
	if opts == nil {
		o = defaultOpts()
	} else {
		o = opts
	}

	o.log.SetPrefix(fmt.Sprintf("(%s:follower) ", r.id))
	timer := time.NewTimer(r.electionTimeout)
	defer func() {
		if !timer.Stop() {
			go func() { <-timer.C }()
		}
		o.log.Println("exiting state ")
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
		case rpcReq := <-r.incoming:
			switch rpcReq.kind {
			case AppendEntry:
				payload, ok := rpcReq.payload.(AppendEntryReq)
				if !ok {
					o.log.Panicf("expected appendEntry from payload, recvd: %+v\n", payload)
				}

				if transit := r.handleAppendEntryRPC(o, payload, rpcReq.reply); transit {
					resetTimer()
					r.term.Store(payload.Term)
					o.log.Println("updated term info and reset timer")
					continue
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
				} else if req.Term == r.term.Load() || req.Term > r.term.Load() {
					// treating rpcReq as a an already established leader
					rpcReq.reply <- RPCReply{
						kind: RequestVote,
						payload: &RequestVoteRes{
							Id:     r.id,
							Acked:  true,
							Term:   r.term.Load(),
							Reason: fmt.Sprintf("%s: your term is higher. i yeild", r.getCurrentState().String()),
						},
					}
					o.log.Printf("requestVoteRPC had a higher term:%d, updating term to match theirs: %s\n", req.Term, r.Diagnostics())
					r.term.Store(req.Term)
					o.log.Println("updated term", r.Diagnostics())
					o.log.Println("term updated transitioning to Follower", r.Diagnostics())
				} else {
					o.log.Panicf(`
					while in a Follower State:
					Unaccounted for: %+v, %s\n\n`,
						rpcReq, r.Diagnostics())
				}

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
				o.log.Printf("rpcRequest not understood: %+v\n", rpcReq)
			}

		case <-timer.C:
			r.transition <- Candidate
			o.log.Println("timeout reached without hearbeat, tranisitioning to Candidate ")
			return
		}
	}
}
