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
		log: *log.New(os.Stdout, "", log.Default().Flags()),
	}
}

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
	const dur = 2 * time.Second
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	for {
		select {
		case <-r.stateCtx.Done():
			return
		// TODO: when this is fully implemented, seperate this into it's own routine
		case <-ticker.C:
			o.log.Println("sending heartbeat to peers...")
		case rpc := <-r.incoming:
			switch rpc.kind {
			case AppendEntry:
				payload, ok := rpc.payload.(AppendEntryReq)
				if !ok {
					o.log.Panicf("expected appendEntry from payload, recvd: %+v\n", payload)
				}

				if transit := r.handleAppendEntryRPC(o, payload, rpc.reply); transit {
					o.log.Println("sending transition request to manager")
					r.transition <- Follower
					o.log.Println("sent tranition request successfully")
					return
				}

			default:
				rpc.reply <- RPCReply{
					kind: AppendEntry,
					payload: &AppendEntryRes{
						Id:           "id",
						Term:         r.term.Load(),
						Data:         "I dont understand this rpc call yet",
						Acknowledged: false,
					},
				}
				log.Printf("rpcRequest not understood: %+v\n", rpc)
			}

		}
	}

}

func (r *Raft) handleAppendEntryRPC(o *Opts, req AppendEntryReq, reply chan RPCReply) bool {
	if req.Term > r.term.Load() {
		o.log.Printf("reqRPC: %d is larger %s\n", req.Term, r.Diagnostics())
		r.term.Store(req.Term)
		reply <- RPCReply{
			kind: AppendEntry,
			payload: &AppendEntryRes{
				Id:           r.id,
				Data:         "leader(-_-) recvd larger rpc, stepping down from leader",
				Acknowledged: true,
			}}
		o.log.Println("sentRPC  sending transition to Follower")
		o.log.Println("sentRPC")
		return true
	}

	o.log.Printf("recvd a lower termRPC: %d,  %s Ignoring rpc\n", req.Term, r.Diagnostics())
	reply <- RPCReply{
		kind: AppendEntry,
		payload: &AppendEntryRes{
			Id:           r.id,
			Term:         r.term.Load(),
			Data:         "disregarding your opinions",
			Acknowledged: false,
		},
	}

	o.log.Printf("sent rpc to lowerClient")
	return false
}

func (r *Raft) handleUnknownRPC(rpc RPC) {}
