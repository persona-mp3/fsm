package main

import (
	"context"
	"fmt"
	"time"
)

func (r *Raft) runCandidate(opts *Opts) {
	var o *Opts
	if opts == nil {
		o = defaultOpts()
	} else {
		o = opts
	}

	o.log.SetPrefix(fmt.Sprintf("(%s:candidate) ", r.id))

	o.log.Println("canididate state not implemented yet, increasing term")
	r.incrementTerm()

	timeout := time.NewTimer(randomTimeout(time.Millisecond))
	defer func() {
		if !timeout.Stop() {
			go func() { <-timeout.C }()
		}

		o.log.Println("drained timer")

	}()

	oneshot := randomTimeout(time.Millisecond)
	oneshotTimer := time.NewTimer(oneshot)

	select {
	case <-timeout.C:
		o.log.Println("electoralTimeout reached, dropping to follower")
		r.transition <- Follower
		return
	case <-r.stateCtx.Done():
		return
	case <-oneshotTimer.C:
		o.log.Println("oneshot timer hit, using electoral stub")
		if int(oneshot.Seconds())%2 == 0 {
			r.transition <- Leader
		} else {
			r.transition <- Follower
		}
		return

	case rpc := <-r.incoming:
		o.log.Printf("recvd rpc: %+v\n", rpc.payload)
		switch rpc.kind {
		case AppendEntry:
			payload, ok := rpc.payload.(AppendEntryReq)
			if !ok {
				o.log.Panicf("expected AppendEntryReq, got: %+v\n\n", payload)
			}

			if r.handleAppendEntryRPC(o, payload, rpc.reply) {
				o.log.Printf("dropping from Candidate to Follower due to higher rpc: %+v\n", payload)
				r.transition <- Follower
				return
			}
		}

	}
}

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

// Candidate increments this nodes term and sends rpcs too all the peers 
// it has connections to
func (r *Raft) Candidate(opts *Opts) { }
