package main

import (
	"context"
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

func (r *Raft) runCandidateDep(opts *Opts) {
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
func (r *Raft) Candidate(opts *Opts) {
	var o *Opts
	if opts == nil {
		o = defaultOpts()
	} else {
		o = opts
	}

	o.log.SetPrefix(fmt.Sprintf("(%s:candidate_) ", r.id))

	electionTimeout := randomTimeout(time.Millisecond)
	electionTimer := time.NewTimer(electionTimeout)

	rpcPeers, failed := dialRaftPeers("tcp", r.peers)

	if failed == len(r.peers) {
		o.log.Panicf(`
		edge case. This node is the only node active here, 
		triedRaisingElection, all rpcPeers failed. 
		failedDials: %d, allPeers: %+v
		`, failed, r.peers)
	}

	totalVotes := atomic.Uint64{}

	// according to the Raft Paper, a node can vote itself when running for an election
	totalVotes.Add(1)

	wg := sync.WaitGroup{}

	for _, rpcClient := range rpcPeers {
		if rpcClient != nil {
			wg.Add(1)
			go func(dialer *rpc.Client, totalVotes *atomic.Uint64) {
				defer wg.Done()
				makeRequestVoteRPC(dialer, totalVotes)

			}(rpcClient, &totalVotes)
		}
	}

	done := make(chan struct{})

	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-r.stateCtx.Done():
		return
	case <-electionTimer.C:
		o.log.Println("electionTimer fired before could make all votes, dropping to follower and waiting...")
		r.transition <- Follower
		return
	case <-done:
		o.log.Println("all votes have been collected before electionTimer, checking vote count")
		result := totalVotes.Load()
		if result > 2 {
			o.log.Println("i'm not going to be a leader...", result)

			// TODO: It's not yet clear that we might need a mutex here considering the fact that
			// each RaftState owns the Node ie only one thread can mutate r.rpcClients
			r.rpcPeers = append(r.rpcPeers, rpcPeers...)
			r.transition <- Leader
			return
		}

		o.log.Println("lost the election, not enough votes, going back to follower")
		r.transition <- Follower

	case rpcReq := <-r.incoming:
		switch rpcReq.kind {
		case AppendEntry:
			req, ok := rpcReq.payload.(AppendEntryReq)
			if !ok {
				o.log.Panicf(`expected AppendEntryReq from RPCRequest got : %+v\n`, rpcReq)
			}

			isHigherTerm := r.handleAppendEntryRPC(o, req, rpcReq.reply)
			if isHigherTerm {
				o.log.Printf("lost the election, recvd higherRPC: %d, %s going back to follower\n", req.Term, r.Diagnostics())
				r.transition <- Follower
				return
			}

		default:
			r.handleUnknownRPCKind(rpcReq, o)
		}

	}

}

func makeRequestVoteRPC(dial *rpc.Client, totalVotes *atomic.Uint64) {
	log.Panicf("[makeRequestVoteRPC] not implemented yet")
}
