package main

import (
	"context"
	"os"
	"os/signal"
	"time"
)

// NOTEs: terms act as logical clocks in Raft. It's possible
// for servers not to observer a whole election on term. One
// correctness for Raft is that, there must at most be one leader
// at a time for a term. From the [Fig 5] diagram, a term contains
// an election period and normal time operation. In a case where
// an election yields no leader for example, a split vote is
// a result of an election (ie two leaders), the term is incremented
// and thus another election till one leader emerges

// Rafts Core
// ----------
// - [ ] Leader Election
// - [ ] Log Replication
// - [ ] Saftey

// Term acts as a logical clock in Raft.
// It allows servers to detect obsolete information such as stale
// leaders, or stale data. Each server stores a [currentNumber] which
// increases monotonically overtime

// Current terms are exchanged whenever servers communicate. If one is lower
// than the other, then they sync up. If a [Candidate] or [Leader] discovers it's
// term is out of date, it immediately resolves to a [Follower] state

// Leader Election
// Leaders send periodic hearbeats ie AppendEntries that carry no log entry
// If a follower recvs no communication over a period of time, [electionTimeout]
// To begin an election, a follower
// __1__ increments it's current term
// __2__ transitions into [Candidate]
// __3__ votes for itself and issues RPC's to other servers in the cluster

/*
Candidate
A node remains a [Candidate] until one of these events occur:
1. Wins the election
2. Another server establishes itself as [Leader]
3. A period of time goes by without a winner

While in a [Candidate] state, the node recvs an AppendEntryRPC from another
node that claims to be a leader, it must first check that the RPC's term, is
at least as large as it's own term. Then the [Candiate] recognizes the [Leader]
as legitimate and returns to a [Follower], otherwise, the candidate rejects it

In the occurence of a split vote where many [Follower]s become [Candidate]s
and there's no majority vote, the candidate will timeout again and start a new
election. Election timeouts are randomized to avoid split votes, typically
chosen at a fixed interval of 150-300ms. For each new election, the [electionTimeout]
is randomised
*/

func main() {
	electionTimeout := generateRandomTimeout(time.Millisecond)
	raft := NewRaft("localhost:8080", []string{}, electionTimeout)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	raft.Run(ctx)
}
