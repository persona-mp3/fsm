package main

type Follower struct {
	network chan any
	store   *Store
}

// If desired, the protocol can be optimized to reduce the
// number of rejected AppendEntries RPCs. For example,
// when rejecting an AppendEntries request, the follower
// can include the term of the conflicting entry and the first
// index it stores for that term. With this information, the
// leader can decrement nextIndex to bypass all of the con-
// flicting entries in that term; one AppendEntries RPC will
// be required for each term with conflicting entries, rather
// than one RPC per entry. In practice, we doubt this opti-
// mization is necessary, since failures happen infrequently
// and it is unlikely that there will be many inconsistent en-
// tries.
type AppendEntry struct {
	prev_log_index uint
	commit_index   uint
	acked          bool
}

func (f Follower) run() {
	req := AppendEntry{}
	logs_match, prev_log_index := f.verify_append_entry_req(req)
	if !logs_match {
		f.send_append_entry(false, prev_log_index)
	}
}

// Not sure if we want a seperate protocol designed for this?  Maybe a UpdateRPC which signifies
// that these nodes are syncing where one is the leader and the other is the follower? or we could
// have a [Reason] enum in our actual implementation. Where a follower could not acked because:
// 1. lower term from 'leader'
// 2. out of synced logs
// 3. some other mysterious case?
// That would completely take away the [Acked] flag on our payload as we can also have success enums
//  1. ReusltAcked
//     And then failures
//  2. ResultStaleLeader
//  3. ResultLogsOutOfSync
//  4. ResultUnknownMyImplementerShouldQuitAndDoFarming
//
// I think this is a better approach due to how we have different error paths.
// On the receiver side, they now have a more concrete idea of where things could be. For example
//
//	switch req.Result {
//		case ResultAcked: continue
//		case ResultLogsOutOfSync:
//				followersPrevLogIndex := req.prevLogIndex
//				snapshot, found := takeSnapshotTill(followerPrevLogIndex)
//				if !found {
//						// this would be the perfect case, because I had never thought a follower would send
//						// something the leader doesn't have at all. Which is kind of odd...
//						return
//	     }
//
//	     // send_snaphost should attempt 3 sends
//				go func() {
//					res, err := send_snapshot(snapshot)
//					if err != nil {
//						logger.Warn("could not send snaphost to follower. Reason: %s", err)
//					}
//				}
//	 case ResultUnknownMyImplementerShouldQuitAndDoFarming: 
//			logger.Warn("you need to start farming")
//	}
//
// I'd also like us to try injecting some new things like go's errgroup. Instead of normal 
// wg.Go or go funcs
//
// PROBLEM: The only issue with the Enum approach is that if it grows, we're more likely to miss some out
// Is there a tool that handles this for us?
func (f Follower) send_append_entry(acked bool, prev_log_index uint) error {
	select {
	case f.network <- AppendEntry{acked: acked, prev_log_index: prev_log_index, commit_index: uint(f.store.get_leader_commit())}:
	default:
		println("warning: network channel blocked. dropping request")
	}
	return nil
}

func (f Follower) verify_append_entry_req(req AppendEntry) (bool, uint) {
	prev_log := f.store.get_previous_log_index()
	if req.prev_log_index != uint(prev_log.idx) {
		return false, uint(prev_log.idx)
	}

	return true, req.prev_log_index
}

func (f Follower) check_snapshot() {

}
