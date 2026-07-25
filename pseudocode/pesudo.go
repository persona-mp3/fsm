package main

type Node struct {
	id           string
	currentTerm  uint
	logs         Logs
	leaderCommit int
	db           Database
}

func (n *Node) GetLogAt(idx int) (*Entry, bool) {
	return nil, false
}

func (n *Node) handleRPC(req AppendEntryRPC) (*AppendEntryReplyRPC, Status) {
	log, found := n.GetLogAt(req.PrevLog.Index)
	if !found {
		return nil, StatusOutOfSyncLogs
	}

	if log.Term != req.PrevLog.Term {
		return nil, StatusOutOfSyncLogs
	}

	if n.leaderCommit == req.PrevLog.LeaderCommit {
		return nil, StatusLogsMatch
	}
	entry := n.applyCommitsTill(req.PrevLog.LeaderCommit)
	return &AppendEntryReplyRPC{
		From: n.id,
		LogProperties: LogProperties{
			Index:        entry.Idx,
			Term:         entry.Term,
			LeaderCommit: entry.Idx,
		},
	}, StatusLogsMatch
}

func (n *Node) applyCommitsTill(stopCommit int) Entry {
	_ = stopCommit
  n.db.Commit("set username ballon_dior")
	return Entry{}
}
