package main

import (
	"fmt"
)

type Entry struct {
	Idx   int
	Term  uint64
	Ops   string
	Key   string
	Value string
}

type Logs struct {
	Entries []*Entry
}

type Status int

const (
	StatusAcked Status = 0
	StatusRejected
	StatusOutOfSyncLogs
	StatusLogsMatch
)

type LogProperties struct {
	//  PreviousLogIndex is the logIndex preceeding new ones. this is used to check against inconsistency
	Index        int
	Term         uint64
	LeaderCommit int
}

type AppendEntryRPC struct {
	From        string
	CurrentTerm uint64
	PrevLog     LogProperties
}

type AppendEntryReplyRPC struct {
	From          string
	Acknowledged  Status
	LogProperties LogProperties
	Status        Status
}

type Database interface {
	Commit(string) (*DBResponse, error)
}

type Machine struct {
	Id      string
	Logs    Logs
	PrevLog LogProperties
	DB      Database
}

// after deciding whether to accept rpc by checking term and it came from a recognized leader...
func (m *Machine) follower(req AppendEntryRPC) AppendEntryReplyRPC {
	reply := AppendEntryReplyRPC{}
	reply.From = m.Id

	switch {
	// leaders lastLogIndex before new ones is 3 and ours is 1
	// if we dont have the last log before the latest one the leader has we fail fast and send outOfSync
	case m.PrevLog.Index != req.PrevLog.Index:
		reply.Status = StatusOutOfSyncLogs
		reply.LogProperties = m.PrevLog

		// if our lastIndexes match but our terms dont match we send outOfSync
	case m.PrevLog.Term != req.PrevLog.Term:
		reply.Status = StatusOutOfSyncLogs
		reply.LogProperties = m.PrevLog

		// if ourTerms and indexes match and the leaderCommit is higher and we have
		// the same amount of logs we apply otherwise outOfSync
	case m.PrevLog.Term == req.PrevLog.Term &&
		m.PrevLog.Index == req.PrevLog.Index &&
		req.PrevLog.LeaderCommit > m.PrevLog.LeaderCommit:
		// checks we have the same amount of logs
		latestCommit, ok := m.applyLogsTill(req.PrevLog.LeaderCommit)
		if !ok {
			reply.Status = StatusOutOfSyncLogs
			reply.LogProperties = m.PrevLog
		} else {
			reply.Status = StatusLogsMatch
			reply.LogProperties.LeaderCommit = latestCommit
		}
	}
	return reply
}


func (m *Machine) applyLogsTill(commitIdx int) (int, bool) {
	// this should not really happend given the saftey checks were
	// applied correctly? so if the number of logs we have is
	//  less than the newCommitIdx we should warn
	return commitIdx, true
}

func (m *Machine) Commit(l *Entry) {
	m.DB.Commit(fmt.Sprintf("%+v", l))
}
