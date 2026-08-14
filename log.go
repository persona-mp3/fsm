package main

import (
	"fmt"
	db "fsm/database"
	"strings"
	"sync"
	"sync/atomic"
)

type Entry struct {
	Idx       int
	Term      uint64
	Operation db.Operation
	Key       string
	Value     string
}

type Logs struct {
	rw               sync.Mutex
	entries          []*Entry
	lastCommited     *atomic.Uint64
	PreviousLogIndex *atomic.Uint64
	size             int
}

func NewLogs() Logs {
	return Logs{
		lastCommited:     &atomic.Uint64{},
		PreviousLogIndex: &atomic.Uint64{},
	}
}

func (l *Logs) Append(e *Entry) int {
	l.rw.Lock()
	defer l.rw.Unlock()

	idx := len(l.entries)
	l.PreviousLogIndex.Store(uint64(idx))
	e.Idx = idx
	l.entries = append(l.entries, e)
	return idx
}

// should we get this to return an atomic pointer
func (l *Logs) LastCommited() uint64 {
	return l.lastCommited.Load()
}

func (l *Logs) getAtomicCommit() *atomic.Uint64 {
	return l.lastCommited
}

func (l *Logs) Size() int {
	l.rw.Lock()
	defer l.rw.Unlock()
	return len(l.entries)
}

func (l *Logs) HasEntry(entry *Entry) bool {
	l.rw.Lock()
	defer l.rw.Unlock()
	for _, e := range l.entries {
		if e.Term == entry.Term &&
			e.Operation == entry.Operation &&
			e.Key == entry.Key &&
			e.Value == entry.Value {
			return true
		}
	}
	return false
}

// todo: will want to do this in reverse instead
func (l *Logs) Get(ops db.Operation, key string) (string, bool) {
	l.rw.Lock()
	defer l.rw.Unlock()
	for _, e := range l.entries {
		if e.Operation == ops && e.Key == key {
			return e.Value, true
		}
	}

	return "", false
}

func (l *Logs) String() string {
	l.rw.Lock()
	defer l.rw.Unlock()

	sb := strings.Builder{}
	for _, e := range l.entries {
		fmt.Fprintf(&sb, "%+v, ", e)
	}

	return sb.String()
}

// we assume that the stopCommit is the exact index of the last log the leader applied to it's
// database, so we can just search for that.
// NOTE: We might also need the *TERM* of the commmit not sure yet but since the commit only
// ever increases, ie it syncs with the term, and this nodes [PreviousLogIndex] matches with
// the leader  it should not be a problem or need
func (l *Logs) FlushTill(stopCommit uint64, jkvs db.Database) error {
	lastCommited := l.lastCommited.Load()
	// apply all logs till stopCommit,
	// check if we have up to size stopCommitLogs
	if len(l.entries) < int(stopCommit) {
		println("beuaurusobv")
		panic(
			fmt.Sprintf(
				`recvd a stopCommit greater than the amount of logs stored locally
	       allLogs: %+v, 
	       logSize: %+v, 
	       stopCommit: %+v,
	   `,
				l.String(),
				len(l.entries),
				stopCommit,
			),
		)
	}

	l.rw.Lock()
	defer l.rw.Unlock()

	// CURRENTLY:
	// At this point, we are to apply these to the database. A docker application will need to be
	// setup so the behaviour can be tested appropriately.
	fmt.Println("flushing------")
	for idx := lastCommited; idx < stopCommit; idx++ {
		entry := l.entries[idx]
		fmt.Println()
		fmt.Printf("(%d) %+v\n\n", idx, entry)
		res, err := jkvs.Commit(db.Command{
			Operation: entry.Operation,
			Key:       entry.Key,
			Value:     entry.Value,
		})
		l.lastCommited.Add(1)
		if err != nil {
			fmt.Printf("could not apply: (%d). Reason: %s\n", idx, err)
			continue
		}

		fmt.Printf("(%d) response: %+v\n", idx, res)
	}

	fmt.Println("-------flushed")
	return nil
}

func (e *Entry) String() string {
	return fmt.Sprintf(
		"Entry: { Idx: %d, Operation: %s, Key: %s, Value: %s }",
		e.Idx, e.Operation, e.Key, e.Value,
	)
}
