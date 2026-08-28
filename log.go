package main

import (
	"errors"
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
	mu               sync.Mutex
	entries          []*Entry
	lastCommited     *atomic.Uint64
	PreviousLogIndex *atomic.Uint64
	size             int
}

func NewLogs() Logs {
	return Logs{
		mu:               sync.Mutex{},
		lastCommited:     &atomic.Uint64{},
		PreviousLogIndex: &atomic.Uint64{},
	}
}

var ErrLogNotFound = errors.New("log with index not found")

// Append adds the entry to it's stored logs, and returns the previous logs index
func (l *Logs) Append(e *Entry) int {
	l.mu.Lock()
	defer l.mu.Unlock()

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
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

func (l *Logs) HasEntry(entry *Entry) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
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
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		if e.Operation == ops && e.Key == key {
			return e.Value, true
		}
	}

	return "", false
}

func (l *Logs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	sb := strings.Builder{}
	for _, e := range l.entries {
		fmt.Fprintf(&sb, "%+v, ", e)
	}

	return sb.String()
}

// TODO: We might also need the *TERM* of the commmit not sure yet but since the commit only
// ever increases, ie it syncs with the term, and this nodes [PreviousLogIndex] matches with
// the leader  it should not be a problem or need

// FlushTill applies all the entries from the last commited entry index, up till stopCommit
// If the stopCommit is greater than the amount of logs, FlushTill panics
func (l *Logs) FlushTill(stopCommit uint64, jkvs db.Database) {
	lastCommited := l.lastCommited.Load()
	// apply all logs till stopCommit,
	// check if we have up to size stopCommitLogs
	if len(l.entries) < int(stopCommit) {
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

	l.mu.Lock()
	defer l.mu.Unlock()

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
}

func (e *Entry) String() string {
	return fmt.Sprintf(
		"Entry: { Idx: %d, Operation: %s, Key: %s, Value: %s }",
		e.Idx, e.Operation, e.Key, e.Value,
	)
}

var (
	ErrNotEnoughLogs = errors.New("Not enough logs")
)

// GetPreviousLogEntry returns the previousLogEntry and the current log size. If the
// If there are not enough logs stored, it returns an [ErrNotEnoughLogs]. If there is only one
// log stored, it sends an empty log and does not return an error
func (l *Logs) GetPreviousLogEntry() (Entry, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	logSize := len(l.entries)
	if logSize == 0 {
		return Entry{}, logSize
	}

	if logSize == 1 {
		return Entry{}, logSize
	}
	previousLogEntry := l.entries[logSize-2]
	return *previousLogEntry, logSize
}

// Snapshot returns a copy of all the logs currently stored
func (l *Logs) Snapshot() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()

	buff := make([]Entry, len(l.entries))
	for _, entry := range l.entries {
		clone := *entry
		buff = append(buff, clone)
	}
	return buff
}

// SnapshotFrom returns a slice of logs that  start from `startIndex` provided. If the logs
// are not up to that index, it returns an [ErrLogNotFound]
func (l *Logs) SnapshotFrom(startIndex, targetTerm uint64) ([]Entry, error) {
	// TODO: Should ideally be SnapshotFrom(startIndex, startTerm)
	l.mu.Lock()
	defer l.mu.Unlock()
	if startIndex > uint64(len(l.entries)) {
		return []Entry{}, ErrLogNotFound
	}

	rest := uint64(len(l.entries)) - startIndex
	matchingEntry := l.entries[startIndex]
	// 	TODO|QUESTION| revisit this!
	// 	In Raft, the leader handles inconsistencies by forcing
	// the followers’ logs to duplicate its own. This means that
	// conflicting entries in follower logs will be overwritten
	// with entries from the leader’s log. Section 5.4 will show
	// that this is safe when coupled with one more restriction.
	// To bring a follower’s log into consistency with its own,
	// the leader must find the latest log entry where the two
	// logs agree, delete any entries in the follower’s log after
	// that point, and send the follower all of the leader’s entries
	// after that point. All of these actions happen in response
	// to the consistency check performed by AppendEntries
	// RPCs. The leader maintains a nextIndex for each follower,
	// which is the index of the next log entry the leader will
	// send to that follower
	if matchingEntry.Term != targetTerm {
		fmt.Printf(`
		[logs] found a log with specified index, but their terms don't match. Follower should 
		overwrite with ours. RaftResultLogNotFound
		`)
	}

	buff := make([]Entry, rest)
	for i := startIndex; i < rest; i++ {
		clone := *l.entries[i]
		buff = append(buff, clone)
	}
	return buff, nil
}
