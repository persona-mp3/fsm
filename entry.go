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
	rw           sync.RWMutex
	entries      []*Entry
	lastCommited *atomic.Uint64
	size         int
}

func (l *Logs) Append(e *Entry) int {
	l.rw.Lock()
	defer l.rw.Unlock()
	idx := len(l.entries)
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
	l.rw.RLock()
	defer l.rw.RUnlock()
	return len(l.entries)
}

func (l *Logs) HasEntry(entry *Entry) bool {
	l.rw.RLock()
	defer l.rw.RUnlock()
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
	l.rw.RLock()
	defer l.rw.RUnlock()
	for _, e := range l.entries {
		if e.Operation == ops && e.Key == key {
			return e.Value, true
		}
	}

	return "", false
}

func (l *Logs) String() string {
	l.rw.RLock()
	defer l.rw.RUnlock()

	sb := strings.Builder{}
	for _, e := range l.entries {
		fmt.Fprintf(&sb, "%+v, ", e)
	}

	return sb.String()
}
