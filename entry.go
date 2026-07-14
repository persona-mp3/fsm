package main

import (
	"fmt"
  db "fsm/database"
	"strings"
	"sync"
)

type Entry struct {
	Idx       int
	Term      uint64
	Operation db.Operation
	Key       string
	Value     string
}

type Logs struct {
	rw             sync.RWMutex
	entries        []*Entry
	latestCommited int
}

func (l *Logs) Append(e *Entry) int {
	l.rw.Lock()
	defer l.rw.Unlock()
	idx := len(l.entries)
	e.Idx = idx
	l.entries = append(l.entries, e)
  return idx
}

func (l *Logs) Contains(idx int, term uint64) bool {
	l.rw.RLock()
	defer l.rw.RUnlock()

	if len(l.entries) <= idx {
		return false
	}

	target := l.entries[idx]
	if target.Term == term {
		return true
	}

	return false
}

func (l *Logs) HasEntry(entry *Entry) bool {
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

func (l *Logs) String() string {
	l.rw.RLock()
	defer l.rw.RUnlock()

	sb := strings.Builder{}
	for _, e := range l.entries {
		fmt.Fprintf(&sb, "%+v, ", e)
	}

	return sb.String()
}
