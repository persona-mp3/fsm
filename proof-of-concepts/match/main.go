package main

import (
	"errors"
	"sync"
)

var (
	ErrorIndexOutOfBound = errors.New("index out of bounds")
)

type Log struct {
	idx   int
	term  uint64
	key   string
	value string
}

type Store struct {
	mu            sync.Mutex
	logs          []Log
	leader_commit int
}

func main() {

}

func NewStore() *Store {
	return &Store{}
}

func (s *Store) get_previous_log_index() Log {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logs[len(s.logs)-1]
}
func (s *Store) get_leader_commit() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leader_commit
}

func (s *Store) snapshot_from(idx int) ([]Log, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// NOTE: should return a copy instead? as a slice just points to the og one
	if idx >= len(s.logs) {
		return []Log{}, ErrorIndexOutOfBound
	}
	// NOTE: The idx should match the actual index in the logs
	log_snapshot := s.logs[idx:]
	return log_snapshot, nil
}
