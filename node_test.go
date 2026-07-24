package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultStateIsFollower(t *testing.T) {
	initialTimeout := randomTimeout(time.Millisecond)

	node, _ := NewNode(t.Name(), "test-addr", []string{}, initialTimeout, t.Output())
	assert.Equal(t, Follower, node.raft.State())
}
