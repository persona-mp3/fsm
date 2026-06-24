package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Cluster struct {
	// current number of nodes running
	activeNodes int

	// addreses of all nodes in the cluster
	nodeAddresses []string

	// raftNodes are the nodes that are currently active in the cluster.
	raftNodes []*Raft

	log *log.Logger
}

func NewCluster(totalNodes int, opts *Opts) *Cluster {
	var logger *log.Logger
	if opts == nil {
		logger = &defaultOpts().log
		logger.SetPrefix("(cluster) ")
	} else {
		logger = &opts.log
	}

	peers := []string{}
	raftNodes := []*Raft{}

	for id := range totalNodes {
		electionTimeout := randomTimeout(time.Millisecond)
		addr := randomListenAddr()
		node := NewRaft(fmt.Sprintf("%d", id), addr, []string{}, electionTimeout, nil)

		peers = append(peers, addr)
		raftNodes = append(raftNodes, node)

	}

	for _, node := range raftNodes {
		for _, addr := range peers {
			if addr != node.serverAddr {
				node.peers = append(node.peers, addr)
			}
		}
	}

	return &Cluster{
		activeNodes: totalNodes,
		raftNodes:   raftNodes,
		log:         logger,
	}
}

func (c *Cluster) Start(parentCtx context.Context) {
	c.log.Println("initialising cluster with totalNodes:", c.activeNodes)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(c.activeNodes)

	for _, node := range c.raftNodes {
		go func(ctx context.Context, node *Raft) {
			defer wg.Done()
			c.log.Println("started node with id:", node.id)
			if err := node.Run(ctx); err != nil {
				log.Println(err)
			}
		}(ctx, node)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		c.log.Println("wg counter returned, sending to done chan")
		done <- struct{}{}
	}()

	select {
	case <-done:
		c.log.Println("all raftNodes have died")
		return
	case <-parentCtx.Done():
		c.log.Println("parentCtx cancelled first, killing all rafts")
		return
	}
}

const (
	minPort = 1024
	maxPort = 65535
)

// generates a localhost address to listen on. It is not garaunteed that
// the address has no process already bound or listening on it
func randomListenAddr() string {
	port := rand.Intn(maxPort-minPort) + minPort
	return fmt.Sprintf("127.0.0.1:%d", port)
}
