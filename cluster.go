package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
)

type Cluster struct {
	// TotalNodes is the total number of nodes that the cluster will start up. Default
	// is 3
	TotalNodes int

	// Addresses contains ip addresses for where each node will start. Defaults are all
	// on localhost, from port 4000 to 4002.
	Addresses []string

	raftNodes []*Node

	log *log.Logger
}

func DefaultCluster() *Cluster {
	addrs := []string{
		"localhost:4000",
		"localhost:4001",
		"localhost:4002",
	}

	totalNodes := 3
	raftNodes := []*Node{}

	for i, addr := range addrs {
		serverAddr, peers := filterAddr(addr, addrs)
		n, err := NewNode(fmt.Sprintf("%d", i+1), serverAddr, peers)
		if err != nil {
			fmt.Println("could not create node with addr: ", serverAddr, err)
			totalNodes -= 1
			continue
		}

		raftNodes = append(raftNodes, n)
	}

	clog := log.New(os.Stdout, "[cluster] ", log.Lmsgprefix)
	return &Cluster{
		TotalNodes: totalNodes,
		Addresses:  addrs,
		raftNodes:  raftNodes,
		log:        clog,
	}
}

func (c *Cluster) Start(parentCtx context.Context) {
	c.log.Println("initialising cluster with totalNodes: ", c.TotalNodes)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(c.TotalNodes)

	for _, node := range c.raftNodes {
		go func(ctx context.Context, node *Node) {
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

func filterAddr(addr string, others []string) (string, []string) {
	peers := []string{}
	for _, peer := range others {
		if peer != addr {
			peers = append(peers, peer)
		}
	}

	return addr, peers
}

const (
	defaultClusterConfigPath = "cluster_config.toml"
)

func parseConfig(path string) (*Cluster, error) {
	if len(strings.ReplaceAll(path, " ", "")) == 0 {
		path = defaultClusterConfigPath
		fmt.Println("  using default cluster config")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not load config. %w ", err)
	}

	cfg := DefaultCluster()
	if _, err := toml.Decode(string(content), cfg); err != nil {
		fmt.Println("could not parse config file: ", err)
		fmt.Println("using defaults")
		cfg = DefaultCluster()
	}

	return cfg, nil

}

const (
	// According to the Raft Paper, it's recommended for timeouts(election) to range from 100-500ms
	minInterval = 100
	maxInterval = 500

	// heartbeatInterval is the rate at which the node when in a [Leader] state sends
	// heartbeats to the followers in the cluster. This value is hardcoded right now
	// because the intervals for elections is always randomised between 100-500ms
	// 10ms makes it very unlikely that a [Leader] doesn't delay in sending a heartbeat RPC
	heartbeatInterval = time.Millisecond * 10
)

func randomTimeout(d time.Duration) time.Duration {
	n := rand.IntN(maxInterval-minInterval) + minInterval

	return d * time.Duration(n)
}
