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

	rlog "fsm/raftlogger"
	"github.com/BurntSushi/toml"
)

type Cluster struct {
	// Addresses are ip addresses for the nodes to start.
	// Defaults are localhost:4000 to 4002.
	Addresses []string

	raftNodes []*Node

	log rlog.RLogger
}

func DefaultCluster() *Cluster {
	l := rlog.NewHumaneLogger("0", "cluster", 0, os.Stdout)
	addrs := []string{
		"localhost:4000",
		"localhost:4001",
		"localhost:4002",
	}

	raftNodes := []*Node{}

	for i, addr := range addrs {
		serverAddr, peers := filterAddr(addr, addrs)
		n, err := NewNode(fmt.Sprintf("%d", i+1), serverAddr, peers, nil)
		if err != nil {
			l.Println("could not create node with addr: ", serverAddr, err)
			continue
		}

		raftNodes = append(raftNodes, n)
	}

	return &Cluster{
		Addresses: addrs,
		raftNodes: raftNodes,
		log:       l,
	}
}

func (c *Cluster) Start(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(len(c.raftNodes))

	for i := range len(c.raftNodes) {
		node := c.raftNodes[i]
		wg.Add(1)
		go func(ctx context.Context, node *Node) {
			defer wg.Done()
			if err := node.Run(ctx); err != nil {
				log.Println(err)
			}
		}(ctx, node)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
		c.log.Println("all raftNodes have died")
		return nil
	case <-parentCtx.Done():
		c.log.Println("parentCtx cancelled first, killing all rafts")
		return nil
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
		fmt.Println("using default cluster config")
		return DefaultCluster(), nil
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not load config. %w ", err)
	}

	cfg := &Cluster{}
	if _, err := toml.Decode(string(content), cfg); err != nil {
		fmt.Println("could not parse config file: ", err)
		fmt.Println("using defaults")
		cfg = DefaultCluster()
	}

	raftNodes := []*Node{}

	for i, addr := range cfg.Addresses {
		serverAddr, peers := filterAddr(addr, cfg.Addresses)
		n, err := NewNode(fmt.Sprintf("%d", i+1), serverAddr, peers, nil)
		if err != nil {
			log.Println("could not create node with addr: ", serverAddr, err)
			continue
		}

		raftNodes = append(raftNodes, n)
	}

	cfg.raftNodes = raftNodes
	l := rlog.NewHumaneLogger("0", "cluster", 0, os.Stdout)
	cfg.log = l

	return cfg, nil

}

const (
	// According to the Raft Paper, it's recommended for timeouts(election) to range from 100-500ms, but
	// we're increasing it because that's too aggressive
	minInterval = 400
	maxInterval = 1500

	// heartbeatInterval is the rate at which the node when in a [Leader] state sends
	// heartbeats to the followers in the cluster. This value is hardcoded right now
	// because the intervals for elections is always randomised between 100-500ms
	// 10ms makes it very unlikely that a [Leader] doesn't delay in sending a heartbeat RPC
	heartbeatInterval = time.Millisecond * 50
)

func randomTimeout(d time.Duration) time.Duration {
	n := rand.IntN(maxInterval-minInterval) + minInterval

	return d * time.Duration(n)
}
