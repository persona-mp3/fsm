package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	rlog "fsm/raftlogger"
	"github.com/BurntSushi/toml"
)

type Cluster struct {
	// Addresses are ip addresses for the nodes to start.
	// Defaults are localhost:4000 to 4002.
	Addresses []string

	raftNodes []*Node

	// Single decides whether this cluster should only
	// run a single node. This was introduced to allow running
	// nodes in isolated containers.
	Single bool
	log    rlog.RLogger
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
		// TODO: create log files for each node
		nodeId := fmt.Sprintf("%d", i+1)
		logdest := fmt.Sprintf("node-log-%s", nodeId)
		logfile, err := os.Create(logdest)
		if err != nil {
			l.Println("could not create logFile, using stdout as defaults. reason: ", err)
			logfile = nil
		}

		l.Println(fmt.Sprintf("using: logfile_%v for node.%s\n", logfile, nodeId))
		n, err := NewNode(nodeId, serverAddr, peers, logfile)
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

	if c.Single {
		c.log.Println("running single-mode cluster")
		node := c.raftNodes[0]
		wg.Go(func() {
			err := node.Run(ctx)
			if err != nil {
				c.log.Println("while in single-mode: %w", err)
				return
			}
		})
	} else {
		for i := range len(c.raftNodes) {
			node := c.raftNodes[i]
			wg.Add(1)
			go func(ctx context.Context, node *Node) {
				defer func() {
					wg.Done()
				}()
				if err := node.Run(ctx); err != nil {
					c.log.Println("node error: ", err)
					return
				}
			}(ctx, node)
		}
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

		nodeId := fmt.Sprintf("%d", i+1)
		logfileName := fmt.Sprintf("log-file-%s", nodeId)

		logfile, err := os.Create(logfileName)
		if err != nil {
			log.Println("could not create logFile, using stdout as defaults. reason: ", err)
			logfile = nil
		}

		n, err := NewNode(fmt.Sprintf("%d", i+1), serverAddr, peers, logfile)
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
