package main

import (
	"context"
	"flag"
	"fmt"
	"fsm/dock"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"sync"
	"syscall"

	"os"
	"time"

	"github.com/BurntSushi/toml"
)

var clusterConfig = "cluster-config.toml"
var topology = "cluster"

var (
	// heartbeatInterval is the rate at which the node when in a [Leader] state sends
	// out heartbeats to follower in a cluster. At the moment, this is set to be 200 which
	// is roughly half the minimum election timeout interval
	heartbeatInterval = time.Millisecond * 200

	// According to the Raft Paper, it's recommended for timeouts(election) to range from 100-500ms, but
	// we're increasing it because that's too aggressive
	minInterval = 500
	maxInterval = 1200
)

func main() {
	if err := parseConfig(); err != nil {
		log.Println(err)
	}
}

func parseConfig() error {
	flag.StringVar(&topology, "topology", topology, "type of topology")
	flag.StringVar(&clusterConfig, "config", clusterConfig, "path to cluster configuration file")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGKILL)
	defer cancel()

	switch topology {
	case "cluster":
		cfg := dock.SingleClusterConfig{}

		meta, err := toml.DecodeFile(clusterConfig, &cfg)
		if err != nil {
			fmt.Printf("undecoded-keys: %+v\n", meta.Undecoded())
			return fmt.Errorf("could not parse cluster config: %w", err)
		}

		minInterval = cfg.ClusterSettings.MinInterval
		maxInterval = cfg.ClusterSettings.MaxInterval
		heartbeatInterval = time.Millisecond * time.Duration(cfg.ClusterSettings.Heartbeat)

		log.Println("running cluster in 'cluster' mode with total of", len(cfg.Peers), cfg.Peers)
		go func() {
			log.Printf("pprof server running on: http://%s/debug/pprof/", cfg.PprofAddr)
			if err := http.ListenAndServe(cfg.PprofAddr, nil); err != nil {
				log.Println("failed to start pprof server: ", err)
				return
			}
		}()

		if err := runClusterTopology(ctx, &cfg); err != nil {
			fmt.Printf("%+v\n", err)
			os.Exit(1)
		}

	case "node":
		cfg := dock.NodeConfig{}
		meta, err := toml.DecodeFile(clusterConfig, &cfg)
		if err != nil {
			fmt.Printf("undecoded-keys: %+v\n", meta.Undecoded())
			return fmt.Errorf("could not parse cluster config: %w", err)
		}

		go func() {
			if err := http.ListenAndServe(cfg.PprofAddr, nil); err != nil {
				log.Println("failed to start pprof server: ", err)
				return
			}
		}()

		minInterval = cfg.ClusterSettings.MinInterval
		maxInterval = cfg.ClusterSettings.MaxInterval
		heartbeatInterval = time.Millisecond * time.Duration(cfg.ClusterSettings.Heartbeat)

		runSingleTopology(ctx, &cfg)

	case "isolated":
		return fmt.Errorf("cannot run an isolated config yet")
	default:
		return fmt.Errorf("unsupported topology %s", topology)
	}

	return nil
}

func runSingleTopology(ctx context.Context, cfg *dock.NodeConfig) {
	fmt.Println("peers:", cfg.Peers)
	out, err := os.Create(fmt.Sprintf("log-file-%d", cfg.Id))
	if err != nil {
		log.Println("could not create logFile for node", cfg.Id)
		out = nil
	}
	node, err := NewNode(fmt.Sprintf("%d", cfg.Id), cfg.Listen, cfg.Peers, out)
	if err != nil {
		log.Println(err)
		return
	}

	nodeCtx, nodeCancel := context.WithCancel(ctx)
	defer nodeCancel()

	done := make(chan struct{})
	go func() {
		err := node.Run(nodeCtx)
		if err != nil {
			close(done)
			log.Println(err)
			return
		}
	}()

	select {
	case <-ctx.Done():
		return
	case <-done:
		return
	}
}

func runClusterTopology(ctx context.Context, cfg *dock.SingleClusterConfig) error {
	nodes := []*Node{}
	for idx, addr := range cfg.Peers {
		id := fmt.Sprintf("%d", idx+1)
		remove := idx
		peers := append([]string{}, cfg.Peers[:remove]...)
		peers = append(peers, cfg.Peers[remove+1:]...)

		out, err := os.Create(fmt.Sprintf("log-file-%s", id))
		if err != nil {
			log.Println("could not create logFile for node", id)
			out = nil
		}
		node, err := NewNode(id, addr, peers, out)

		nodes = append(nodes, node)
		if err != nil {
			log.Fatal(err)
		}

	}
	wg := sync.WaitGroup{}
	nodeCtx, nodeCancel := context.WithCancel(ctx)
	defer nodeCancel()

	for _, node := range nodes {
		wg.Go(func() {
			if err := node.Run(nodeCtx); err != nil {
				log.Println(err)
				return
			}
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("all raftNodes have died")
		return nil
	case <-ctx.Done():
		log.Println("parentCtx cancelled first, killing all rafts")
		return nil
	}
}
