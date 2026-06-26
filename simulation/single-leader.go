package main

import (
	"context"
	"fmt"
	rlog "fsm/raftlogger"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
)

type AppendEntryRequest struct {
	Id      string
	Term    uint64
	Message string
}

type AppendEntryReply struct {
	Id      string
	Term    uint64
	Acked   bool
	Message string
}

const (
	defaultSimulationConfigPath = "cluster_config.toml"
)

type Simulation struct {
	// TotalNodes is the total number of nodes that the cluster will start up. Default is 3
	TotalNodes int

	// Addresses contains ip addresses for where each node will start. Defaults are all
	// on localhost, from port 4000 to 4002.
	Addresses []string

	// TODO: ForceTerm  needs to be higher than the term a node or cluster is in
	ForceTerm uint64

	ForceDurationMs int

	log rlog.RLogger

	// TODO: Pesists tells the simulation to increment the term till it get's accepted
	Persist bool
}

func main() {
	sim, err := parseConfig("")
	if err != nil {
		fmt.Println(err)
		return
	}

	sim.Run()
}

func DefaultSimulation() *Simulation {
	l := rlog.NewHumaneLogger("0", "simulation", 0, os.Stdout)
	addrs := []string{
		"localhost:4000",
	}

	totalNodes := 3

	return &Simulation{
		TotalNodes: totalNodes,
		Addresses:  addrs,
		log:        l,
	}
}

func parseConfig(path string) (*Simulation, error) {
	if len(strings.ReplaceAll(path, " ", "")) == 0 {
		path = defaultSimulationConfigPath
		fmt.Printf("using default cluster config for simulation\n\n")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not load config. %w ", err)
	}

	cfg := DefaultSimulation()
	if _, err := toml.Decode(string(content), cfg); err != nil {
		fmt.Println("could not parse config file: ", err)
		fmt.Println("using defaults")
		cfg = DefaultSimulation()
	}

	return cfg, nil

}

const defaultForceTimeInterval = time.Millisecond * 10

func (sc *Simulation) Run() {
	ctx, cancel := signal.NotifyContext(context.Background())
	defer cancel()

	sc.log.Println("", sc)

	wg := sync.WaitGroup{}
	for idx := range sc.TotalNodes {
		at := sc.Addresses[idx]
		dial, err := rpc.Dial("tcp", at)
		if err != nil {
			sc.log.Println("could not dial", at, err)
			continue
		}

		wg.Add(1)
		go func(dial *rpc.Client) {
			defer wg.Done()
			sc.sendAppendEntries(ctx, dial, sc.ForceTerm, defaultForceTimeInterval)
		}(dial)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	<-done

}

func (sc *Simulation) sendAppendEntries(ctx context.Context, d *rpc.Client, forceTerm uint64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			req := AppendEntryRequest{
				Id:      "single-leader.go",
				Term:    forceTerm,
				Message: "simulation sending appendEntries to enforce leadership",
			}
			res := &AppendEntryReply{}
			if err := d.Call("Server.AppendEntryRPC", req, res); err != nil {
				sc.log.Println("could not call call service", err)
				return
			}

			if !res.Acked {
				sc.log.Println("refused to ack:", res)
				return
			}
			sc.log.Println("", res)
			ticker.Reset(interval)
		}
	}
}
