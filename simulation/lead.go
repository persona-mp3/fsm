package main

import (
	"context"
	"fmt"
	"fsm/dock"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
)

type Leader struct {
	ForceTerm int  `toml:"force_term"`
	Rounds    int  `toml:"rounds"`
	Persist   bool `toml:"persist"`
}

type TestConfig struct {
	Peers           []string             `toml:"peers"`
	Leader          Leader               `toml:"leader"`
	ClusterSettings dock.ClusterSettings `toml:"cluster_settings"`
}

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

var testTomlPath = "test.toml"

const (
	GreenEscapeCode = "\033[32m"
	RedEscapeCode   = "\033[31m"
	ResetEscapeCode = "\033[0m"
)

func main() {
	cfg, err := parseTestConfig()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("running leader simulation with config:", cfg.String())
	if err := runLeader(cfg); err != nil {
		fmt.Println(err)
	}
}

func runLeader(cfg *TestConfig) error {
	rootCtx := context.Background()

	ctx, stop := signal.NotifyContext(
		rootCtx,
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	responses := make(chan AppendEntryReply, len(cfg.Peers)*cfg.Leader.Rounds)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var wg sync.WaitGroup

	for _, addr := range cfg.Peers {
		conn, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("could not dial:", addr, err)
			continue
		}

		wg.Add(1)

		go func(addr string, rpcConn *rpc.Client) {
			defer wg.Done()
			if err := cfg.sendAppendEntries(workerCtx, rpcConn, responses); err != nil {
				fmt.Println(err)
			}

		}(addr, conn)
	}

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	allResponses := []AppendEntryReply{}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("shutdown received")
			return nil

		case res := <-responses:
			allResponses = append(allResponses, res)

		case <-done:
			fmt.Println("all workers finished")
			goto PROCESS
		}
	}

PROCESS:
	fmt.Println("processing node responses ...", len(allResponses))

	expectedTerm := uint64(cfg.Leader.ForceTerm)

	passed := 0
	failed := 0

	for _, res := range allResponses {
		if res.Term != expectedTerm {
			fmt.Printf(
				"node %s returned different term. Expected: %d Got: %d payload: %+v\n",
				res.Id,
				expectedTerm,
				res.Term,
				res.Message,
			)
			failed++
			continue
		}

		if !res.Acked {
			fmt.Printf("node refused Ack. Payload: %+v\n", res)
			failed++
			continue
		}

		fmt.Printf("expected response met passed %+v\n", res)
		passed++
	}

	fmt.Printf("total passed:%s %d %s\n",
		GreenEscapeCode, passed, ResetEscapeCode,
	)
	fmt.Printf("total failed:%s %d %s\n\n",
		RedEscapeCode, failed, ResetEscapeCode,
	)

	return nil
}

func parseTestConfig() (*TestConfig, error) {
	cfg := TestConfig{}
	meta, err := toml.DecodeFile(testTomlPath, &cfg)
	if err != nil {
		return nil, fmt.Errorf("could not parse config. %w\n unparsed keys: %s", err, meta.Undecoded())
	}

	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("cannot test on empty peers")
	}

	if cfg.Leader.ForceTerm == 0 {
		return nil, fmt.Errorf("force-term cannot be 0")
	}

	return &cfg, nil
}

func (cfg *TestConfig) sendAppendEntries(ctx context.Context, conn *rpc.Client, responses chan<- AppendEntryReply) error {
	heartbeatInterval := time.Millisecond * time.Duration(cfg.ClusterSettings.Heartbeat)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	roundsDone := 0
	for {
		if roundsDone == cfg.Leader.Rounds {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:

			req := AppendEntryRequest{
				Id:      "sim/leader.go",
				Term:    uint64(cfg.Leader.ForceTerm),
				Message: "simulation sending appendEntries to enforce leadership",
			}
			res := &AppendEntryReply{}
			if err := conn.Call("Server.AppendEntryRPC", req, res); err != nil {
				fmt.Println("could not call call service", err)
				return nil
			}
			responses <- *res
			roundsDone++
			ticker.Reset(heartbeatInterval)
		}
	}
}

func (cfg *TestConfig) String() string {
	heartbeat := time.Millisecond * time.Duration(cfg.ClusterSettings.Heartbeat)
	return fmt.Sprintf("peers: %+v, heartbeat: %s\n", cfg.Peers, heartbeat)
}
