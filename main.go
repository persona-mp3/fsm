package main

import (
	"context"
	"fmt"
	"fsm/dock"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

const (
	DebugAddr = "localhost:6061"
)

func main() {
	// cluster, err := parseConfig("cluster_config.toml")
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// ticker := time.NewTicker(2 * time.Second)
	// defer ticker.Stop()
	//
	// go func() {
	// 	f, err := os.Create("goroutinetrack")
	// 	if err != nil {
	// 		log.Println("could not create log file goroutinetrack", err)
	// 		f = os.Stdout
	// 	}
	// 	defer f.Close()
	//
	// 	log.SetOutput(f)
	// 	for t := range ticker.C {
	// 		_ = t
	// 		log.Printf("GOROUTINES::::: %d\n", runtime.NumGoroutine())
	// 	}
	// }()
	//
	// // TODO: Would want this to be by default, maybe later we could add flags to use trace.
	// // Not sure if this server could cost the application but it should me next to nothing
	// go func() {
	// 	if err := http.ListenAndServe(DebugAddr, nil); err != nil {
	// 		fmt.Println("could not start ptrace server::", err)
	// 	}
	// }()
	// ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	// defer cancel()
	//
	// err = cluster.Start(ctx)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	clusterConfig, err := readConfig()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGKILL)
	defer cancel()
	switch clusterConfig.ClusterSettings.Mode {

	case dock.ModeSingle:
		fmt.Println("running cluster in single mode")
		runSingleMode(ctx, clusterConfig)

	case dock.ModeCluster:
		fmt.Println("running cluster in cluster mode")

	case dock.ModeIsolated:
		fmt.Println("running cluster in isolated mode")

	default:
		panic(fmt.Sprintf("unexpected cluster Mode: %#v", clusterConfig.ClusterSettings.Mode))
	}
}

func runSingleMode(ctx context.Context, cfg *dock.NodeConfig) {
	go func() {
		if err := http.ListenAndServe(cfg.PprofAddr, nil); err != nil {
			log.Println("failed to start pprof server: ", err)
			return
		}
	}()

  fmt.Println("peers:", cfg.Peers)
	node, err := NewNode(fmt.Sprintf("%d", cfg.Id), cfg.Listen, cfg.Peers, os.Stdout)
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
func runClusterMode(cfg dock.NodeConfig) {
}
