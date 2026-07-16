package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"time"
)

const (
	DebugAddr = "localhost:6061"
)

func main() {
	cluster, err := parseConfig("cluster_config.toml")
	if err != nil {
		fmt.Println(err)
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	go func() {
		f, err := os.Create("goroutinetrack")
		if err != nil {
			log.Println("could not create log file goroutinetrack", err)
			f = os.Stdout
		}
		defer f.Close()

		log.SetOutput(f)
		for t := range ticker.C {
			_ = t
			log.Printf("GOROUTINES::::: %d\n", runtime.NumGoroutine())
		}
	}()

	// TODO: Would want this to be by default, maybe later we could add flags to use trace.
	// Not sure if this server could cost the application but it should me next to nothing
	go func() {
		if err := http.ListenAndServe(DebugAddr, nil); err != nil {
			fmt.Println("could not start ptrace server::", err)
		}
	}()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	err = cluster.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
