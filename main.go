package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/trace"
)

func main() {
	const stubTotalNodes = 4
	cluster := NewCluster(stubTotalNodes, nil)

	go func() {
		f, _ := os.Create("trace.out")
		trace.Start(f)
		defer trace.Stop()

		if err := http.ListenAndServe("localhost:6061", nil); err != nil {
			log.Panicln("could not run tacer::", err)
		}

	}()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cluster.Start(ctx)
}
