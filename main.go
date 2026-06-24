package main

import (
	"context"
	"os"
	"os/signal"
)

func main() {
	const stubTotalNodes = 3
	cluster := NewCluster(stubTotalNodes, nil)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cluster.Start(ctx)
}
