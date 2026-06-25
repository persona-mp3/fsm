package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
)

func main() {
	cluster, err := parseConfig("")
	if err != nil {
		fmt.Println(err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cluster.Start(ctx)
}
