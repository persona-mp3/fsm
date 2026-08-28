package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"time"
)

func randomTimeout(d time.Duration) time.Duration {
	// crypto/rand requires a *big.Int for limits
	limit := big.NewInt(int64(MaxInterval - MinInterval + 1))
	n, err := rand.Int(rand.Reader, limit)
	if err != nil {
		log.Println("warning:: random generator returned 1", n, err)
	}

	actualInterval := n.Int64() + int64(MinInterval)
	return d * time.Duration(actualInterval)
}

func backgroundSendCh[T any](parentCtx context.Context, ch chan T, data T) {
	ctx, cancel := context.WithTimeout(parentCtx, SEND_TIMEOUT)
	go func() {
		defer cancel()
		select {
		case ch <- data:
		case <-ctx.Done():
			fmt.Printf("[backgroundSendCh] timeout for sending reached: %s, %+v\n", SEND_TIMEOUT, ctx.Err())
			return
		}
	}()
}

func clearScreen() {
}
