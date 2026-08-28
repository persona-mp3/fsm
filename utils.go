package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"log/slog"
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

func debugClearScreen() {
	fmt.Print("\033[H\033[2J")
}

func attemptRequest[Request any, Reply any](
	service RPCServiceName,
	req Request,
	reply *Reply,
	peer *Peer,
	logger *slog.Logger,
) error {
	var failedDials int
	var rpcErr error
	delay := randomTimeout(time.Millisecond)

	for failedDials < MAX_RPC_CALL_RETRIALS {
		if rpcErr = peer.rpcConn.Call(string(service), req, reply); rpcErr != nil {
			logger.Error(
				"failed to dial peer, retrying again after",
				slog.Any("failedDials", failedDials),
				slog.Any("peerAddr", peer.addr), slog.Any("reason", rpcErr),
			)
			failedDials++
			time.Sleep(delay)
		} else {
			break
		}
	}

	if failedDials == MAX_RPC_CALL_RETRIALS {
		return fmt.Errorf("failed to dial contact client after retrials. %w", rpcErr)
	}

	return nil
}
