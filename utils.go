package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"log/slog"
	"math/big"
	"net/rpc"
	"sync"
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
	// ctx, cancel := context.WithTimeout(parentCtx, SEND_TIMEOUT)// ctx, cancel := context.WithTimeout(parentCtx, SEND_TIMEOUT)
	go func() {
		select {
		case ch <- data:
		case <-time.After(3000 * time.Millisecond):
			fmt.Printf("[backgroundSendCh] timeout for sending reached: %s\n", SEND_TIMEOUT)
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

	for failedDials < MAX_RPC_DIALS {
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

	if failedDials == MAX_RPC_DIALS {
		return fmt.Errorf("failed to dial contact client after retrials. %w", rpcErr)
	}

	fmt.Println("[debug] sent payload successully::", string(service))
	return nil
}

func backgroundWait(name string, wg *sync.WaitGroup, signal chan<- struct{}) {
	wg.Wait()
	select {
	case signal <- struct{}{}:
	default:
		fmt.Printf(`[warn] could not send signal after wg fired for %s\n`, name)
	}
}

func connectTo(id int, network, addr string) (*Peer, error) {
	failedDials := 0
	var rpcConn *rpc.Client
	var err error
	for failedDials <= MAX_RPC_DIALS {
		if rpcConn, err = rpc.Dial(network, addr); err == nil {
			fmt.Printf("[debug] connected to %s successfully\n", addr)
			break
		}
		failedDials++
		fmt.Println("[debug] failed dials while connecting: ", failedDials)
	}

	if err != nil {
		return nil, err
	}

	return &Peer{
		id:          id,
		addr:        addr,
		rpcConn:     rpcConn,
		replicateCh: make(chan replicate),
	}, nil

}
