package main

import (
	"crypto/rand"
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
