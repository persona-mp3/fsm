package main

import (
	"context"
	"fmt"
	"log"
	"os"
)

type Server struct {
	id       string
	addr     string
	incoming chan RPC
	log      *log.Logger
}

func NewServer(id, addr string, incoming chan RPC) *Server {
	prefix := fmt.Sprintf("(%s:server) ", id)
	logger := log.New(os.Stdout, prefix, log.Ldate|log.Lmicroseconds|log.Lmsgprefix)
	return &Server{
		id:       id,
		addr:     addr,
		incoming: incoming,
		log:      logger,
	}
}

func (s *Server) Listen(ctx context.Context, network, addr string) error {
	go func() {
		<-ctx.Done()
		s.log.Println("shutting down server")
	}()

	s.log.Println("listening on", network, addr)
	return nil
}
