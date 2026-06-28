package main

import (
	"context"
	"errors"
	"fmt"
	rlog "fsm/raftlogger"
	"net"
	"net/rpc"
)

type Server struct {
	id       string
	addr     string
	incoming chan RPC
	log      rlog.RLogger
}

func NewServer(id, addr string, incoming chan RPC, logger rlog.RLogger) *Server {
	// logger := log.New(os.Stdout, prefix, log.Ldate|log.Lmicroseconds|log.Lmsgprefix)
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

	handler := rpc.NewServer()
	if err := handler.Register(s); err != nil {
		return fmt.Errorf("could not start rpcServer. %w", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("could not start server: %w", err)
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	s.log.Println("tcp server active at", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				s.log.Println("listener closed")
				return nil
			}
			s.log.Println("could not accept connection: ", err)
			continue
		}

		go handler.ServeConn(conn)

	}
}
