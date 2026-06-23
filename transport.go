package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
)

type TestReq struct {
	From    string
	Message string
}

type Server struct {
	incoming chan RPC
}

func NewServer(in chan RPC, out chan any) *Server {
	return &Server{
		incoming: in,
	}
}

func (s *Server) Listen(ctx context.Context, addr string) error {
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

	log.Println("tcp server active at", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("listener closed")
				return nil
			}
			log.Println("could not accept connection: ", err)
			continue
		}

		go handler.ServeConn(conn)

	}

}
