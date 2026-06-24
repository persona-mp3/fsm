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
	id       string
	incoming chan RPC
	log      *log.Logger
}

func NewServer(id string, incoming chan RPC, opts *Opts) *Server {
	var o *Opts
	if opts == nil {
		o = defaultOpts()
	} else {
		o = opts
	}

	o.log.SetPrefix(fmt.Sprintf("(%s:server) ", id))

	return &Server{
		id:       id,
		incoming: incoming,
		log:      &o.log,
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
