package database

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
)

type Operation string

const (
	GetOps    Operation = "get"
	SetOps              = "set"
	RemoveOps           = "rm"
)

type Command struct {
	Operation Operation
	Key       string
	Value     string
}

type Response struct {
	From    string
	Message string
}

type Database interface {
	Connect() error

	// Send sends a request to the database. At the moment, the underlying protocol
	// is RESP inspired as that is was the database supports
	Send(Command) (*Response, error)

	// Disconnect safely disconnects from the database, returns an error if it
	// failed
	Disconnect() error

	Ping() error

	// WritePump(incoming <-chan Command)
	// ReadPump(responses chan<- Response)
}

type JKVS struct {
	Network string
	Addr    string
	Conn    net.Conn
}

func NewJKVSDatabase(network, addr string) *JKVS {
	return &JKVS{
		Network: network,
		Addr:    addr,
	}
}

func (jkvs *JKVS) Connect() error {
	conn, err := net.Dial(jkvs.Network, jkvs.Addr)
	if err != nil {
		return fmt.Errorf("could not dial database: %w", err)
	}
	jkvs.Conn = conn
	return nil
}

func (jkvs *JKVS) Send(cmd Command) (*Response, error) {
	payload := fmt.Sprintf("%s\r\n%s\r\n%s\r\n", cmd.Operation, cmd.Key, cmd.Value)
	log.Println("formatted_payload::", payload)
  raw := []byte(payload)

  header := make([]byte, 4)
  binary.BigEndian.PutUint32(header, uint32(len(raw)))
  header = append(header, raw...)


	if _, err := jkvs.Conn.Write(header); err != nil {
		return nil, fmt.Errorf("could not send payload to database: %w", err)
	}

	log.Println("sent to database successfully")
  

	return &Response{From: "mock_jkvs.send()", Message: "stub_message"}, nil
}

func (jkvs *JKVS) Disconnect() error {
	log.Println("mock_disconnected")
	return nil
}

func (jkvs *JKVS) Ping() error {
	log.Println("mock_ping::")
	return nil
}
