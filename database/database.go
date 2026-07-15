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
	SetOps    Operation = "set"
	RemoveOps Operation = "rm"
)

const (
	HeaderSizeBytes = 4
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
	Commit(Command) (*Response, error)

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

func (jkvs *JKVS) Commit(cmd Command) (*Response, error) {
	payload := fmt.Sprintf("%s\r\n%s\r\n%s\r\n", cmd.Operation, cmd.Key, cmd.Value)
	raw := []byte(payload)

	header := make([]byte, HeaderSizeBytes)
	binary.BigEndian.PutUint32(header, uint32(len(raw)))
	header = append(header, raw...)

	if _, err := jkvs.Conn.Write(header); err != nil {
		return nil, fmt.Errorf("could not send payload to database: %w", err)
	}

	log.Println("sent to database successfully")

	buff := make([]byte, HeaderSizeBytes)
	_, err := jkvs.Conn.Read(buff)
	if err != nil {
		return nil, fmt.Errorf("could not read header response from jkvs: %w", err)
	}

	packetSize := binary.BigEndian.Uint32(buff)
	packet := make([]byte, packetSize)
	n, err := jkvs.Conn.Read(packet)
	if err != nil {
		return nil, fmt.Errorf("could not read  response from jkvs: %w", err)
	}

	content := packet[:n]
	return &Response{From: "mock_jkvs.send()", Message: fmt.Sprintf("from-jkvs::%s", content)}, nil
}

func (jkvs *JKVS) Disconnect() error {
	log.Println("mock_disconnected")
	err := jkvs.Conn.Close()
	if err != nil {
		return fmt.Errorf("could not close connection with jkvs: %w", err)
	}
	return nil
}

func (jkvs *JKVS) Ping() error {
	log.Println("mock_ping::")
	return nil
}
