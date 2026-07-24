package database

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
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
}

type JKVS struct {
	// Network JKVS supports. Currently TCP
	Network string
	// Addr JKVS is running on
	Addr string
	// Underlying connection
	Conn net.Conn
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

// Commit sends the command for JKVS or the underlying database to apply to its log
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
	_, err := io.ReadFull(jkvs.Conn, buff)
	if err != nil {
		return nil, fmt.Errorf("could not read header response from jkvs: %w", err)
	}

	packetSize := binary.BigEndian.Uint32(buff)
	packet := make([]byte, packetSize)

	if _, err := io.ReadFull(jkvs.Conn, packet); err != nil {
		return nil, fmt.Errorf("could not read response body from jkvs: %w", err)
	}

	return &Response{From: "fsm-jkvs", Message: fmt.Sprintf("from-jkvs::%s", packet)}, nil
}

func (jkvs *JKVS) Disconnect() error {
	err := jkvs.Conn.Close()
	if err != nil {
		return fmt.Errorf("could not close connection with jkvs: %w", err)
	}
	return nil
}

// Ping protocol hasn't yet been implemented on JKVS
func (jkvs *JKVS) Ping() error {
	return nil
}

type TestDatabase struct {
	Network   string
	Addr      string
	Conn      net.Conn
	Connected atomic.Bool
	Commits   []any
}

func NewTestDatabase(network, addr string, conn net.Conn) Database {
	return &TestDatabase{
		Network:   network,
		Addr:      addr,
		Conn:      conn,
		Connected: atomic.Bool{},
	}
}

func (td *TestDatabase) Commit(cmd Command) (*Response, error) {
	td.Commits = append(td.Commits, cmd)
	return &Response{}, nil
}

func (td *TestDatabase) Connect() error {
	td.Connected.Store(true)
	return nil
}

func (td *TestDatabase) Disconnect() error {
	td.Connected.Store(false)
	return nil
}

func (td *TestDatabase) Ping() error {
	return nil
}
