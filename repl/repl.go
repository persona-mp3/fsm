package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"
)

const MIN_LENGTH = 5

var raftLeaderAddr = "localhost:5003"

type Operation string

const (
	OperationGet    Operation = "get"
	OperationSet    Operation = "set"
	OperationRemove Operation = "rm"
)

type Command struct {
	Ops   Operation
	Key   string
	Value string
}

type Reply struct {
	From   string
	Result string
}

func main() {
	flag.StringVar(&raftLeaderAddr, "addr", raftLeaderAddr, "address of the raft leader for a cluster")
	flag.Parse()

	conn, err := rpc.Dial("tcp", raftLeaderAddr)
	if err != nil {
		log.Fatal("could not dial raftLeader: ", err)
	}

	if err := startRepl(conn); err != nil {
		fmt.Println(err)
	}
}

func startRepl(conn *rpc.Client) error {
	defer func() {
		if conn != nil {
			if err := conn.Close(); err != nil {
				log.Println(err)
			}
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if scanner.Err() != nil {
			return scanner.Err()
		}

		input := scanner.Text()
		cmd, err := parseInput(input)
		if err != nil {
			fmt.Println(err)
			continue
		}
		reply := Reply{}

		if err := conn.Call("Server.CommandRPC", *cmd, &reply); err != nil {
			return err
		}

		fmt.Printf("> %s\n", reply.String())
	}
	return nil
}

func parseInput(input string) (*Command, error) {
	if len(input) <= MIN_LENGTH {
		return nil, fmt.Errorf("command too short")
	}

	// get command type first
	result := strings.SplitN(input, " ", 3)

	if len(result) < 2 {
		return nil, fmt.Errorf("command too short")
	}

	cmd := Command{}
	switch result[0] {
	case OperationGet.String():
		key := result[1]
		cmd.Ops = OperationGet
		cmd.Key = key
	case OperationRemove.String():
		key := result[1]
		cmd.Ops = OperationGet
		cmd.Key = key
	case OperationSet.String():
		key := result[1]
		value := result[2]
		cmd.Ops = OperationSet
		cmd.Key = key
		cmd.Value = value

	default:
		return nil, fmt.Errorf("what is that? %s", input)
	}
	return &cmd, nil
}

func (ops Operation) String() string {
	switch ops {
	case OperationGet:
		return "get"
	case OperationRemove:
		return "rm"
	case OperationSet:
		return "set"
	default:
		return "unknown"
	}
}

func (r *Reply) String() string {
	return fmt.Sprintf("Reply: {From: %s, Result: %s}", r.From, r.Result)
}
