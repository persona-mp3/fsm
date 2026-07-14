package main

import (
	"flag"
	"log"
	"net/rpc"
)

type Operation string

const (
	Set    Operation = "set"
	Get              = "get"
	Remove           = "rm"
)

type CommandReq struct {
	From      string
	Operation Operation
	Key       string
	Value     string
}

type CommandReply struct {
	From   string
	Result string
}

func main() {
	var leaderAddr string
	flag.StringVar(&leaderAddr, "leader", "localhost:4000", "address of the leader node in the cluter")
	flag.Parse()

	// sim, err := utils.ParseConfig("")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	req := CommandReq{From: "sim-client-request", Operation: Set, Key: "username", Value: "person-amp3"}
	res := &CommandReply{}

	dial, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		log.Fatal("could not dial node: ", err)
	}

	if err := dial.Call("Server.CommandRPC", req, res); err != nil {
		log.Fatal("could not call service: ", err)
	}
	log.Printf("response from raft-server:: %#v\n", res)

}
