package main

import (
	"fsm/simulation/utils"
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
	sim, err := utils.ParseConfig("")
	if err != nil {
		log.Fatal(err)
	}

  req := CommandReq{From: "sim-client-request", Operation: Get, Key: "bryson_tyler", Value: "Right my wrongs"}
	res := &CommandReply{}

	dial, err := rpc.Dial("tcp", sim.Addresses[0])
	if err != nil {
		log.Fatal("could not dial node: ", err)
	}

	if err := dial.Call("Server.CommandRPC", req, res); err != nil {
		log.Fatal("could not call service: ", err)
	}
	log.Printf("response from raft-server:: %#v\n", res)

}
