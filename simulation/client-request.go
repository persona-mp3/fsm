package main

import (
	"fsm/simulation/utils"
	"log"
	"net/rpc"
)

type Operation int

const (
	Set Operation = iota
	Get
	Remove
)

type CommandReq struct {
	From      string
	Operation Operation
	Result    string
}

func main() {
	sim, err := utils.ParseConfig("")
	if err != nil {
		log.Fatal(err)
	}

	req := CommandReq{From: "sim-client-request", Operation: Get, Result: ""}
	res := &CommandReq{}

	dial, err := rpc.Dial("tcp", sim.Addresses[0])
	if err != nil {
		log.Fatal("could not dial node: ", err)
	}

	if err := dial.Call("Server.ClientCommandRPC", req, res); err != nil {
		log.Fatal("could not call service: ", err)
	}
	log.Printf("response from raft-server:: %#v\n", res)

}
