package main

import (
	"flag"
	"log"
	"net/rpc"
	"sync"
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
	dial, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		log.Fatal("could not dial node: ", err)
	}

	sendCommands(dial, commands())

}

func sendCommands(dial *rpc.Client, cmds []CommandReq) {
	wg := sync.WaitGroup{}
	for _, cmd := range cmds {
		wg.Add(1)
		go func(req CommandReq) {
			defer wg.Done()
			res := &CommandReply{}
			log.Printf("waiting for reply on....%s\n", req.Key)
			err := dial.Call("Server.CommandRPC", req, res)
			if err != nil {
				log.Println("could not dial server:: ", err)
			}
			log.Printf("request: %s, reply: %+v\n", req.Key, res)

		}(cmd)
	}
	wg.Wait()

}

func commands() []CommandReq {
	commands := []CommandReq{
		CommandReq{
			From:      "sim/client-request.go",
			Operation: Set,
			Key:       "command_1",
			Value:     "this is the first command, listening to Master of None by Beach house",
		},

		CommandReq{
			From:      "sim/client-request.go",
			Operation: Get,
			Key:       "command_2",
			Value:     "",
		},
		CommandReq{
			From:      "sim/client-request.go",
			Operation: Set,
			Key:       "command_3",
			Value:     "Evain Bottle water",
		},
		CommandReq{
			From:      "sim/client-request.go",
			Operation: Get,
			Key:       "command_4",
			Value:     "",
		},
		CommandReq{
			From:      "sim/client-request.go",
			Operation: Set,
			Key:       "command_5",
			Value:     "persona-mp3@github.com",
		},
		CommandReq{
			From:      "sim/client-request.go",
			Operation: Get,
			Key:       "command_6",
			Value:     "",
		},
	}
	return commands
}




