package main

import (
	"log"
	"net/rpc"
	"time"
)

type TestReq struct {
	From    string
	Message string
}

type AppendEntryReq struct {
	Id   string
	Term uint64
	Data string
}

type AppendEntryRes struct {
	Id           string
	Term         uint64
	Data         string
	Acknowledged bool
	err          error
}

var term = 100

func lead(d *rpc.Client) {
	ticker := time.NewTicker(1)
	defer ticker.Stop()
	req := AppendEntryReq{
		Term: uint64(term),
		Id:   "devtools/test-client",
		Data: "seding rpc as leader",
	}

	for t := range ticker.C {
		_ = t
		res := &AppendEntryRes{}
		if err := d.Call("Server.AppendEntryRPC", req, res); err != nil {
			log.Println("(error) appendEntryRPC failed: ", err)
			return
		}

		if res.err != nil {
			log.Println("(error) remoteServer failed down or sent error", res.err)
			return
		}

		log.Printf("(rpc_res) from remoteServer: %+v\n", res)
	}
}

func main() {
	addr := "localhost:8080"
	dialer, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatal("could not dial addr", err)
	}
	lead(dialer)
}
