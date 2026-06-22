package main

import (
	"log"
	"net/rpc"
)

type TestReq struct {
	From    string
	Message string
}

func main() {
	addr := "localhost:8080"
	dialer, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatal("could not dial addr", err)
	}

	req := TestReq{From: "devtools/test-client.go", Message: "Hello World"}
	res := &TestReq{}
	if err := dialer.Call("Server.TestServer", req, res); err != nil {
		log.Fatal("while calling service ", err)
	}

	log.Printf("response: %+v\n", res)

}
