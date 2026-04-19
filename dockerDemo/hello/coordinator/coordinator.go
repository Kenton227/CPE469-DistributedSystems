package main

import (
	"fmt"
	"net"
	"net/rpc"
)

type HelloAPI struct{}

func (hello *HelloAPI) SayHello(request string, response *string) error {
	*response = "hello from coordinator"
	fmt.Printf("Received: %v, response, %v\n", request, *response)
	return nil
}

func main() {

	hello := new(HelloAPI)
	rpc.Register(hello)
	listener, _ := net.Listen("tcp", ":3001")

	for {
		conn, _ := listener.Accept()
		fmt.Printf("Connection received!\n")
		go rpc.ServeConn(conn)
	}

}
